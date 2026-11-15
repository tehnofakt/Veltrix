"""Veltrix: a small personal Telegram userbot. Python 3.11+."""

import asyncio
import logging
import math
import os
import re
import sqlite3
import time
from collections import deque
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, errors, events, utils

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
LOG = logging.getLogger("veltrix")
HELP = """Veltrix
.spam 10 привет — отправить 10 отдельных сообщений
.spam forever привет — повторять только в «Избранном»
.stopspam — остановить отправку в текущем чате
.mute — архивировать и удалять новые входящие в личном чате
.unmute — отключить мут в текущем личном чате
.help — эта справка

Количество сообщений не ограничено в коде. Telegram может назначить FloodWait.
Архивируются новые входящие личные сообщения, пока Veltrix запущен."""


class Archive:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS muted (chat_id INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS messages (
                message_id INTEGER PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL,
                sender_name TEXT NOT NULL,
                text TEXT NOT NULL,
                media_note TEXT NOT NULL,
                photo_path TEXT,
                photo_id INTEGER,
                created_at TEXT NOT NULL,
                suppressed INTEGER NOT NULL DEFAULT 0,
                notified INTEGER NOT NULL DEFAULT 0
            );
        """)

    def get(self, message_id):
        return self.db.execute(
            "SELECT * FROM messages WHERE message_id = ?", (message_id,)
        ).fetchone()

    def is_muted(self, chat_id):
        return self.db.execute(
            "SELECT 1 FROM muted WHERE chat_id = ?", (chat_id,)
        ).fetchone() is not None

    def mute(self, chat_id, enabled):
        with self.db:
            if enabled:
                self.db.execute("INSERT OR IGNORE INTO muted VALUES (?)", (chat_id,))
            else:
                self.db.execute("DELETE FROM muted WHERE chat_id = ?", (chat_id,))

    def save(self, message, sender_name, media_note, photo_id, photo_path):
        with self.db:
            self.db.execute("""
                INSERT INTO messages
                    (message_id, chat_id, sender_id, sender_name, text, media_note,
                     photo_path, photo_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(message_id) DO UPDATE SET
                    sender_name=excluded.sender_name, text=excluded.text,
                    media_note=excluded.media_note, photo_path=excluded.photo_path,
                    photo_id=excluded.photo_id
            """, (message.id, message.chat_id, message.sender_id, sender_name,
                  message.raw_text or "", media_note, photo_path, photo_id,
                  message.date.isoformat()))

    def flag(self, message_id, *, suppressed=None, notified=None):
        with self.db:
            if suppressed is not None:
                self.db.execute("UPDATE messages SET suppressed=? WHERE message_id=?",
                                (int(suppressed), message_id))
            if notified is not None:
                self.db.execute("UPDATE messages SET notified=? WHERE message_id=?",
                                (int(notified), message_id))

    def close(self):
        self.db.close()


class Veltrix:
    def __init__(self, client, archive, owner_id, delay=1.5, media_dir=None):
        self.client = client
        self.archive = archive
        self.owner_id = owner_id
        self.delay = delay
        self.media_dir = media_dir or DATA / "media"
        self.media_dir.mkdir(parents=True, exist_ok=True)
        self.spam_tasks = {}
        # Serialize archive events so deletion cannot overtake a photo download.
        self.archive_lock = asyncio.Lock()
        self.send_lock = asyncio.Lock()
        self.generated_ids = deque(maxlen=2048)
        self.flood_until = 0.0

    async def telegram(self, operation):
        """Retry FloodWait/slow mode, sharing the cooldown across operations."""
        while True:
            remaining = self.flood_until - time.monotonic()
            if remaining > 0:
                await asyncio.sleep(remaining)
                continue
            try:
                return await operation()
            except (errors.FloodWaitError, errors.SlowModeWaitError) as exc:
                seconds = max(1, exc.seconds) + 1
                self.flood_until = max(self.flood_until, time.monotonic() + seconds)
                LOG.warning("Telegram requested a pause of %s seconds", seconds)

    async def edit(self, event, text):
        try:
            await self.telegram(lambda: event.edit(text, parse_mode=None))
        except errors.MessageNotModifiedError:
            pass

    async def notify(self, text):
        # 1,000 Unicode code points stay below Telegram's UTF-16 text limit.
        for offset in range(0, len(text), 1000):
            await self.telegram(lambda: self.client.send_message(
                "me", text[offset:offset + 1000], parse_mode=None, link_preview=False
            ))

    async def spam(self, chat_id, peer, count, text, ready):
        sent = 0
        try:
            await ready.wait()
            while count is None or sent < count:
                async def send():
                    # Register generated messages before their command events run.
                    async with self.send_lock:
                        message = await self.client.send_message(
                            peer, text, parse_mode=None, link_preview=False
                        )
                        self.generated_ids.append((chat_id, message.id))
                await self.telegram(send)
                sent += 1
                if count is None or sent < count:
                    await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.error("Sending stopped: %s", type(exc).__name__)
            try:
                await self.notify(
                    f"⚠️ Отправка в чате {chat_id} остановлена. Отправлено: {sent}. "
                    f"Ошибка: {type(exc).__name__}."
                )
            except Exception as notification_error:
                LOG.error("Notification failed: %s", type(notification_error).__name__)
        finally:
            if self.spam_tasks.get(chat_id) is asyncio.current_task():
                self.spam_tasks.pop(chat_id, None)

    async def command(self, event):
        if not event.out or event.sender_id != self.owner_id:
            return
        async with self.send_lock:
            if (event.chat_id, event.id) in self.generated_ids:
                return
        text = event.raw_text or ""
        parts = text.split(maxsplit=2)
        if not parts or parts[0] not in {
            ".spam", ".stopspam", ".mute", ".unmute", ".help"
        }:
            return
        command = parts[0]
        chat_id = event.chat_id
        if command == ".help":
            await self.edit(event, HELP)
        elif command in {".mute", ".unmute"}:
            if not event.is_private or chat_id == self.owner_id:
                await self.edit(event, "⚠️ Мут доступен только в личном чате с собеседником")
                return
            enabled = command == ".mute"
            self.archive.mute(chat_id, enabled)
            await self.edit(event, "🔇 Мут активирован" if enabled else "🔊 Мут отключён")
        elif command == ".stopspam":
            task = self.spam_tasks.pop(chat_id, None)
            if task:
                task.cancel()  # Also interrupts a FloodWait or interval immediately.
                await asyncio.gather(task, return_exceptions=True)
            await self.edit(event, "⏹ Отправка остановлена" if task else "ℹ️ Активной отправки нет")
        elif command == ".spam":
            if len(parts) != 3 or not parts[2].strip():
                await self.edit(event, "Использование: .spam <количество|forever> <текст>")
                return
            count_arg, payload = parts[1:]
            if count_arg == "forever":
                if chat_id != self.owner_id:
                    await self.edit(event, "⚠️ Бесконечная отправка разрешена только в «Избранном»")
                    return
                count = None
            else:
                try:
                    count = int(count_arg)
                    if count <= 0:
                        raise ValueError
                except ValueError:
                    await self.edit(event, "⚠️ Количество должно быть положительным целым числом")
                    return
            if len(payload.encode("utf-16-le")) // 2 > 4096:
                await self.edit(event, "⚠️ Текст превышает лимит одного сообщения Telegram")
                return
            peer = await event.get_input_chat()
            if chat_id in self.spam_tasks:
                await self.edit(event, "⚠️ Отправка уже запущена; сначала выполните .stopspam")
                return
            ready = asyncio.Event()
            task = asyncio.create_task(self.spam(chat_id, peer, count, payload, ready))
            self.spam_tasks[chat_id] = task
            try:
                status = (f"✅ Запущена отправка {count} сообщений" if count is not None
                          else "✅ Запущена непрерывная отправка в «Избранном»")
                await self.edit(event, status)
            except BaseException:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                raise
            finally:
                ready.set()

    async def archive_message(self, message):
        previous = self.archive.get(message.id)
        sender = await message.get_sender()
        name = utils.get_display_name(sender) if sender else str(message.sender_id)
        photo = message.photo
        ttl = getattr(message.media, "ttl_seconds", None)
        photo_id = photo.id if photo and not ttl else None
        photo_path = None
        note = ""
        if ttl:
            note = "[Одноразовое или самоуничтожающееся медиа не сохраняется]"
        elif photo:
            note = "[Фото]"
            if previous and previous["photo_id"] == photo_id:
                photo_path = previous["photo_path"]
        elif message.media:
            note = "[Медиа: локально сохраняются только обычные фотографии]"
        # Persist text first, even when downloading media fails.
        self.archive.save(message, name, note, photo_id, photo_path)
        if photo_id and (not photo_path or not Path(photo_path).is_file()):
            path = self.media_dir / f"{message.chat_id}_{message.id}_{photo_id}.jpg"
            downloaded = await self.telegram(
                lambda: self.client.download_media(message, file=str(path))
            )
            if not downloaded:
                raise RuntimeError("Photo download returned no file")
            photo_path = str(downloaded)
            self.archive.save(message, name, note, photo_id, photo_path)
        return previous

    async def incoming(self, event):
        if not event.is_private or event.out or event.sender_id == self.owner_id:
            return
        async with self.archive_lock:
            await self.archive_message(event.message)
            if self.archive.is_muted(event.chat_id):
                # Durable marker must precede the RPC, which may emit an update.
                self.archive.flag(event.id, suppressed=True)
                try:
                    await self.telegram(lambda: self.client.delete_messages(
                        event.chat_id, [event.id], revoke=True
                    ))
                except (errors.ChatAdminRequiredError, errors.MessageDeleteForbiddenError):
                    # Best effort if Telegram disallows deletion for both parties.
                    try:
                        await self.telegram(lambda: self.client.delete_messages(
                            event.chat_id, [event.id], revoke=False
                        ))
                    except errors.RPCError:
                        self.archive.flag(event.id, suppressed=False)
                        raise
                except errors.RPCError:
                    self.archive.flag(event.id, suppressed=False)
                    raise
                # A cancelled/disconnected request may already have reached the
                # server. Keep its marker to avoid reporting our own deletion.

    async def edited(self, event):
        if not event.is_private or event.out or event.sender_id == self.owner_id:
            return
        async with self.archive_lock:
            previous = await self.archive_message(event.message)
            new_text = event.raw_text or ""
            if previous and previous["text"] != new_text:
                await self.notify(
                    f"✏️ Сообщение изменено\n"
                    f"От: {previous['sender_name']} (ID {previous['sender_id']})\n"
                    f"Чат: {previous['chat_id']}, сообщение: {event.id}\n\n"
                    f"Было:\n{previous['text'] or '[Без текста]'}\n\n"
                    f"Стало:\n{new_text or '[Без текста]'}"
                )

    async def deleted(self, event):
        # Private deletion updates have no chat_id. Channel IDs may overlap.
        if event.chat_id is not None:
            return
        async with self.archive_lock:
            for message_id in event.deleted_ids:
                row = self.archive.get(message_id)
                if not row or row["suppressed"] or row["notified"]:
                    continue
                await self.notify(
                    f"🗑 Сообщение удалено\n"
                    f"От: {row['sender_name']} (ID {row['sender_id']})\n"
                    f"Чат: {row['chat_id']}, сообщение: {message_id}\n\n"
                    f"{row['text'] or '[Без текста]'}\n{row['media_note']}"
                )
                if row["photo_path"]:
                    if Path(row["photo_path"]).is_file():
                        await self.telegram(lambda: self.client.send_file(
                            "me", row["photo_path"],
                            caption=f"Сохранённое фото: сообщение {message_id}",
                            parse_mode=None,
                        ))
                    else:
                        await self.notify("⚠️ Сохранённое фото отсутствует на диске.")
                self.archive.flag(message_id, notified=True)

    def guarded(self, handler):
        async def wrapped(event):
            try:
                await handler(event)
            except Exception as exc:
                # Do not log message contents, phone numbers or credentials.
                LOG.error("%s failed: %s", handler.__name__, type(exc).__name__)
        return wrapped

    def register(self):
        self.client.add_event_handler(self.guarded(self.command), events.NewMessage(outgoing=True))
        self.client.add_event_handler(self.guarded(self.incoming), events.NewMessage(incoming=True))
        self.client.add_event_handler(self.guarded(self.edited), events.MessageEdited(incoming=True))
        self.client.add_event_handler(self.guarded(self.deleted), events.MessageDeleted())

    async def stop(self):
        tasks = list(self.spam_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.spam_tasks.clear()


def configuration():
    load_dotenv(ROOT / ".env")
    try:
        api_id = int(os.environ.get("API_ID", ""))
        api_hash = os.environ.get("API_HASH", "").strip()
        delay = float(os.environ.get("SPAM_DELAY", "1.5"))
        if api_id <= 0 or not re.fullmatch(r"[0-9a-fA-F]{32}", api_hash):
            raise ValueError
        if not math.isfinite(delay) or delay < 1:
            raise ValueError
    except ValueError as exc:
        raise ValueError(
            "Заполните API_ID и API_HASH в .env; SPAM_DELAY должен быть числом >= 1."
        ) from exc
    return api_id, api_hash, os.environ.get("PHONE", "").strip() or None, delay


async def main():
    api_id, api_hash, phone, delay = configuration()
    DATA.mkdir(parents=True, exist_ok=True)
    archive = Archive(DATA / "archive.sqlite3")
    client = TelegramClient(
        str(DATA / "veltrix"), api_id, api_hash,
        auto_reconnect=True, connection_retries=None, retry_delay=3,
        flood_sleep_threshold=0, raise_last_call_error=True,
    )
    bot = None
    try:
        await client.start(phone=phone or (lambda: input("Номер телефона (+7…): ")))
        owner = await client.get_me()
        if owner.bot:
            raise ValueError("Veltrix требует личный аккаунт, а не токен бота.")
        bot = Veltrix(client, archive, owner.id, delay)
        bot.register()
        print("Veltrix запущен. Команды: .help в Telegram. Остановка: Ctrl+C.")
        await client.run_until_disconnected()
    finally:
        if bot:
            await bot.stop()
        await client.disconnect()
        archive.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("telethon").setLevel(logging.CRITICAL)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Veltrix остановлен.")
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(1)
    except Exception as exc:
        LOG.error("Veltrix stopped: %s", type(exc).__name__)
        raise SystemExit(1)
