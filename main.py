"""Veltrix: one server-side Telegram bot for multiple connected accounts."""

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.types import FSInputFile, KeyboardButton, ReplyKeyboardMarkup
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("veltrix")
PAGE_SIZE = 5
INSTRUCTIONS = """Подключение Veltrix
1. Откройте настройки своего профиля Telegram → «Чат-боты» / Chatbots.
В некоторых версиях приложения раздел находится в «Telegram для бизнеса».
2. Добавьте этого бота и выберите чаты, к которым разрешаете доступ.
3. Разрешите ответы и удаление всех сообщений в выбранных чатах.
4. Вернитесь сюда и отправьте /status.

Если раздела нет, обновите официальный клиент Telegram. Одного /start
недостаточно: подключение нужно подтвердить в настройках Telegram.
Номер телефона, коды входа, пароль и API-ключи здесь не нужны.

После подключения новые входящие сообщения и обычные фотографии будут
сохраняться на сервере Veltrix. Удаления и изменения поступят сюда.
Владелец сервера технически имеет доступ к сохранённым данным.
Выбирайте только те чаты, которые готовы доверить этому сервису.
История до подключения и секретные чаты недоступны.
/stop — остановить обработку; /erase confirm — удалить свой архив с сервера.
Отключить доступ Telegram можно в том же разделе «Чат-боты»."""
HELP = """Veltrix
В подключённой личной переписке, от имени владельца аккаунта:
.spam 10 привет — 10 отдельных сообщений
.stopspam — остановить отправку в этом чате
.mute — архивировать и удалять новые входящие сообщения
.unmute — отключить мут
.help — справка

В диалоге с Veltrix:
/connect — инструкция подключения
/status — подключение и выданные права
/chats — известные боту чаты и их ID
/archive — свой архив; /archive 2 — следующая страница
/mute ID и /unmute ID — управление мутом по ID из /chats
/spam ID 10 привет — отправка в известный подключённый чат
/stopspam ID — остановить её
.spam 10 тест — тест здесь, сообщениями от бота
.spam forever тест — непрерывный тест ТОЛЬКО здесь
.stopspam — остановить тест
/stop — приостановить обработку и все свои отправки
/start — возобновить обработку
/erase confirm — остановить обработку и удалить свой архив

Конечное количество не ограничено в коде. Telegram ограничивает частоту
и отправку от вашего имени: обычно нужны входящие за последние 24 часа.
Права и выбранные вами чаты определяют доступные действия.
Уведомления приходят сюда, не в «Избранное»."""
MENU = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="/status"), KeyboardButton(text="/connect")],
    [KeyboardButton(text="/archive"), KeyboardButton(text="/chats")],
    [KeyboardButton(text="/help")],
], resize_keyboard=True)


class Archive:
    """All user data lives locally on the bot server, never in GitHub."""

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.media = self.directory / "media"
        self.media.mkdir(exist_ok=True)
        # Keep the old userbot database separate and untouched.
        self.db = sqlite3.connect(self.directory / "veltrix.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                owner_id INTEGER PRIMARY KEY, chat_id INTEGER NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS connections (
                id TEXT PRIMARY KEY, owner_id INTEGER NOT NULL,
                enabled INTEGER NOT NULL, rights TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS connection_owner ON connections(owner_id);
            CREATE TABLE IF NOT EXISTS chats (
                owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
                title TEXT NOT NULL, last_seen INTEGER NOT NULL,
                PRIMARY KEY (owner_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS muted (
                owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
                PRIMARY KEY (owner_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS messages (
                owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL, sender_id INTEGER NOT NULL,
                sender_name TEXT NOT NULL, text TEXT NOT NULL, date INTEGER NOT NULL,
                media_note TEXT NOT NULL, photo_id TEXT, photo_path TEXT,
                deleted INTEGER NOT NULL DEFAULT 0,
                suppressed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (owner_id, chat_id, message_id)
            );
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL,
                text TEXT NOT NULL, photo_path TEXT, text_offset INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0
            );
        """)

    def user(self, owner):
        return self.db.execute("SELECT * FROM users WHERE owner_id=?", (owner,)).fetchone()

    def start_user(self, owner, chat_id):
        with self.db:
            self.db.execute("""INSERT INTO users VALUES (?, ?, 1)
                ON CONFLICT(owner_id) DO UPDATE SET chat_id=excluded.chat_id, active=1""",
                            (owner, chat_id))

    def pause_user(self, owner):
        with self.db:
            self.db.execute("UPDATE users SET active=0 WHERE owner_id=?", (owner,))

    def connection(self, connection_id):
        row = self.db.execute("SELECT * FROM connections WHERE id=?", (connection_id,)).fetchone()
        return dict(row, rights=json.loads(row["rights"])) if row else None

    def active_connection(self, owner):
        row = self.db.execute(
            "SELECT id FROM connections WHERE owner_id=? AND enabled=1", (owner,)
        ).fetchone()
        return self.connection(row["id"]) if row else None

    def save_connection(self, connection):
        owner = connection.user.id
        rights = connection.rights.model_dump() if connection.rights else {}
        with self.db:
            if connection.is_enabled:
                self.db.execute("UPDATE connections SET enabled=0 WHERE owner_id=?", (owner,))
            self.db.execute("""INSERT INTO connections VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET enabled=excluded.enabled, rights=excluded.rights""",
                            (connection.id, owner, int(connection.is_enabled), json.dumps(rights)))

    def known_chat(self, owner, chat_id):
        return self.db.execute("SELECT * FROM chats WHERE owner_id=? AND chat_id=?",
                               (owner, chat_id)).fetchone()

    def touch_chat(self, owner, message):
        title = message.chat.full_name or str(message.chat.id)
        with self.db:
            self.db.execute("""INSERT INTO chats VALUES (?, ?, ?, ?)
                ON CONFLICT(owner_id, chat_id) DO UPDATE SET
                title=excluded.title, last_seen=MAX(last_seen, excluded.last_seen)""",
                            (owner, message.chat.id, title, int(message.date.timestamp())))

    def is_muted(self, owner, chat_id):
        return self.db.execute("SELECT 1 FROM muted WHERE owner_id=? AND chat_id=?",
                               (owner, chat_id)).fetchone() is not None

    def mute(self, owner, chat_id, enabled):
        with self.db:
            if enabled:
                self.db.execute("INSERT OR IGNORE INTO muted VALUES (?, ?)", (owner, chat_id))
            else:
                self.db.execute("DELETE FROM muted WHERE owner_id=? AND chat_id=?", (owner, chat_id))

    def get(self, owner, chat_id, message_id):
        return self.db.execute(
            "SELECT * FROM messages WHERE owner_id=? AND chat_id=? AND message_id=?",
            (owner, chat_id, message_id),
        ).fetchone()

    def save_message(self, owner, message, note, photo_id, path):
        sender = message.from_user
        with self.db:
            self.db.execute("""INSERT INTO messages
                (owner_id, chat_id, message_id, sender_id, sender_name, text,
                 date, media_note, photo_id, photo_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner_id, chat_id, message_id) DO UPDATE SET
                text=excluded.text, sender_name=excluded.sender_name,
                media_note=excluded.media_note, photo_id=excluded.photo_id,
                photo_path=excluded.photo_path""",
                (owner, message.chat.id, message.message_id, sender.id, sender.full_name,
                 message.text or message.caption or "", int(message.date.timestamp()),
                 note, photo_id, path))

    def suppress(self, owner, chat_id, message_id, enabled=True):
        with self.db:
            self.db.execute("""UPDATE messages SET suppressed=?
                WHERE owner_id=? AND chat_id=? AND message_id=?""",
                            (int(enabled), owner, chat_id, message_id))

    def enqueue(self, owner, text, photo_path=None):
        with self.db:
            self.db.execute("INSERT INTO notifications(owner_id, text, photo_path) VALUES (?, ?, ?)",
                            (owner, text, photo_path))

    def mark_deleted(self, row):
        # Commit the deletion state and notification atomically.
        with self.db:
            self.db.execute("""UPDATE messages SET deleted=1
                WHERE owner_id=? AND chat_id=? AND message_id=?""",
                            (row["owner_id"], row["chat_id"], row["message_id"]))
            if not row["suppressed"]:
                self.db.execute(
                    "INSERT INTO notifications(owner_id, text, photo_path) VALUES (?, ?, ?)",
                    (row["owner_id"], self.describe(row, "🗑 Сообщение удалено"), row["photo_path"]),
                )

    @staticmethod
    def describe(row, heading=None):
        if heading is None:
            heading = "🔇 Удалено мутом" if row["suppressed"] else (
                "🗑 Удалено" if row["deleted"] else "📨 Сохранено")
        return (f"{heading}\nОт: {row['sender_name']} (ID {row['sender_id']})\n"
                f"Чат: {row['chat_id']}; сообщение: {row['message_id']}\n\n"
                f"{row['text'] or '[Без текста]'}\n{row['media_note']}")

    def erase(self, owner):
        for path in self.media.glob(f"{int(owner)}_*"):
            if path.is_file():
                path.unlink()
        with self.db:
            for table in ("messages", "muted", "chats", "notifications", "connections"):
                self.db.execute(f"DELETE FROM {table} WHERE owner_id=?", (owner,))
            self.db.execute("UPDATE users SET active=0 WHERE owner_id=?", (owner,))
        # Logical deletion, not secure erasure of disk blocks or backups.

    def close(self):
        self.db.close()


class Veltrix:
    def __init__(self, bot, archive, delay=1.5):
        self.bot = bot
        self.archive = archive
        self.delay = delay
        self.spam_tasks = {}
        self.locks = defaultdict(asyncio.Lock)
        self.flood_until = 0.0
        self.notification_task = None
        self.handler_tasks = set()
        self.closing = False

    async def telegram(self, operation):
        while True:
            wait = self.flood_until - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            try:
                return await operation()
            except TelegramRetryAfter as exc:
                seconds = max(1, exc.retry_after) + 1
                self.flood_until = max(self.flood_until, time.monotonic() + seconds)
                LOG.warning("Telegram requested a %s second cooldown", seconds)

    async def send_text(self, chat_id, text, **kwargs):
        for start in range(0, len(text), 1000):
            await self.telegram(lambda start=start: self.bot.send_message(
                chat_id, text[start:start + 1000], parse_mode=None, **kwargs
            ))

    def require_connection(self, connection_id, *, reply=False, delete=False):
        connection = self.archive.connection(connection_id)
        user = self.archive.user(connection["owner_id"]) if connection else None
        if not connection or not connection["enabled"] or not user or not user["active"]:
            raise ValueError("Подключение выключено. Проверьте /status и настройки Telegram.")
        rights = connection["rights"]
        if reply and not rights.get("can_reply"):
            raise ValueError("Нет права отвечать от вашего имени. Проверьте настройки подключения.")
        if delete and not rights.get("can_delete_all_messages"):
            raise ValueError("Для мута разрешите удаление всех сообщений в выбранных чатах.")
        return connection

    async def resolve_connection(self, connection_id):
        connection = self.archive.connection(connection_id)
        if not connection:
            data = await self.telegram(lambda: self.bot.get_business_connection(connection_id))
            self.archive.save_connection(data)
            connection = self.archive.connection(connection_id)
        return connection

    async def stop_tasks(self, owner=None):
        tasks = []
        for key, task in list(self.spam_tasks.items()):
            if owner is None or key[0] == owner:
                self.spam_tasks.pop(key, None)
                task.cancel()
                tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)

    async def connected(self, connection):
        self.archive.save_connection(connection)
        await self.stop_tasks(connection.user.id)
        if self.archive.user(connection.user.id):
            state = "подключён" if connection.is_enabled else "отключён"
            self.archive.enqueue(connection.user.id, f"Veltrix {state}. Проверка: /status.")

    async def archive_message(self, owner, message):
        previous = self.archive.get(owner, message.chat.id, message.message_id)
        photo_id = message.photo[-1].file_id if message.photo else None
        unsupported = message.has_protected_content or getattr(message, "ttl_seconds", None)
        path, note = None, ""
        if unsupported:
            photo_id, note = None, "[Защищённое/одноразовое медиа не копируется]"
        elif photo_id:
            note = "[Фото]"
            if previous and previous["photo_id"] == photo_id:
                path = previous["photo_path"]
        elif not message.text:
            note = "[Нет текста; локально сохраняются только обычные фотографии]"
        self.archive.save_message(owner, message, note, photo_id, path)
        if photo_id and (not path or not Path(path).is_file()):
            suffix = hashlib.sha256(photo_id.encode()).hexdigest()[:16]
            target = self.archive.media / f"{owner}_{message.chat.id}_{message.message_id}_{suffix}.jpg"
            try:
                file = await self.telegram(lambda: self.bot.get_file(photo_id))
                if not file.file_path:
                    raise ValueError("Photo has no download path")
                await self.telegram(lambda: self.bot.download_file(file.file_path, destination=target))
                if not target.is_file():
                    raise OSError("Photo was not written")
                path = str(target.resolve())
                self.archive.save_message(owner, message, note, photo_id, path)
            except Exception as exc:
                LOG.warning("Photo archive failed: %s", type(exc).__name__)
                self.archive.save_message(owner, message, "[Фото не удалось сохранить]", photo_id, None)
                return previous, False
        return previous, True

    async def business_message(self, message, edited=False):
        cid = message.business_connection_id
        if (not cid or message.chat.type != "private" or not message.from_user
                or message.sender_business_bot):
            return
        # Owner commands bypass downloads/deletions to keep stop responsive.
        connection = self.archive.connection(cid)
        if connection and message.from_user.id == connection["owner_id"]:
            if not edited:
                await self.business_command(message, connection)
            return
        async with self.locks[cid]:
            connection = await self.resolve_connection(cid)
            owner = connection["owner_id"]
            user = self.archive.user(owner)
            if not connection["enabled"] or not user or not user["active"]:
                return
            if message.from_user.id == owner:
                if not edited:
                    await self.business_command(message, connection)
                return
            self.archive.touch_chat(owner, message)
            previous, saved = await self.archive_message(owner, message)
            text = message.text or message.caption or ""
            if edited:
                if previous and previous["text"] != text:
                    self.archive.enqueue(owner,
                        f"✏️ Сообщение изменено\nОт: {message.from_user.full_name} "
                        f"(ID {message.from_user.id})\nЧат: {message.chat.id}; "
                        f"сообщение: {message.message_id}\n\n"
                        f"Было:\n{previous['text'] or '[Без текста]'}\n\n"
                        f"Стало:\n{text or '[Без текста]'}")
                return
            if saved and self.archive.is_muted(owner, message.chat.id):
                try:
                    self.require_connection(cid, delete=True)
                except ValueError:
                    return
                self.archive.suppress(owner, message.chat.id, message.message_id)
                try:
                    async def delete():
                        self.require_connection(cid, delete=True)
                        return await self.bot.delete_business_messages(cid, [message.message_id])
                    await self.telegram(delete)
                    row = self.archive.get(owner, message.chat.id, message.message_id)
                    self.archive.mark_deleted(row)
                except (TelegramBadRequest, TelegramForbiddenError, ValueError):
                    self.archive.suppress(owner, message.chat.id, message.message_id, False)
                    self.archive.enqueue(owner,
                        f"⚠️ Не удалось удалить сообщение {message.message_id} "
                        f"в чате {message.chat.id}. Оно сохранено. Проверьте права в /status.")
                # Keep suppression on ambiguous network failure/cancellation.

    async def edited(self, message):
        await self.business_message(message, edited=True)

    async def deleted(self, event):
        async with self.locks[event.business_connection_id]:
            connection = await self.resolve_connection(event.business_connection_id)
            user = self.archive.user(connection["owner_id"])
            if not user or not user["active"] or not connection["enabled"]:
                return
            for message_id in event.message_ids:
                row = self.archive.get(connection["owner_id"], event.chat.id, message_id)
                if row and not row["deleted"]:
                    self.archive.mark_deleted(row)

    async def business_status(self, message, connection, text):
        try:
            async def edit():
                self.require_connection(connection["id"], reply=True)
                return await self.bot.edit_message_text(
                    text=text, chat_id=message.chat.id, message_id=message.message_id,
                    business_connection_id=connection["id"], parse_mode=None)
            await self.telegram(edit)
        except (TelegramAPIError, ValueError):
            self.archive.enqueue(connection["owner_id"],
                f"Чат {message.chat.id}: {text}\n"
                "Не удалось отредактировать команду в переписке; результат показан здесь.")

    async def business_command(self, message, connection):
        parts = (message.text or "").split(maxsplit=2)
        if not parts or parts[0] not in {".mute", ".unmute", ".spam", ".stopspam", ".help"}:
            return
        owner, chat_id = connection["owner_id"], message.chat.id
        try:
            self.require_connection(connection["id"])
            self.archive.touch_chat(owner, message)
            if parts[0] == ".help":
                status = HELP
            elif parts[0] in {".mute", ".unmute"}:
                enabled = parts[0] == ".mute"
                if enabled:
                    self.require_connection(connection["id"], delete=True)
                self.archive.mute(owner, chat_id, enabled)
                status = "🔇 Мут активирован" if enabled else "🔊 Мут отключён"
            elif parts[0] == ".stopspam":
                await self.stop_spam((owner, "business", chat_id))
                status = "⏹ Отправка остановлена"
            else:
                if len(parts) != 3:
                    raise ValueError("Использование: .spam <количество> <текст>")
                count = self.parse_count(parts[1])
                self.launch_spam(owner, chat_id, count, parts[2], connection["id"])
                status = f"✅ Запущена отправка {count} сообщений"
        except ValueError as exc:
            status = f"⚠️ {exc}"
        await self.business_status(message, connection, status)

    @staticmethod
    def parse_count(value, test=False):
        if value == "forever":
            if test:
                return None
            raise ValueError("Бесконечный тест разрешён только в личном диалоге с Veltrix.")
        try:
            count = int(value)
            if count <= 0:
                raise ValueError
            return count
        except ValueError as exc:
            raise ValueError("Количество должно быть положительным целым числом.") from exc

    def launch_spam(self, owner, chat_id, count, text, connection_id=None):
        if not text.strip() or len(text.encode("utf-16-le")) // 2 > 4096:
            raise ValueError("Нужен непустой текст в пределах лимита одного сообщения Telegram.")
        if connection_id:
            self.require_connection(connection_id, reply=True)
        key = (owner, "business" if connection_id else "test", chat_id)
        if key in self.spam_tasks:
            raise ValueError("Отправка уже запущена; сначала остановите её.")
        self.spam_tasks[key] = asyncio.create_task(self.spam(key, count, text, connection_id))

    async def spam(self, key, count, text, connection_id):
        owner, _, chat_id = key
        sent = 0
        try:
            while count is None or sent < count:
                async def send():
                    user = self.archive.user(owner)
                    if not user or not user["active"]:
                        raise ValueError("Обработка остановлена.")
                    if connection_id:
                        self.require_connection(connection_id, reply=True)
                    return await self.bot.send_message(
                        chat_id, text, business_connection_id=connection_id, parse_mode=None)
                await self.telegram(send)
                sent += 1
                if count is None or sent < count:
                    await asyncio.sleep(self.delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("Sending stopped: %s", type(exc).__name__)
            self.archive.enqueue(owner,
                f"⚠️ Отправка в чате {chat_id} остановлена. Подтверждено сообщений: {sent}. "
                f"Ошибка: {type(exc).__name__}. Проверьте права и активность чата. "
                "При сетевом сбое последнее сообщение могло быть отправлено без подтверждения.")
        finally:
            if self.spam_tasks.get(key) is asyncio.current_task():
                self.spam_tasks.pop(key, None)

    async def stop_spam(self, key):
        task = self.spam_tasks.pop(key, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def private(self, message):
        if (message.chat.type != "private" or not message.from_user
                or message.from_user.id != message.chat.id or message.from_user.is_bot):
            return
        owner, chat_id = message.from_user.id, message.chat.id
        parts = (message.text or "").split(maxsplit=1)
        if not parts:
            return
        command = parts[0].split("@")[0]
        argument = parts[1] if len(parts) > 1 else ""
        if command == "/start":
            self.archive.start_user(owner, chat_id)
            await self.send_text(chat_id, "Veltrix — помощник для подключённых личных чатов.\n\n"
                                 + INSTRUCTIONS, reply_markup=MENU)
            return
        user = self.archive.user(owner)
        if not user:
            await self.send_text(chat_id, "Сначала нажмите /start.")
            return
        try:
            if command in {"/connect", "/privacy"}:
                await self.send_text(chat_id, INSTRUCTIONS)
            elif command in {"/help", ".help"}:
                await self.send_text(chat_id, HELP)
            elif command == "/status":
                connection = self.archive.active_connection(owner)
                rights = connection["rights"] if connection else {}
                await self.send_text(chat_id,
                    f"Обработка: {'включена' if user['active'] else 'приостановлена'}\n"
                    f"Подключение Telegram: {'есть' if connection else 'нет'}\n"
                    f"Ответы от вашего имени: {'да' if rights.get('can_reply') else 'нет'}\n"
                    f"Удаление входящих: {'да' if rights.get('can_delete_all_messages') else 'нет'}\n"
                    "Доступны только выбранные в настройках Telegram чаты.\n"
                    "Отправка зависит также от недавних входящих сообщений.\n"
                    "Инструкция: /connect")
            elif command == "/chats":
                rows = self.archive.db.execute("""SELECT * FROM chats WHERE owner_id=?
                    ORDER BY last_seen DESC LIMIT 30""", (owner,)).fetchall()
                summary = "\n".join(
                    f"{row['chat_id']} — {row['title']}"
                    + (" 🔇" if self.archive.is_muted(owner, row["chat_id"]) else "") for row in rows)
                await self.send_text(chat_id, "Последние известные чаты:\n" + (summary or
                    "Пока пусто. Подключите бота и дождитесь нового сообщения в разрешённом чате."))
            elif command == "/archive":
                await self.show_archive(owner, argument)
            elif command == "/stop":
                self.archive.pause_user(owner)
                await self.stop_tasks(owner)
                await self.send_text(chat_id, "Обработка и отправки остановлены. Архив сохранён. "
                                     "Возобновить: /start. Отозвать доступ: настройки Telegram → Чат-боты.")
            elif command == "/erase":
                if argument != "confirm":
                    await self.send_text(chat_id, "Чтобы удалить свой архив с сервера и остановить "
                                         "обработку, отправьте /erase confirm. Сообщения, уже доставленные "
                                         "в этот диалог, удалите отдельно в Telegram.")
                else:
                    self.archive.pause_user(owner)
                    await self.stop_tasks(owner)
                    ids = [row[0] for row in self.archive.db.execute(
                        "SELECT id FROM connections WHERE owner_id=?", (owner,))]
                    locks = [self.locks[cid] for cid in sorted(ids)]
                    for lock in locks:
                        await lock.acquire()
                    try:
                        self.archive.erase(owner)
                    finally:
                        for lock in reversed(locks):
                            lock.release()
                    await self.send_text(chat_id, "Архив и настройки удалены с сервера. "
                                         "Обработка выключена. Отзовите подключение в Telegram. "
                                         "Ранее отправленные уведомления остаются в этом диалоге.")
            elif command in {"/mute", "/unmute", "/spam", "/stopspam"}:
                connection = self.archive.active_connection(owner)
                if not connection:
                    raise ValueError("Нет активного подключения. Инструкция: /connect")
                self.require_connection(connection["id"])
                args = argument.split(maxsplit=2)
                try:
                    target = int(args[0])
                except (ValueError, IndexError) as exc:
                    raise ValueError("Укажите ID своего чата из /chats после команды.") from exc
                if not self.archive.known_chat(owner, target) or target == owner:
                    raise ValueError("Чат не найден среди ваших подключённых переписок: /chats")
                if command in {"/mute", "/unmute"}:
                    enabled = command == "/mute"
                    if enabled:
                        self.require_connection(connection["id"], delete=True)
                    self.archive.mute(owner, target, enabled)
                    await self.send_text(chat_id, "🔇 Мут активирован" if enabled else "🔊 Мут отключён")
                elif command == "/stopspam":
                    await self.stop_spam((owner, "business", target))
                    await self.send_text(chat_id, "⏹ Отправка остановлена")
                else:
                    if len(args) != 3:
                        raise ValueError("Использование: /spam ID количество текст")
                    count = self.parse_count(args[1])
                    self.launch_spam(owner, target, count, args[2], connection["id"])
                    await self.send_text(chat_id, f"✅ Запущена отправка {count} сообщений")
            elif command == ".spam":
                if not user["active"]:
                    raise ValueError("Обработка остановлена. Возобновить: /start")
                args = argument.split(maxsplit=1)
                if len(args) != 2:
                    raise ValueError("Использование для теста здесь: .spam количество текст")
                count = self.parse_count(args[0], test=True)
                self.launch_spam(owner, chat_id, count, args[1])
                await self.send_text(chat_id, "✅ Запущен тест в диалоге с Veltrix")
            elif command == ".stopspam":
                await self.stop_spam((owner, "test", chat_id))
                await self.send_text(chat_id, "⏹ Тест остановлен")
            else:
                await self.send_text(chat_id, "Команды: /help. Подключение: /connect.")
        except ValueError as exc:
            await self.send_text(chat_id, f"⚠️ {exc}")

    async def show_archive(self, owner, argument):
        try:
            page = int(argument or "1")
            if page < 1:
                raise ValueError
        except ValueError as exc:
            raise ValueError("Использование: /archive или /archive 2; номер страницы >= 1.") from exc
        count = self.archive.db.execute("SELECT COUNT(*) FROM messages WHERE owner_id=?",
                                        (owner,)).fetchone()[0]
        pages = max(1, (count + PAGE_SIZE - 1) // PAGE_SIZE)
        if page > pages:
            raise ValueError(f"Страниц в архиве: {pages}.")
        rows = self.archive.db.execute("""SELECT * FROM messages WHERE owner_id=?
            ORDER BY date DESC, message_id DESC, chat_id DESC LIMIT ? OFFSET ?""",
                                       (owner, PAGE_SIZE, (page - 1) * PAGE_SIZE)).fetchall()
        user = self.archive.user(owner)
        await self.send_text(user["chat_id"], f"Архив: страница {page}/{pages}; сообщений: {count}.")
        for row in rows:
            await self.send_text(user["chat_id"], self.archive.describe(row))
            if row["photo_path"] and Path(row["photo_path"]).is_file():
                await self.telegram(lambda row=row: self.bot.send_photo(
                    user["chat_id"], FSInputFile(row["photo_path"]),
                    caption=f"Сохранённое фото: сообщение {row['message_id']}"))
        if page < pages:
            await self.send_text(user["chat_id"], f"Далее: /archive {page + 1}")

    async def deliver_notifications(self):
        rows = self.archive.db.execute("""SELECT n.*, u.chat_id FROM notifications n
            JOIN users u ON u.owner_id=n.owner_id
            WHERE n.delivered=0 AND n.retry_at<=? AND u.active=1
            ORDER BY n.id LIMIT 20""", (time.time(),)).fetchall()
        for row in rows:
            def eligible(row=row):
                user = self.archive.user(row["owner_id"])
                exists = self.archive.db.execute(
                    "SELECT 1 FROM notifications WHERE id=?", (row["id"],)).fetchone()
                return user and user["active"] and exists
            if not eligible():
                continue
            try:
                offset = row["text_offset"]
                while offset < len(row["text"]):
                    async def send_piece(row=row, offset=offset, eligible=eligible):
                        if not eligible():
                            raise ValueError("Notification paused or erased")
                        return await self.bot.send_message(
                            row["chat_id"], row["text"][offset:offset + 1000], parse_mode=None)
                    await self.telegram(send_piece)
                    offset += 1000
                    with self.archive.db:
                        self.archive.db.execute("UPDATE notifications SET text_offset=? WHERE id=?",
                                                (offset, row["id"]))
                if row["photo_path"]:
                    async def send_photo(row=row, eligible=eligible):
                        if not eligible():
                            raise ValueError("Notification paused or erased")
                        if Path(row["photo_path"]).is_file():
                            return await self.bot.send_photo(
                                row["chat_id"], FSInputFile(row["photo_path"]))
                        return await self.bot.send_message(
                            row["chat_id"], "⚠️ Фотография отсутствует на сервере.")
                    await self.telegram(send_photo)
                with self.archive.db:
                    self.archive.db.execute("UPDATE notifications SET delivered=1 WHERE id=?", (row["id"],))
            except TelegramForbiddenError:
                self.archive.pause_user(row["owner_id"])
                await self.stop_tasks(row["owner_id"])
            except Exception as exc:
                LOG.warning("Notification delayed: %s", type(exc).__name__)
                with self.archive.db:
                    self.archive.db.execute("UPDATE notifications SET retry_at=? WHERE id=?",
                                            (time.time() + 60, row["id"]))

    async def notifications(self):
        while True:
            try:
                await self.deliver_notifications()
            except Exception as exc:
                LOG.error("Notification worker: %s", type(exc).__name__)
            await asyncio.sleep(1)

    def guarded(self, handler):
        async def wrapped(event):
            if self.closing:
                return
            task = asyncio.current_task()
            self.handler_tasks.add(task)
            try:
                await handler(event)
            except Exception as exc:
                # Exception strings may contain URLs/tokens or message contents.
                LOG.error("%s failed: %s", handler.__name__, type(exc).__name__)
            finally:
                self.handler_tasks.discard(task)
        return wrapped

    def register(self, dispatcher):
        dispatcher.message.register(self.guarded(self.private))
        dispatcher.business_connection.register(self.guarded(self.connected))
        dispatcher.business_message.register(self.guarded(self.business_message))
        dispatcher.edited_business_message.register(self.guarded(self.edited))
        dispatcher.deleted_business_messages.register(self.guarded(self.deleted))

    async def close(self):
        self.closing = True
        handlers = list(self.handler_tasks)
        for task in handlers:
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
        await self.stop_tasks()
        if self.notification_task:
            self.notification_task.cancel()
            await asyncio.gather(self.notification_task, return_exceptions=True)


def configuration():
    load_dotenv(ROOT / ".env")
    token = os.environ.get("BOT_TOKEN", "").strip()
    try:
        delay = float(os.environ.get("SPAM_DELAY", "1.5"))
        if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]{20,}", token):
            raise ValueError
        if not math.isfinite(delay) or delay < 1:
            raise ValueError
    except ValueError as exc:
        raise ValueError("Укажите настоящий BOT_TOKEN в .env; SPAM_DELAY должен быть числом >= 1.") from exc
    return token, delay


async def main():
    token, delay = configuration()
    archive = Archive(ROOT / "data")
    bot = Bot(token)
    app = Veltrix(bot, archive, delay)
    dispatcher = Dispatcher()
    app.register(dispatcher)
    try:
        while True:
            try:
                me = await app.telegram(bot.get_me)
                await app.telegram(lambda: bot.delete_webhook(drop_pending_updates=False))
                break
            except TelegramNetworkError:
                LOG.warning("Telegram unavailable at startup; retrying in 3 seconds")
                await asyncio.sleep(3)
        if not me.can_connect_to_business:
            print("Внимание: включите Secretary Mode / Business Mode у бота в @BotFather.")
        app.notification_task = asyncio.create_task(app.notifications())
        print(f"Veltrix запущен: @{me.username}. Остановка: Ctrl+C.")
        await dispatcher.start_polling(bot, allowed_updates=[
            "message", "business_connection", "business_message",
            "edited_business_message", "deleted_business_messages",
        ], close_bot_session=False)
    finally:
        await app.close()
        await bot.session.close()
        archive.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("aiogram").setLevel(logging.CRITICAL)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Veltrix остановлен.")
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(1) from None
    except Exception as exc:
        LOG.error("Veltrix stopped: %s", type(exc).__name__)
        raise SystemExit(1) from None
