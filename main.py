"""Veltrix: a multi-user Telegram Secretary bot. Python 3.11+."""

import asyncio
import hashlib
import json
import logging
import math
import os
import re
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
from aiogram.types import FSInputFile, LinkPreviewOptions, Message
from dotenv import load_dotenv

import interface as ui
from storage import Archive

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("veltrix")
HELP = ui.HELP
INSTRUCTIONS = ui.CONNECT
COMMANDS = {".mute", ".unmute", ".spam", ".stopspam", ".speed", ".help"}


class Veltrix:
    def __init__(self, bot, archive, delay=0.1):
        self.bot, self.archive, self.delay = bot, archive, delay
        self.spam_tasks = {}
        self.media_tasks = {}
        self.mute_tasks = {}
        self.delete_started = {}
        self.handler_tasks = set()
        self.locks = defaultdict(asyncio.Lock)
        self.owner_locks = defaultdict(asyncio.Lock)
        self.cooldowns = defaultdict(float)
        self.media_slots = asyncio.Semaphore(3)
        self.notification_task = None
        self.maintenance_task = None
        self.wakeup = asyncio.Event()
        self.closing = False
        self.username = ""

    @property
    def flood_until(self):
        return self.cooldowns["send"]

    @flood_until.setter
    def flood_until(self, value):
        self.cooldowns["send"] = value

    async def telegram(self, operation, *, lane="send"):
        # A sendMessage cooldown must not artificially postpone deleteBusinessMessages.
        # Each method family still observes every retry_after it receives.
        while True:
            wait = self.cooldowns[lane] - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue
            try:
                return await operation()
            except TelegramRetryAfter as exc:
                seconds = max(0, exc.retry_after) + 0.05
                self.cooldowns[lane] = max(self.cooldowns[lane], time.monotonic() + seconds)
                LOG.warning("Telegram cooldown: %s, %.2fs", lane, seconds)

    async def send_text(self, chat_id, text, **kwargs):
        for offset in range(0, len(text), 1000):
            await self.telegram(lambda offset=offset: self.bot.send_message(
                chat_id, text[offset:offset + 1000], parse_mode=None, **kwargs))

    async def card(self, chat_id, text, markup=None, *, message_id=None):
        options = dict(parse_mode="HTML", reply_markup=markup,
                       link_preview_options=LinkPreviewOptions(is_disabled=True))
        if message_id is not None:
            try:
                return await self.telegram(lambda: self.bot.edit_message_text(
                    text=text, chat_id=chat_id, message_id=message_id, **options))
            except TelegramBadRequest as exc:
                if "message is not modified" in exc.message.lower():
                    return None
                raise
        return await self.telegram(lambda: self.bot.send_message(chat_id, text, **options))

    def require_connection(self, cid, *, reply=False, delete=False):
        connection = self.archive.connection(cid)
        user = self.archive.user(connection["owner_id"]) if connection else None
        if not connection or not connection["enabled"] or not user or not user["active"]:
            raise ValueError("Подключение выключено. Проверьте /status и настройки Telegram.")
        if reply and not connection["rights"].get("can_reply"):
            raise ValueError("Разрешите ответы от вашего имени в настройках подключения.")
        if delete and not connection["rights"].get("can_delete_all_messages"):
            raise ValueError("Для мута разрешите удаление всех сообщений в выбранных чатах.")
        return connection

    async def resolve_connection(self, cid):
        connection = self.archive.connection(cid)
        if not connection:
            data = await self.telegram(lambda: self.bot.get_business_connection(cid), lane="control")
            self.archive.save_connection(data)
            connection = self.archive.connection(cid)
        return connection

    def enqueue(self, owner, text, **kwargs):
        self.archive.enqueue(owner, text, **kwargs)
        self.wakeup.set()

    async def connected(self, connection):
        self.archive.save_connection(connection)
        await self.stop_tasks(connection.user.id, all_jobs=True)
        if self.archive.user(connection.user.id):
            status = "🟢 Veltrix подключён. Новая переписка будет сохраняться здесь." if connection.is_enabled else (
                "⚪️ Подключение отключено. Сохранённые переписки остаются в архиве.")
            self.enqueue(connection.user.id, status)

    @staticmethod
    def parse_delay(value):
        try:
            delay = float(str(value).replace(",", "."))
            if not math.isfinite(delay) or delay < 0:
                raise ValueError
            return delay
        except ValueError as exc:
            raise ValueError("Пауза должна быть конечным числом от 0 секунд.") from exc

    @staticmethod
    def parse_count(value, test=False):
        if value == "forever":
            if test:
                return None
            raise ValueError("Непрерывный тест доступен только в диалоге с Veltrix.")
        try:
            count = int(value)
            if count <= 0:
                raise ValueError
            return count
        except ValueError as exc:
            raise ValueError("Количество должно быть положительным целым числом.") from exc

    def delay_for(self, owner):
        user = self.archive.user(owner)
        return self.delay if not user or user["spam_delay"] is None else user["spam_delay"]

    def chat_name(self, owner, chat_id):
        chat = self.archive.known_chat(owner, chat_id)
        return chat["title"] if chat else "Переписка"

    def _schedule_media(self, row):
        key = (row["owner_id"], row["chat_id"], row["message_id"])
        if key not in self.media_tasks and row["photo_state"] in {"pending", "retry"} and row["photo_retry_at"] <= time.time():
            self.media_tasks[key] = asyncio.create_task(self._download_photo(dict(row)))

    async def _download_photo(self, row):
        owner, chat_id, mid = row["owner_id"], row["chat_id"], row["message_id"]
        key = (owner, chat_id, mid)
        digest = hashlib.sha256(row["photo_id"].encode()).hexdigest()[:16]
        target = self.archive.media / f"{owner}_{chat_id}_{mid}_{digest}.jpg"
        partial = target.with_name(f"{target.stem}_{id(asyncio.current_task()):x}.part")
        try:
            async with self.media_slots:
                file = await self.telegram(lambda: self.bot.get_file(row["photo_id"]), lane="media")
                if not file.file_path:
                    raise ValueError("Photo download path unavailable")
                await self.telegram(lambda: self.bot.download_file(file.file_path, destination=partial), lane="media")
                latest = self.archive.get(owner, chat_id, mid)
                if not latest or latest["photo_id"] != row["photo_id"]:
                    return
                if not partial.is_file():
                    raise OSError("Photo was not downloaded")
                partial.replace(target)
                self.archive.finish_photo(row, str(target.resolve()))
                latest = self.archive.get(owner, chat_id, mid)
                if latest and latest["delete_pending"]:
                    self._schedule_mute(latest)
                self.wakeup.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("Photo archive failed: %s", type(exc).__name__)
            self.archive.finish_photo(row)
            latest = self.archive.get(owner, chat_id, mid)
            if latest and latest["delete_pending"]:
                self._schedule_mute(latest)
        finally:
            partial.unlink(missing_ok=True)
            if self.media_tasks.get(key) is asyncio.current_task():
                self.media_tasks.pop(key, None)

    def _schedule_mute(self, row):
        owner, chat_id, mid = row["owner_id"], row["chat_id"], row["message_id"]
        if row["deleted"] or not row["delete_pending"]:
            return
        user = self.archive.user(owner)
        if not user or not user["active"]:
            return
        connection = self.archive.connection(row["connection_id"])
        if not connection or not connection["enabled"] or not connection["rights"].get("can_delete_all_messages"):
            self.archive.suppress(owner, chat_id, mid, bool(row["suppressed"]), pending=False)
            return
        if user["mute_mode"] == "safe" and row["photo_state"] in {"pending", "retry"}:
            return
        if user["mute_mode"] == "safe" and row["photo_state"] == "failed":
            self.archive.suppress(owner, chat_id, mid, False, pending=False)
            self.enqueue(owner, f"Не удалось скачать фото из переписки «{self.chat_name(owner, chat_id)}». "
                         "В режиме сохранения фото оригинал оставлен на месте.")
            return
        key = (owner, chat_id, mid)
        if key not in self.mute_tasks:
            self.mute_tasks[key] = asyncio.create_task(self._mute_message(dict(row)))

    async def _mute_message(self, row):
        owner, chat_id, mid = row["owner_id"], row["chat_id"], row["message_id"]
        key = (owner, chat_id, mid)
        try:
            async def delete():
                latest = self.archive.get(owner, chat_id, mid)
                if not latest or latest["deleted"] or not self.archive.is_muted(owner, chat_id):
                    return False
                self.require_connection(row["connection_id"], delete=True)
                self.archive.suppress(owner, chat_id, mid)
                self.delete_started[key] = asyncio.current_task()
                return await self.bot.delete_business_messages(row["connection_id"], [mid])
            result = await self.telegram(delete, lane="delete")
            latest = self.archive.get(owner, chat_id, mid)
            if result is False:
                self.archive.suppress(owner, chat_id, mid, bool(row["suppressed"]), pending=False)
            elif latest:
                self.archive.mark_deleted(latest)
        except asyncio.CancelledError:
            # A request can reach Telegram before local cancellation. Keep its marker.
            raise
        except (TelegramBadRequest, TelegramForbiddenError, ValueError):
            self.archive.suppress(owner, chat_id, mid, bool(row["suppressed"]), pending=False)
            if self.archive.is_muted(owner, chat_id):
                self.enqueue(owner, f"⚠️ Telegram не разрешил удалить сообщение в переписке "
                             f"«{self.chat_name(owner, chat_id)}». Копия сохранена. Проверьте права в /status.")
        except Exception as exc:
            LOG.warning("Mute request failed: %s", type(exc).__name__)
            # Avoid tight retries and retain suppression on an uncertain RPC result.
            with self.archive.db:
                self.archive.db.execute("UPDATE messages SET delete_pending=0 WHERE owner_id=? AND chat_id=? AND message_id=?", key)
            self.enqueue(owner, f"⚠️ Не удалось подтвердить удаление в переписке «{self.chat_name(owner, chat_id)}». "
                         "Копия сохранена; результат запроса неизвестен из-за сетевой ошибки.")
        finally:
            if self.delete_started.get(key) is asyncio.current_task():
                self.delete_started.pop(key, None)
            if self.mute_tasks.get(key) is asyncio.current_task():
                self.mute_tasks.pop(key, None)

    async def archive_message(self, owner, message, *, edited=False, mute_pending=False):
        photo = message.photo[-1].file_id if message.photo else None
        unsupported = message.has_protected_content or getattr(message, "ttl_seconds", None)
        if unsupported:
            photo, note = None, "Защищённое или одноразовое медиа не копируется"
        elif photo:
            note = "Фото"
        elif message.text is None:
            note = "Вложение: сохранены текст и подпись, файл не скачивается"
        else:
            note = ""
        previous = self.archive.save_message(owner, message, note, photo, edited=edited, mute_pending=mute_pending)
        return previous, True

    async def business_message(self, message, edited=False):
        cid = message.business_connection_id
        if not cid or message.chat.type != "private" or not message.from_user:
            return
        async with self.locks[cid]:
            connection = await self.resolve_connection(cid)
            owner = connection["owner_id"]
            user = self.archive.user(owner)
            if not connection["enabled"] or not user or not user["active"]:
                return
            mute_pending = (not edited and message.from_user.id != owner and not message.sender_business_bot
                            and self.archive.is_muted(owner, message.chat.id)
                            and connection["rights"].get("can_delete_all_messages"))
            await self.archive_message(owner, message, edited=edited, mute_pending=bool(mute_pending))
            row = self.archive.get(owner, message.chat.id, message.message_id)
            outgoing = bool(row["outgoing"])
            # Start deletion immediately after the durable metadata commit, before downloads.
            if not outgoing and not edited and not row["deleted"] and self.archive.is_muted(owner, message.chat.id):
                if connection["rights"].get("can_delete_all_messages"):
                    self._schedule_mute(row)
            self._schedule_media(row)
        if edited:
            self.wakeup.set()
        elif outgoing and not message.sender_business_bot:
            await self.business_command(message, connection)

    async def edited(self, message):
        await self.business_message(message, edited=True)

    async def deleted(self, event):
        async with self.locks[event.business_connection_id]:
            connection = await self.resolve_connection(event.business_connection_id)
            user = self.archive.user(connection["owner_id"])
            if not user or not user["active"] or not connection["enabled"]:
                return
            self.archive.delete_batch(connection["owner_id"], event.chat.id, event.message_ids)
            self.wakeup.set()

    async def business_status(self, message, connection, text):
        try:
            async def edit():
                self.require_connection(connection["id"], reply=True)
                return await self.bot.edit_message_text(
                    text=text, chat_id=message.chat.id, message_id=message.message_id,
                    business_connection_id=connection["id"], parse_mode=None)
            result = await self.telegram(edit)
            if isinstance(result, Message) and result.from_user:
                result = result.model_copy(update={"business_connection_id": connection["id"]})
                await self.archive_message(connection["owner_id"], result)
        except (TelegramAPIError, ValueError):
            self.enqueue(connection["owner_id"], f"{self.chat_name(connection['owner_id'], message.chat.id)}\n{text}")

    async def set_mute(self, owner, chat_id, enabled):
        self.archive.mute(owner, chat_id, enabled)
        if not enabled:
            for key, task in list(self.mute_tasks.items()):
                if key[:2] == (owner, chat_id):
                    row = self.archive.get(*key)
                    started = key in self.delete_started or bool(row and row["suppressed"])
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    self.archive.suppress(*key, enabled=started, pending=False)
            with self.archive.db:
                self.archive.db.execute("UPDATE messages SET delete_pending=0 WHERE owner_id=? AND chat_id=?", (owner, chat_id))

    async def business_command(self, message, connection):
        parts = (message.text or "").split(maxsplit=2)
        if not parts or parts[0] not in COMMANDS or message.from_user.id != connection["owner_id"]:
            return
        owner, chat_id = connection["owner_id"], message.chat.id
        try:
            self.require_connection(connection["id"])
            if parts[0] == ".help":
                status = "Veltrix\n.mute · .unmute\n.spam количество текст · .stopspam\n.speed секунды\nПолная справка — в диалоге с ботом."
            elif parts[0] in {".mute", ".unmute"}:
                enabled = parts[0] == ".mute"
                if enabled:
                    self.require_connection(connection["id"], delete=True)
                await self.set_mute(owner, chat_id, enabled)
                status = "🔇 Мут активирован" if enabled else "🔊 Мут отключён"
            elif parts[0] == ".stopspam":
                await self.stop_spam((owner, "business", chat_id))
                status = "⏹ Отправка остановлена"
            elif parts[0] == ".speed":
                if len(parts) != 2:
                    raise ValueError("Пример: .speed 0.1. Значение 0 убирает дополнительную паузу.")
                delay = self.parse_delay(parts[1])
                self.archive.preference(owner, delay=delay)
                status = f"⚡ Пауза между сообщениями: {delay:g} с"
            else:
                if len(parts) != 3:
                    raise ValueError("Использование: .spam количество текст")
                count = self.parse_count(parts[1])
                self.launch_spam(owner, chat_id, count, parts[2], connection["id"])
                status = f"✅ Запущена отправка {count} сообщений"
        except ValueError as exc:
            status = f"⚠️ {exc}"
        await self.business_status(message, connection, status)

    def launch_spam(self, owner, chat_id, count, text, connection_id=None):
        if not text.strip() or len(text.encode("utf-16-le")) // 2 > 4096:
            raise ValueError("Нужен непустой текст в пределах лимита одного сообщения Telegram.")
        if connection_id:
            connection = self.require_connection(connection_id, reply=True)
            if connection["owner_id"] != owner:
                raise ValueError("Подключение недоступно")
            if count is None:
                raise ValueError("Непрерывная отправка разрешена только для теста в этом боте.")
        elif not self.archive.user(owner) or chat_id != self.archive.user(owner)["chat_id"]:
            raise ValueError("Тест доступен только в своём диалоге с Veltrix")
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
                        raise ValueError("Обработка приостановлена")
                    if connection_id:
                        self.require_connection(connection_id, reply=True)
                    return await self.bot.send_message(chat_id, text, business_connection_id=connection_id, parse_mode=None)
                result = await self.telegram(send)
                if connection_id and isinstance(result, Message) and result.from_user:
                    result = result.model_copy(update={"business_connection_id": connection_id})
                    await self.archive_message(owner, result)
                sent += 1
                if count is None or sent < count:
                    # Always yield even at zero delay, so stop commands can interrupt.
                    await asyncio.sleep(self.delay_for(owner))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("Sending stopped: %s", type(exc).__name__)
            self.enqueue(owner, f"⚠️ Отправка остановлена\n{self.chat_name(owner, chat_id)}\n"
                         f"Подтверждено сообщений: {sent}. Проверьте доступность чата и права. "
                         "При сетевом сбое последнее сообщение могло уйти без подтверждения.")
        finally:
            if self.spam_tasks.get(key) is asyncio.current_task():
                self.spam_tasks.pop(key, None)

    async def stop_spam(self, key):
        task = self.spam_tasks.pop(key, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def stop_tasks(self, owner=None, *, all_jobs=False):
        groups = [self.spam_tasks] + ([self.mute_tasks, self.media_tasks] if all_jobs else [])
        pending = []
        for group in groups:
            for key, task in list(group.items()):
                if owner is None or key[0] == owner:
                    group.pop(key, None)
                    task.cancel()
                    pending.append(task)
        await asyncio.gather(*pending, return_exceptions=True)

    async def show_home(self, owner, *, message_id=None, start=False):
        user = self.archive.user(owner)
        connected = self.archive.active_connection(owner)
        text = ui.home(bool(connected), bool(user["active"]))
        if not connected:
            text += "\n\nОткройте «Подключение»: выберите бота и разрешённые чаты в настройках Telegram."
            text += "\nНовые сообщения сохраняются на сервере Veltrix. Подробнее — в инструкции подключения."
        markup = ui.MENU if start else ui.keyboard([
            [("💬 Переписки", "chats:0"), ("⚙️ Настройки", "settings")],
            [("🔌 Подключение", "connect"), ("❔ Помощь", "help")]])
        await self.card(user["chat_id"], text, markup, message_id=message_id)

    async def show_chats(self, owner, page=0, *, message_id=None):
        if page < 0:
            raise ValueError("Страница должна быть положительной")
        rows, count = self.archive.chats(owner, page)
        pages = max(1, (count + 7) // 8)
        if page >= pages:
            raise ValueError(f"В архиве страниц: {pages}")
        text = "<b>Ваши переписки</b>\nВыберите диалог — внутри история, фото и скачивание архива."
        buttons = []
        for row in rows:
            muted = "🔇 " if self.archive.is_muted(owner, row["chat_id"]) else ""
            label = f"{muted}{ui.short(row['title'], 32)} · {row['total']}"
            buttons.append([(label, f"chat:{row['key']}:-1")])
        if not rows:
            text += "\n\nПока пусто. Подключите бота и дождитесь нового сообщения в разрешённой переписке."
        navigation = []
        if page:
            navigation.append(("‹ Назад", f"chats:{page - 1}"))
        if page + 1 < pages:
            navigation.append(("Далее ›", f"chats:{page + 1}"))
        if navigation:
            buttons.append(navigation)
        buttons.append([("⌂ Главное меню", "home")])
        await self.card(self.archive.user(owner)["chat_id"], text, ui.keyboard(buttons), message_id=message_id)

    async def show_archive(self, owner, argument="", *, message_id=None):
        try:
            page = int(argument or "1") - 1
        except ValueError as exc:
            raise ValueError("Пример: /archive или /archive 2") from exc
        await self.show_chats(owner, page, message_id=message_id)

    async def show_conversation(self, owner, key, page=-1, *, message_id=None):
        chat, rows, total, page, pages = self.archive.conversation(owner, key, page)
        muted = self.archive.is_muted(owner, chat["chat_id"])
        buttons = [[("🔊 Отключить мут" if muted else "🔇 Включить мут", f"mute:{key}"),
                    ("⏹ Остановить отправку", f"stop:{key}")],
                   [("📥 Скачать всю переписку", f"export:{key}")]]
        for row in rows:
            if row["photo_state"] == "ready":
                label = "📷 Фото · " + ui.stamp(row["date"])
                buttons.append([(label, f"photo:{key}:{row['message_id']}")])
        nav = []
        if page:
            nav.append(("‹ Раньше", f"chat:{key}:{page - 1}"))
        if page + 1 < pages:
            nav.append(("Позже ›", f"chat:{key}:{page + 1}"))
        if nav:
            buttons.append(nav)
        if pages > 1:
            buttons.append([("В начало", f"chat:{key}:0"), ("Последние", f"chat:{key}:-1")])
        buttons.append([("‹ Все переписки", "chats:0")])
        await self.card(self.archive.user(owner)["chat_id"], ui.conversation(chat, rows, total, page, pages),
                        ui.keyboard(buttons), message_id=message_id)

    async def show_settings(self, owner, *, message_id=None):
        user = self.archive.user(owner)
        delay = self.delay_for(owner)
        fast = user["mute_mode"] == "fast"
        description = ("Запись и file_id сохраняются до удаления; фото скачивается в фоне." if fast else
                       "Фото сначала скачивается на сервер. Удаление ждёт загрузки.")
        text = (f"<b>Настройки</b>\n\n<b>Отправка</b>\nДополнительная пауза: <b>{delay:g} с</b>.\n"
                "0 — без дополнительной паузы. Скорость всё равно зависит от API и ожиданий Telegram.\n\n"
                f"<b>Мут: {'быстрый' if fast else 'сначала сохранить фото'}</b>\n{description}\n\n"
                "Мгновенное удаление без задержки сети невозможно.")
        rows = [[("0 с", "speed:0"), ("0.1 с", "speed:0.1"), ("0.3 с", "speed:0.3"), ("1 с", "speed:1")],
                [("✓ Быстрый мут" if fast else "Быстрый мут", "mode:fast")],
                [("Сначала сохранить фото" if fast else "✓ Сначала сохранить фото", "mode:safe")],
                [("О данных", "privacy"), ("⌂ Меню", "home")]]
        await self.card(user["chat_id"], text, ui.keyboard(rows), message_id=message_id)

    async def show_status(self, owner, *, message_id=None):
        user = self.archive.user(owner)
        connection = self.archive.active_connection(owner)
        rights = connection["rights"] if connection else {}
        def yes(value):
            return "✓" if value else "—"
        text = ("<b>Подключение</b>\n\n"
                f"{yes(connection)} Связь с Telegram\n{yes(user['active'])} Обработка сообщений\n"
                f"{yes(rights.get('can_reply'))} Ответы от вашего имени\n"
                f"{yes(rights.get('can_delete_all_messages'))} Удаление входящих\n\n"
                "Только выбранные вами чаты. Для отправки нужны недавние входящие сообщения.")
        await self.card(user["chat_id"], text, ui.keyboard([
            [("Как подключить", "connect"), ("⌂ Меню", "home")]]), message_id=message_id)

    async def show_info(self, owner, page, *, message_id=None):
        text = {"help": ui.HELP, "connect": ui.CONNECT, "privacy": ui.PRIVACY}[page]
        if page == "connect" and self.username:
            text += f"\n\nВаш бот: <code>@{ui.esc(self.username)}</code>"
        await self.card(self.archive.user(owner)["chat_id"], text, ui.keyboard([
            [("Проверить подключение", "status"), ("О данных", "privacy")], [("⌂ Меню", "home")]]), message_id=message_id)

    async def export_chat(self, owner, key):
        async with self.owner_locks[owner]:
            chat, rows, revisions = self.archive.snapshot(owner, key)
            user = self.archive.user(owner)
            task = asyncio.create_task(asyncio.to_thread(
                ui.build_export, self.archive.exports, owner, chat, rows, revisions, self.archive.media))
            path = None
            try:
                try:
                    path, photos = await asyncio.shield(task)
                except asyncio.CancelledError:
                    result = await task
                    result[0].unlink(missing_ok=True)
                    raise
                caption = f"{ui.short(chat['title'], 120)}\nСохранено: {ui.messages_count(len(rows))}. "
                caption += "Внутри HTML-переписка и сохранённые фотографии." if photos else (
                    "Весь текст сохранён. Фотографии не поместились в один файл и доступны в боте.")
                await self.telegram(lambda: self.bot.send_document(user["chat_id"], FSInputFile(path), caption=caption))
            finally:
                if path:
                    path.unlink(missing_ok=True)

    async def erase_owner(self, owner):
        self.archive.pause_user(owner)
        await self.stop_tasks(owner, all_jobs=True)
        async with self.owner_locks[owner]:
            ids = [row[0] for row in self.archive.db.execute("SELECT id FROM connections WHERE owner_id=?", (owner,))]
            locks = [self.locks[cid] for cid in sorted(ids)]
            for lock in locks:
                await lock.acquire()
            try:
                self.archive.erase(owner)
            finally:
                for lock in reversed(locks):
                    lock.release()

    async def private(self, message):
        if (message.chat.type != "private" or not message.from_user or message.from_user.is_bot
                or message.from_user.id != message.chat.id):
            return
        owner = message.from_user.id
        text = {"💬 Переписки": "/archive", "⚙️ Настройки": "/settings", "🔌 Подключение": "/connect",
                "❔ Помощь": "/help"}.get(message.text, message.text or "")
        parts = text.split(maxsplit=1)
        if not parts:
            return
        command, argument = parts[0].split("@")[0], parts[1] if len(parts) > 1 else ""
        if command == "/start":
            self.archive.start_user(owner, message.chat.id)
            await self.show_home(owner, start=True)
            return
        user = self.archive.user(owner)
        if not user:
            await self.send_text(message.chat.id, "Нажмите /start, чтобы открыть Veltrix.")
            return
        try:
            if command in {"/connect", "/privacy", "/help", ".help"}:
                await self.show_info(owner, "help" if command == ".help" else command[1:])
            elif command == "/status":
                await self.show_status(owner)
            elif command in {"/archive", "/chats"}:
                await self.show_archive(owner, argument)
            elif command == "/settings":
                await self.show_settings(owner)
            elif command in {"/speed", ".speed"}:
                self.archive.preference(owner, delay=self.parse_delay(argument))
                await self.show_settings(owner)
            elif command == "/stop":
                self.archive.pause_user(owner)
                await self.stop_tasks(owner, all_jobs=True)
                await self.card(message.chat.id, "⏸ <b>Обработка приостановлена</b>\nАрхив сохранён. Возобновить: /start.")
            elif command == "/erase":
                if argument != "confirm":
                    await self.card(message.chat.id, "<b>Удаление своего архива</b>\nОтправьте <code>/erase confirm</code>. "
                                    "Уже доставленные сообщения в Telegram удаляются отдельно.")
                else:
                    await self.erase_owner(owner)
                    await self.card(message.chat.id, "<b>Архив удалён с сервера</b>\nОбработка выключена. "
                                    "Отзовите доступ в настройках Telegram. Копии в этом диалоге остаются.")
            elif command in {"/mute", "/unmute", "/spam", "/stopspam"}:
                connection = self.archive.active_connection(owner)
                if not connection:
                    raise ValueError("Сначала подключите бота: /connect")
                self.require_connection(connection["id"])
                args = argument.split(maxsplit=2)
                try:
                    target = int(args[0])
                except (ValueError, IndexError) as exc:
                    raise ValueError("Выберите переписку кнопкой в разделе «Переписки» или укажите ID после команды.") from exc
                if not self.archive.known_chat(owner, target) or target == owner:
                    raise ValueError("Переписка недоступна")
                if command in {"/mute", "/unmute"}:
                    enabled = command == "/mute"
                    if enabled:
                        self.require_connection(connection["id"], delete=True)
                    await self.set_mute(owner, target, enabled)
                    await self.card(message.chat.id, "🔇 Мут активирован" if enabled else "🔊 Мут отключён")
                elif command == "/stopspam":
                    await self.stop_spam((owner, "business", target))
                    await self.card(message.chat.id, "⏹ Отправка остановлена")
                else:
                    if len(args) != 3:
                        raise ValueError("Пример: /spam ID количество текст")
                    self.launch_spam(owner, target, self.parse_count(args[1]), args[2], connection["id"])
                    await self.card(message.chat.id, "✅ Отправка запущена")
            elif command == ".spam":
                if not user["active"]:
                    raise ValueError("Обработка приостановлена. Возобновить: /start")
                args = argument.split(maxsplit=1)
                if len(args) != 2:
                    raise ValueError("Пример теста здесь: .spam 10 привет")
                self.launch_spam(owner, message.chat.id, self.parse_count(args[0], test=True), args[1])
                await self.card(message.chat.id, "✅ Тест отправки запущен\nОстановка: <code>.stopspam</code>")
            elif command == ".stopspam":
                await self.stop_spam((owner, "test", message.chat.id))
                await self.card(message.chat.id, "⏹ Тест остановлен")
            else:
                await self.show_home(owner)
        except ValueError as exc:
            await self.card(message.chat.id, "⚠️ " + ui.esc(exc))

    async def callback(self, query):
        owner = query.from_user.id
        user = self.archive.user(owner)
        if not user or not query.message or query.message.chat.id != user["chat_id"]:
            await self.telegram(lambda: query.answer("Откройте свой диалог с Veltrix и нажмите /start.", show_alert=True), lane="control")
            return
        answered = False
        try:
            data = (query.data or "").split(":")
            action = data[0]
            key = None
            if action in {"chat", "mute", "stop", "export", "photo"}:
                key = int(data[1])
                chat = self.archive.chat_by_key(owner, key)
                if not chat:
                    raise ValueError("Переписка недоступна")
            # Callback acknowledgements do not sit behind sendMessage cooldowns.
            await self.telegram(lambda: query.answer(text="Готовлю архив…" if action == "export" else None), lane="control")
            answered = True
            mid = query.message.message_id
            if action == "home":
                await self.show_home(owner, message_id=mid)
            elif action in {"connect", "help", "privacy"}:
                await self.show_info(owner, action, message_id=mid)
            elif action == "status":
                await self.show_status(owner, message_id=mid)
            elif action == "settings":
                await self.show_settings(owner, message_id=mid)
            elif action == "speed":
                self.archive.preference(owner, delay=self.parse_delay(data[1]))
                await self.show_settings(owner, message_id=mid)
            elif action == "mode":
                self.archive.preference(owner, mode=data[1])
                await self.show_settings(owner, message_id=mid)
            elif action == "chats":
                await self.show_chats(owner, int(data[1]), message_id=mid)
            elif action == "chat":
                await self.show_conversation(owner, key, int(data[2]), message_id=mid)
            elif action == "mute":
                connection = self.archive.active_connection(owner)
                if not connection:
                    raise ValueError("Подключите бота: /connect")
                enabled = not self.archive.is_muted(owner, chat["chat_id"])
                self.require_connection(connection["id"], delete=enabled)
                await self.set_mute(owner, chat["chat_id"], enabled)
                await self.show_conversation(owner, key, message_id=mid)
            elif action == "stop":
                await self.stop_spam((owner, "business", chat["chat_id"]))
                await self.show_conversation(owner, key, message_id=mid)
            elif action == "export":
                await self.export_chat(owner, key)
            elif action == "photo":
                row = self.archive.get(owner, chat["chat_id"], int(data[2]))
                if not row or not row["photo_path"] or not Path(row["photo_path"]).is_file():
                    raise ValueError("Фото ещё загружается или недоступно")
                await self.telegram(lambda: self.bot.send_photo(user["chat_id"], FSInputFile(row["photo_path"])))
        except (ValueError, IndexError) as exc:
            text = str(exc) if isinstance(exc, ValueError) and not str(exc).startswith("invalid literal") else "Кнопка недоступна. Откройте меню заново."
            if not answered:
                await self.telegram(lambda: query.answer(text=text[:180], show_alert=True), lane="control")
            else:
                await self.card(user["chat_id"], "⚠️ " + ui.esc(text))

    async def deliver_notifications(self):
        rows = self.archive.db.execute("""SELECT n.*,u.chat_id FROM notifications n JOIN users u USING(owner_id)
            WHERE n.delivered=0 AND n.retry_at<=? AND u.active=1 ORDER BY n.id LIMIT 30""", (time.time(),)).fetchall()
        for row in rows:
            owner = row["owner_id"]
            payload = json.loads(row["payload"])
            def eligible(row=row):
                user = self.archive.user(row["owner_id"])
                exists = self.archive.db.execute("SELECT 1 FROM notifications WHERE id=?", (row["id"],)).fetchone()
                return user and user["active"] and exists
            if not eligible():
                continue
            try:
                chat = self.archive.known_chat(owner, payload["chat_id"]) if payload.get("chat_id") else None
                buttons = ui.keyboard([[("💬 Открыть переписку", f"chat:{chat['key']}:-1")]]) if chat else None
                if not row["text_sent"]:
                    async def send_notice(row=row, payload=payload, buttons=buttons, eligible=eligible):
                        if not eligible():
                            raise ValueError("Notification paused or erased")
                        return await self.bot.send_message(row["chat_id"], ui.notice(payload, row["text"]),
                            parse_mode="HTML", reply_markup=buttons, link_preview_options=LinkPreviewOptions(is_disabled=True))
                    await self.telegram(send_notice)
                    with self.archive.db:
                        self.archive.db.execute("UPDATE notifications SET text_sent=1,text_offset=LENGTH(text) WHERE id=?", (row["id"],))
                path = row["photo_path"]
                if payload.get("kind") == "deleted" and len(payload.get("message_ids", [])) == 1:
                    message = self.archive.get(owner, payload["chat_id"], payload["message_ids"][0])
                    if message and message["photo_state"] in {"pending", "retry"}:
                        with self.archive.db:
                            self.archive.db.execute("UPDATE notifications SET retry_at=? WHERE id=?", (time.time() + 0.5, row["id"]))
                        continue  # Deliver other users' notices while the photo is downloading.
                    path = message["photo_path"] if message else None
                    if message and message["photo_id"] and message["photo_state"] == "failed":
                        await self.telegram(lambda row=row: self.bot.send_message(
                            row["chat_id"], "Фотографию не удалось скачать. Сохранённый текст остался в переписке."))
                if path:
                    async def send_photo(path=path, row=row, eligible=eligible):
                        if not eligible():
                            raise ValueError("Notification paused or erased")
                        if Path(path).is_file():
                            return await self.bot.send_photo(row["chat_id"], FSInputFile(path))
                        return await self.bot.send_message(row["chat_id"], "Фото недоступно. Сохранённый текст остался в переписке.")
                    await self.telegram(send_photo)
                with self.archive.db:
                    self.archive.db.execute("UPDATE notifications SET delivered=1 WHERE id=?", (row["id"],))
            except TelegramForbiddenError:
                self.archive.pause_user(owner)
                await self.stop_tasks(owner, all_jobs=True)
            except Exception as exc:
                LOG.warning("Notification delayed: %s", type(exc).__name__)
                with self.archive.db:
                    self.archive.db.execute("UPDATE notifications SET retry_at=? WHERE id=?", (time.time() + 10, row["id"]))

    async def notifications(self):
        while True:
            self.wakeup.clear()
            try:
                await self.deliver_notifications()
                await asyncio.wait_for(self.wakeup.wait(), timeout=0.3)
            except TimeoutError:
                pass
            except Exception as exc:
                LOG.error("Notification worker: %s", type(exc).__name__)
                await asyncio.sleep(1)

    async def maintain_once(self):
        for row in self.archive.pending_deletions():
            self._schedule_mute(row)
        for row in self.archive.pending_photos():
            self._schedule_media(row)

    async def maintenance(self):
        while True:
            try:
                await self.maintain_once()
            except Exception as exc:
                LOG.error("Archive worker: %s", type(exc).__name__)
            await asyncio.sleep(0.3)

    def guarded(self, handler):
        async def wrapped(event):
            if self.closing:
                return
            task = asyncio.current_task()
            self.handler_tasks.add(task)
            try:
                await handler(event)
            except Exception as exc:
                LOG.error("%s failed: %s", handler.__name__, type(exc).__name__)
            finally:
                self.handler_tasks.discard(task)
        return wrapped

    def register(self, dispatcher):
        dispatcher.message.register(self.guarded(self.private))
        dispatcher.callback_query.register(self.guarded(self.callback))
        dispatcher.business_connection.register(self.guarded(self.connected))
        dispatcher.business_message.register(self.guarded(self.business_message))
        dispatcher.edited_business_message.register(self.guarded(self.edited))
        dispatcher.deleted_business_messages.register(self.guarded(self.deleted))

    async def close(self):
        self.closing = True
        workers = [task for task in (self.notification_task, self.maintenance_task) if task]
        for task in workers:
            task.cancel()
        handlers = list(self.handler_tasks)
        for task in handlers:
            task.cancel()
        await asyncio.gather(*workers, *handlers, return_exceptions=True)
        await self.stop_tasks(all_jobs=True)


def configuration():
    load_dotenv(ROOT / ".env")
    token = os.environ.get("BOT_TOKEN", "").strip()
    if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]{20,}", token):
        raise ValueError("Укажите настоящий BOT_TOKEN в локальном .env.")
    return token, Veltrix.parse_delay(os.environ.get("SPAM_DELAY", "0.1"))


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
                me = await app.telegram(bot.get_me, lane="control")
                await app.telegram(lambda: bot.delete_webhook(drop_pending_updates=False), lane="control")
                break
            except TelegramNetworkError:
                LOG.warning("Telegram unavailable at startup; retrying in 3 seconds")
                await asyncio.sleep(3)
        app.username = me.username or ""
        if not me.can_connect_to_business:
            print("Включите Secretary Mode / Business Mode у бота в @BotFather.")
        app.notification_task = asyncio.create_task(app.notifications())
        app.maintenance_task = asyncio.create_task(app.maintenance())
        print(f"Veltrix запущен: @{app.username}. Остановка: Ctrl+C.")
        await dispatcher.start_polling(bot, allowed_updates=[
            "message", "callback_query", "business_connection", "business_message",
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
