import asyncio
import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram import Bot, Dispatcher
from aiogram.client.session.base import BaseSession
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.methods import SendMessage
from aiogram.types import (
    BusinessBotRights,
    BusinessConnection,
    BusinessMessagesDeleted,
    Chat,
    Message,
    PhotoSize,
    Update,
    User,
)

from main import Archive, Veltrix, configuration

NOW = datetime.now(timezone.utc)


def user(uid):
    return User(id=uid, is_bot=False, first_name=f"User {uid}")


def connection(owner=1, cid="c1", enabled=True, reply=True, delete=True):
    return BusinessConnection(id=cid, user=user(owner), user_chat_id=owner,
                              date=NOW, is_enabled=enabled,
                              rights=BusinessBotRights(can_reply=reply,
                                                       can_delete_all_messages=delete))


def message(text="hello", sender=2, chat=2, mid=10, cid="c1", photo=False, **kwargs):
    photos = [PhotoSize(file_id="photo-id", file_unique_id="photo-unique",
                        width=200, height=200)] if photo else None
    return Message(message_id=mid, date=NOW, from_user=user(sender),
                   chat=Chat(id=chat, type="private", first_name=f"Chat {chat}"),
                   text=None if photo else text, caption=text if photo else None,
                   photo=photos, business_connection_id=cid, **kwargs)


def private(text, owner=1):
    return message(text, sender=owner, chat=owner, cid=None, mid=100)


def deletion(cid="c1", chat=2, ids=None):
    return BusinessMessagesDeleted(business_connection_id=cid,
                                   chat=Chat(id=chat, type="private"),
                                   message_ids=ids or [10])


def network_error():
    return TelegramNetworkError(method=SendMessage(chat_id=1, text="test"), message="offline")


class VeltrixTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.archive = Archive(self.temp.name)
        self.archive.start_user(1, 1)
        self.archive.save_connection(connection())
        self.bot = SimpleNamespace(
            send_message=AsyncMock(), edit_message_text=AsyncMock(),
            send_photo=AsyncMock(), delete_business_messages=AsyncMock(),
            get_business_connection=AsyncMock(return_value=connection()),
            get_file=AsyncMock(return_value=SimpleNamespace(file_path="photos/test.jpg")),
            download_file=AsyncMock(),
        )
        self.app = Veltrix(self.bot, self.archive, delay=0)

    async def asyncTearDown(self):
        await self.app.close()
        self.archive.close()
        self.temp.cleanup()

    def pending(self):
        return self.archive.db.execute(
            "SELECT * FROM notifications WHERE delivered=0 ORDER BY id").fetchall()

    async def finish_spam(self):
        tasks = list(self.app.spam_tasks.values())
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks), 1)

    def second_owner(self):
        self.archive.start_user(3, 3)
        self.archive.save_connection(connection(3, "c3"))

    async def test_start_explains_connection_and_storage(self):
        await self.app.private(private("/start", owner=4))
        self.assertTrue(self.archive.user(4)["active"])
        text = "".join(call.args[1] for call in self.bot.send_message.await_args_list)
        self.assertIn("Чат-боты", text)
        self.assertIn("сохраняться на сервере", text)
        self.assertIn("коды входа", text)

    async def test_no_archive_without_start_or_while_paused(self):
        self.archive.save_connection(connection(4, "c4"))
        await self.app.business_message(message(cid="c4"))
        self.assertIsNone(self.archive.get(4, 2, 10))
        self.archive.pause_user(1)
        await self.app.business_message(message())
        self.assertIsNone(self.archive.get(1, 2, 10))

    async def test_only_account_owner_can_issue_dot_commands(self):
        await self.app.business_message(message(".mute", sender=2))
        self.assertFalse(self.archive.is_muted(1, 2))
        self.assertEqual(self.archive.get(1, 2, 10)["text"], ".mute")
        self.bot.edit_message_text.assert_not_awaited()

    async def test_generated_command_echo_is_ignored(self):
        bot_user = User(id=99, is_bot=True, first_name="Veltrix")
        await self.app.business_message(message(".mute", sender=1, sender_business_bot=bot_user))
        self.assertFalse(self.archive.is_muted(1, 2))
        self.bot.edit_message_text.assert_not_awaited()

    async def test_mute_unmute_edit_and_persist(self):
        await self.app.business_message(message(".mute", sender=1))
        self.bot.edit_message_text.assert_awaited_once_with(
            text="🔇 Мут активирован", chat_id=2, message_id=10,
            business_connection_id="c1", parse_mode=None)
        other_db = Archive(self.temp.name)
        try:
            self.assertTrue(other_db.is_muted(1, 2))
        finally:
            other_db.close()
        await self.app.business_message(message(".unmute", sender=1, mid=11))
        self.assertFalse(self.archive.is_muted(1, 2))
        self.assertEqual(self.bot.edit_message_text.await_args.kwargs["text"], "🔊 Мут отключён")

    async def test_missing_delete_right_blocks_mute(self):
        self.archive.save_connection(connection(delete=False))
        await self.app.business_message(message(".mute", sender=1))
        self.assertFalse(self.archive.is_muted(1, 2))
        self.assertIn("удаление", self.bot.edit_message_text.await_args.kwargs["text"])

    async def test_status_falls_back_to_bot_dialog_if_edit_forbidden(self):
        self.archive.save_connection(connection(reply=False))
        await self.app.business_message(message(".mute", sender=1))
        self.assertTrue(self.archive.is_muted(1, 2))
        self.bot.edit_message_text.assert_not_awaited()
        await self.app.deliver_notifications()
        self.assertEqual(self.bot.send_message.await_args.args[0], 1)
        self.assertIn("Мут активирован", self.bot.send_message.await_args.args[1])

    async def test_finite_sending_has_no_ten_message_cap(self):
        await self.app.business_message(message(".spam 21 привет", sender=1))
        await self.finish_spam()
        self.assertEqual(self.bot.send_message.await_count, 21)
        self.assertTrue(all(call.args == (2, "привет")
                            and call.kwargs["business_connection_id"] == "c1"
                            for call in self.bot.send_message.await_args_list))
        self.assertEqual(self.bot.edit_message_text.await_args.kwargs["text"],
                         "✅ Запущена отправка 21 сообщений")

    async def test_infinite_only_in_bot_dialog(self):
        await self.app.business_message(message(".spam forever test", sender=1))
        self.assertFalse(self.app.spam_tasks)
        self.app.delay = 60
        await self.app.private(private(".spam forever test"))
        self.assertIn((1, "test", 1), self.app.spam_tasks)
        await self.app.private(private(".stopspam"))
        self.assertFalse(self.app.spam_tasks)
        self.assertTrue(all(call.args[0] == 1 for call in self.bot.send_message.await_args_list))

    async def test_stop_interrupts_floodwait_without_waiting_for_status(self):
        self.app.business_status = AsyncMock()
        self.app.flood_until = time.monotonic() + 3600
        await self.app.business_message(message(".spam 100 test", sender=1))
        await asyncio.sleep(0)
        await asyncio.wait_for(self.app.business_message(message(".stopspam", sender=1)), 0.5)
        self.assertFalse(self.app.spam_tasks)
        self.bot.send_message.assert_not_awaited()

    async def test_duplicate_task_and_invalid_count_rejected(self):
        for count in ("0", "-1", "1.5", "wrong"):
            await self.app.business_message(message(f".spam {count} text", sender=1))
        self.assertFalse(self.app.spam_tasks)
        self.app.delay = 60
        await self.app.business_message(message(".spam 100 text", sender=1))
        original = self.app.spam_tasks[(1, "business", 2)]
        await self.app.business_message(message(".spam 2 other", sender=1))
        self.assertIs(self.app.spam_tasks[(1, "business", 2)], original)

    async def test_reconnection_keeps_mute_and_revocation_cancels_only_owner(self):
        self.second_owner()
        self.app.delay = 60
        self.archive.mute(1, 2, True)
        self.app.launch_spam(1, 2, 100, "one", "c1")
        self.app.launch_spam(3, 2, 100, "three", "c3")
        await self.app.connected(connection(enabled=False))
        self.assertNotIn((1, "business", 2), self.app.spam_tasks)
        self.assertIn((3, "business", 2), self.app.spam_tasks)
        await self.app.connected(connection(cid="new"))
        self.assertTrue(self.archive.is_muted(1, 2))
        self.assertFalse(self.archive.connection("c1")["enabled"])

    async def test_unknown_connection_is_fetched(self):
        self.archive.start_user(4, 4)
        self.bot.get_business_connection.return_value = connection(4, "c4")
        await self.app.business_message(message(cid="c4"))
        self.bot.get_business_connection.assert_awaited_once_with("c4")
        self.assertIsNotNone(self.archive.get(4, 2, 10))

    async def test_archive_precedes_mute_delete_and_own_deletion_is_silent(self):
        self.archive.mute(1, 2, True)
        async def delete(cid, ids):
            row = self.archive.get(1, 2, ids[0])
            self.assertEqual(row["text"], "saved")
            self.assertTrue(row["suppressed"])
        self.bot.delete_business_messages.side_effect = delete
        await self.app.business_message(message("saved"))
        await self.app.deleted(deletion())
        self.assertTrue(self.archive.get(1, 2, 10)["deleted"])
        self.assertEqual(len(self.pending()), 0)

    async def test_delete_failure_clears_marker_but_network_ambiguity_keeps_it(self):
        self.archive.mute(1, 2, True)
        self.bot.delete_business_messages.side_effect = TelegramBadRequest(
            method=SendMessage(chat_id=1, text="test"), message="forbidden")
        await self.app.business_message(message())
        self.assertFalse(self.archive.get(1, 2, 10)["suppressed"])
        self.bot.delete_business_messages.side_effect = network_error()
        with self.assertRaises(TelegramNetworkError):
            await self.app.business_message(message(mid=11))
        self.assertTrue(self.archive.get(1, 2, 11)["suppressed"])

    async def test_lost_delete_right_keeps_archive_without_delete(self):
        self.archive.mute(1, 2, True)
        self.archive.save_connection(connection(delete=False))
        await self.app.business_message(message())
        self.assertIsNotNone(self.archive.get(1, 2, 10))
        self.bot.delete_business_messages.assert_not_awaited()

    async def test_deleted_message_notifies_once_and_survives_restart(self):
        await self.app.business_message(message("saved text"))
        await self.app.deleted(deletion())
        await self.app.deleted(deletion())
        self.assertEqual(len(self.pending()), 1)
        restarted = Archive(self.temp.name)
        try:
            new_app = Veltrix(self.bot, restarted)
            await new_app.deliver_notifications()
            self.assertIn("saved text", self.bot.send_message.await_args.args[1])
            self.assertEqual(self.bot.send_message.await_args.args[0], 1)
        finally:
            restarted.close()
        self.assertEqual(len(self.pending()), 0)

    async def test_edit_shows_previous_and_new_and_ignores_duplicate(self):
        await self.app.business_message(message("before"))
        await self.app.edited(message("after", edit_date=int(NOW.timestamp())))
        await self.app.edited(message("after", edit_date=int(NOW.timestamp())))
        self.assertEqual(len(self.pending()), 1)
        self.assertIn("Было:\nbefore", self.pending()[0]["text"])
        self.assertIn("Стало:\nafter", self.pending()[0]["text"])
        self.assertEqual(self.archive.get(1, 2, 10)["text"], "after")

    async def test_tenant_isolation_for_archive_edits_deletions_and_control(self):
        self.second_owner()
        await self.app.business_message(message("one secret"))
        await self.app.business_message(message("three secret", cid="c3"))
        await self.app.edited(message("three edited", cid="c3"))
        await self.app.deleted(deletion(cid="c3"))
        self.assertEqual(self.archive.get(1, 2, 10)["text"], "one secret")
        self.assertFalse(self.archive.get(1, 2, 10)["deleted"])
        self.assertTrue(all(row["owner_id"] == 3 for row in self.pending()))
        await self.app.private(private("/archive"))
        text = "".join(call.args[1] for call in self.bot.send_message.await_args_list)
        self.assertIn("one secret", text)
        self.assertNotIn("three secret", text)
        self.assertNotIn("three edited", text)
        await self.app.business_message(message("other", chat=8, cid="c3"))
        await self.app.private(private("/mute 8"))
        self.assertFalse(self.archive.is_muted(1, 8))

    async def test_photo_saved_before_delete_and_sent_from_local_file(self):
        async def download(path, destination):
            Path(destination).write_bytes(b"synthetic test photo")
        self.bot.download_file.side_effect = download
        await self.app.business_message(message("caption", photo=True))
        row = self.archive.get(1, 2, 10)
        self.assertTrue(Path(row["photo_path"]).is_file())
        await self.app.deleted(deletion())
        await self.app.deliver_notifications()
        self.bot.send_photo.assert_awaited_once()
        self.assertEqual(self.bot.send_photo.await_args.args[0], 1)

    async def test_deletion_waits_for_photo_archive(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def download(path, destination):
            started.set()
            await release.wait()
            Path(destination).write_bytes(b"synthetic test photo")
        self.bot.download_file.side_effect = download
        saving = asyncio.create_task(self.app.business_message(message(photo=True)))
        await asyncio.wait_for(started.wait(), 1)
        deleting = asyncio.create_task(self.app.deleted(deletion()))
        await asyncio.sleep(0)
        self.assertFalse(deleting.done())
        release.set()
        await asyncio.wait_for(asyncio.gather(saving, deleting), 1)
        self.assertTrue(Path(self.pending()[0]["photo_path"]).is_file())

    async def test_photo_failure_preserves_text_and_does_not_delete(self):
        self.archive.mute(1, 2, True)
        self.bot.download_file.side_effect = OSError("offline")
        await self.app.business_message(message("caption", photo=True))
        self.assertEqual(self.archive.get(1, 2, 10)["text"], "caption")
        self.bot.delete_business_messages.assert_not_awaited()

    async def test_protected_photo_is_not_downloaded(self):
        await self.app.business_message(message(photo=True, has_protected_content=True))
        self.bot.get_file.assert_not_awaited()
        self.assertIsNone(self.archive.get(1, 2, 10)["photo_path"])

    async def test_notification_retries_photo_without_repeating_text(self):
        path = self.archive.media / "1_photo.jpg"
        path.write_bytes(b"synthetic")
        self.archive.enqueue(1, "keep this", str(path))
        self.bot.send_photo.side_effect = [network_error(), None]
        await self.app.deliver_notifications()
        self.assertEqual(len(self.pending()), 1)
        self.archive.db.execute("UPDATE notifications SET retry_at=0")
        self.archive.db.commit()
        await self.app.deliver_notifications()
        self.assertEqual(len(self.pending()), 0)
        self.assertEqual(self.bot.send_message.await_count, 1)
        self.assertEqual(self.bot.send_photo.await_count, 2)

    async def test_blocking_bot_pauses_user_and_stops_jobs(self):
        self.app.delay = 60
        self.app.launch_spam(1, 2, 100, "text", "c1")
        self.archive.enqueue(1, "notice")
        self.bot.send_message.side_effect = TelegramForbiddenError(
            method=SendMessage(chat_id=1, text="test"), message="blocked")
        await self.app.deliver_notifications()
        self.assertFalse(self.archive.user(1)["active"])
        self.assertFalse(self.app.spam_tasks)

    async def test_erase_deletes_only_own_data_and_does_not_auto_resume(self):
        self.second_owner()
        await self.app.business_message(message("one"))
        await self.app.business_message(message("three", cid="c3"))
        one = self.archive.media / "1_photo.jpg"
        three = self.archive.media / "3_photo.jpg"
        one.write_bytes(b"one")
        three.write_bytes(b"three")
        await self.app.private(private("/erase confirm"))
        self.assertIsNone(self.archive.get(1, 2, 10))
        self.assertIsNotNone(self.archive.get(3, 2, 10))
        self.assertFalse(one.exists())
        self.assertTrue(three.exists())
        self.assertFalse(self.archive.user(1)["active"])
        await self.app.business_message(message(mid=11))
        self.assertIsNone(self.archive.get(1, 2, 11))

    async def test_archive_pagination_and_invalid_page(self):
        for mid in range(12):
            await self.app.business_message(message(f"text{mid}", mid=mid))
        await self.app.private(private("/archive 2"))
        text = "".join(call.args[1] for call in self.bot.send_message.await_args_list)
        self.assertIn("страница 2/3", text)
        self.assertIn("/archive 3", text)
        await self.app.private(private("/archive -1"))
        self.assertIn("номер страницы", self.bot.send_message.await_args.args[1])

    async def test_floodwait_sets_shared_cooldown_and_retries(self):
        operation = AsyncMock(side_effect=[TelegramRetryAfter(
            method=SendMessage(chat_id=1, text="test"), message="flood", retry_after=2), "ok"])
        real_sleep = asyncio.sleep
        async def fast_sleep(seconds):
            self.assertGreater(self.app.flood_until, time.monotonic() + 2)
            self.app.flood_until = 0
            await real_sleep(0)
        with patch("main.asyncio.sleep", side_effect=fast_sleep):
            self.assertEqual(await self.app.telegram(operation), "ok")
        self.assertEqual(operation.await_count, 2)

    async def test_network_send_is_not_blindly_repeated(self):
        self.bot.send_message.side_effect = network_error()
        self.app.launch_spam(1, 2, 20, "text", "c1")
        await self.finish_spam()
        self.bot.send_message.assert_awaited_once()
        self.assertIn("без подтверждения", self.pending()[0]["text"])

    async def test_long_text_uses_plain_chunks_within_utf16_limit(self):
        await self.app.send_text(1, "😀" * 4500)
        chunks = [call.args[1] for call in self.bot.send_message.await_args_list]
        self.assertEqual("".join(chunks), "😀" * 4500)
        self.assertTrue(all(len(s.encode("utf-16-le")) // 2 <= 4096 for s in chunks))

    async def test_private_group_and_spoofed_owner_are_ignored(self):
        fake = private("/start", 4).model_copy(update={"from_user": user(5)})
        await self.app.private(fake)
        await self.app.private(private("/start", 4).model_copy(
            update={"chat": Chat(id=-100, type="supergroup", title="Group")}))
        self.assertIsNone(self.archive.user(4))

    async def test_shutdown_cancels_inflight_handlers_before_database_closes(self):
        started = asyncio.Event()
        async def download(path, destination):
            started.set()
            await asyncio.Event().wait()
        self.bot.download_file.side_effect = download
        task = asyncio.create_task(self.app.guarded(self.app.business_message)(message(photo=True)))
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.wait_for(self.app.close(), 1)
        self.assertTrue(task.cancelled())
        self.assertFalse(self.app.handler_tasks)
        self.assertTrue(self.app.closing)

    async def test_notification_stops_between_chunks_when_user_erases_archive(self):
        self.archive.enqueue(1, "x" * 2500)
        async def send(*args, **kwargs):
            self.archive.erase(1)
        self.bot.send_message.side_effect = send
        await self.app.deliver_notifications()
        self.bot.send_message.assert_awaited_once()
        self.assertFalse(self.pending())
        self.assertFalse(self.archive.user(1)["active"])


class ConfigurationTests(unittest.TestCase):
    def test_bot_token_only_and_delay_validation(self):
        token = "123456:" + "x" * 35
        with patch("main.load_dotenv"), patch.dict(os.environ, {"BOT_TOKEN": token}, clear=True):
            self.assertEqual(configuration(), (token, 1.5))
            for value in ("0", "nan", "inf", "wrong"):
                os.environ["SPAM_DELAY"] = value
                with self.assertRaises(ValueError):
                    configuration()
        with patch("main.load_dotenv"), patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "BOT_TOKEN"):
                configuration()


class FakeSession(BaseSession):
    """Exercises aiogram's real dispatcher and method models without networking."""

    def __init__(self):
        super().__init__()
        self.calls = []

    async def close(self):
        pass

    async def make_request(self, bot, method, timeout=None):
        self.calls.append(method)
        if method.__api_method__ in {"sendMessage", "editMessageText"}:
            return message(method.text, sender=1, chat=int(method.chat_id), mid=90)
        return True

    async def stream_content(self, *args, **kwargs):
        yield b"synthetic"


class DispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_business_and_private_updates_route_through_real_aiogram(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Archive(directory)
            session = FakeSession()
            bot = Bot("123456:" + "x" * 35, session=session)
            app = Veltrix(bot, archive, delay=0)
            dispatcher = Dispatcher()
            app.register(dispatcher)
            try:
                updates = [
                    Update(update_id=1, message=private("/start")),
                    Update(update_id=2, business_connection=connection()),
                    Update(update_id=3, business_message=message(".mute", sender=1, mid=100)),
                    Update(update_id=4, business_message=message("saved")),
                    Update(update_id=5, deleted_business_messages=deletion()),
                    Update(update_id=6, message=private("/archive")),
                ]
                for update in updates:
                    await dispatcher.feed_update(bot, update)
                self.assertTrue(archive.is_muted(1, 2))
                self.assertEqual(archive.get(1, 2, 10)["text"], "saved")
                self.assertTrue(archive.get(1, 2, 10)["deleted"])
                names = [call.__api_method__ for call in session.calls]
                self.assertIn("editMessageText", names)
                self.assertIn("deleteBusinessMessages", names)
                self.assertIn("sendMessage", names)
            finally:
                await app.close()
                await bot.session.close()
                archive.close()


if __name__ == "__main__":
    unittest.main()
