import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from telethon import errors, types

from main import Archive, Veltrix


def message(message_id=10, text="original", chat_id=2, sender_id=2):
    result = SimpleNamespace(
        id=message_id, raw_text=text, chat_id=chat_id, sender_id=sender_id,
        date=datetime.now(timezone.utc), photo=None, media=None,
        get_sender=AsyncMock(return_value=types.User(id=sender_id, first_name="Alice")),
    )
    return result


def event(text="", chat_id=2, sender_id=1, message_id=100, private=True):
    msg = message(message_id, text, chat_id, sender_id)
    return SimpleNamespace(
        id=message_id, raw_text=text, chat_id=chat_id, sender_id=sender_id,
        out=sender_id == 1, is_private=private, message=msg,
        edit=AsyncMock(), get_input_chat=AsyncMock(return_value=chat_id),
    )


class VeltrixTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.archive = Archive(self.path / "archive.sqlite3")
        self.next_id = 200

        async def send(*args, **kwargs):
            self.next_id += 1
            return SimpleNamespace(id=self.next_id)

        self.client = SimpleNamespace(
            send_message=AsyncMock(side_effect=send),
            delete_messages=AsyncMock(), send_file=AsyncMock(),
            download_media=AsyncMock(),
        )
        self.bot = Veltrix(self.client, self.archive, owner_id=1,
                           delay=0, media_dir=self.path / "media")

    async def asyncTearDown(self):
        await self.bot.stop()
        self.archive.close()
        self.temp.cleanup()

    async def finish_spam(self, chat_id=2):
        task = self.bot.spam_tasks.get(chat_id)
        if task:
            await asyncio.wait_for(task, 1)

    async def test_finite_spam_has_no_ten_message_cap(self):
        command = event(".spam 12 привет")
        await self.bot.command(command)
        await self.finish_spam()
        command.edit.assert_awaited_once_with(
            "✅ Запущена отправка 12 сообщений", parse_mode=None
        )
        self.assertEqual(self.client.send_message.await_count, 12)
        self.assertTrue(all(call.args == (2, "привет")
                            for call in self.client.send_message.await_args_list))

    async def test_commands_require_owner(self):
        command = event(".mute", sender_id=2)
        await self.bot.command(command)
        command.out = True  # Also reject spoofed outgoing messages from another ID.
        await self.bot.command(command)
        self.assertFalse(self.archive.is_muted(2))
        command.edit.assert_not_awaited()

    async def test_generated_commands_do_not_recurse(self):
        await self.bot.command(event(".spam 1 .mute"))
        await self.finish_spam()
        command = event(".mute", message_id=self.next_id)
        await self.bot.command(command)
        self.assertFalse(self.archive.is_muted(2))

    async def test_infinite_only_in_saved_messages_and_stop(self):
        await self.bot.command(event(".spam forever hello"))
        self.assertFalse(self.bot.spam_tasks)
        self.bot.delay = 30
        await self.bot.command(event(".spam forever hello", chat_id=1))
        await asyncio.sleep(0)
        await self.bot.command(event(".stopspam", chat_id=1))
        count = self.client.send_message.await_count
        await asyncio.sleep(0)
        self.assertEqual(self.client.send_message.await_count, count)
        self.assertFalse(self.bot.spam_tasks)

    async def test_stop_interrupts_flood_cooldown(self):
        self.bot.flood_until = time.monotonic() + 3600
        # Let command confirmations proceed while the sender uses the real cooldown.
        self.bot.edit = AsyncMock()
        await self.bot.command(event(".spam 100 test"))
        await asyncio.sleep(0)
        await asyncio.wait_for(self.bot.command(event(".stopspam")), 0.5)
        self.client.send_message.assert_not_awaited()
        self.assertFalse(self.bot.spam_tasks)

    async def test_flood_wait_retries_after_cooldown(self):
        operation = AsyncMock(side_effect=[errors.FloodWaitError(None, capture=2), "ok"])
        # End the cooldown as soon as the helper has caught the error.
        async def release():
            while not self.bot.flood_until:
                await asyncio.sleep(0)
            self.assertGreater(self.bot.flood_until, time.monotonic() + 2)
            self.bot.flood_until = 0
        release_task = asyncio.create_task(release())
        # Mock only sleep, retaining a real yield to the release task.
        from unittest.mock import patch
        real_sleep = asyncio.sleep
        async def short_sleep(_):
            await real_sleep(0)
        with patch("main.asyncio.sleep", side_effect=short_sleep):
            self.assertEqual(await self.bot.telegram(operation), "ok")
        await release_task
        self.assertEqual(operation.await_count, 2)

    async def test_invalid_count_and_duplicate_start(self):
        for count in ("0", "-1", "1.5", "invalid"):
            await self.bot.command(event(f".spam {count} text"))
        self.assertFalse(self.bot.spam_tasks)
        self.bot.delay = 30
        await self.bot.command(event(".spam 100 text"))
        first = self.bot.spam_tasks[2]
        await self.bot.command(event(".spam 2 other"))
        self.assertIs(self.bot.spam_tasks[2], first)

    async def test_mute_persists_and_unmute_edits(self):
        await self.bot.command(event(".mute"))
        second = Archive(self.path / "archive.sqlite3")
        try:
            self.assertTrue(second.is_muted(2))
        finally:
            second.close()
        command = event(".unmute")
        await self.bot.command(command)
        self.assertFalse(self.archive.is_muted(2))
        command.edit.assert_awaited_once_with("🔊 Мут отключён", parse_mode=None)

    async def test_mute_rejects_groups_and_saved_messages(self):
        await self.bot.command(event(".mute", private=False))
        await self.bot.command(event(".mute", chat_id=1))
        self.assertFalse(self.archive.is_muted(2))
        self.assertFalse(self.archive.is_muted(1))

    async def test_archive_before_self_delete_and_suppress_notification(self):
        self.archive.mute(2, True)
        async def delete(chat, ids, revoke):
            row = self.archive.get(ids[0])
            self.assertEqual(row["text"], "secret")
            self.assertTrue(row["suppressed"])
            self.assertTrue(revoke)
        self.client.delete_messages.side_effect = delete
        incoming = event("secret", sender_id=2, message_id=10)
        await self.bot.incoming(incoming)
        await self.bot.deleted(SimpleNamespace(chat_id=None, deleted_ids=[10]))
        self.client.send_message.assert_not_awaited()

    async def test_delete_failure_clears_suppression(self):
        self.archive.mute(2, True)
        self.client.delete_messages.side_effect = errors.RPCError(None, "FAILED", 400)
        with self.assertRaises(errors.RPCError):
            await self.bot.incoming(event("text", sender_id=2, message_id=10))
        self.assertFalse(self.archive.get(10)["suppressed"])

    async def test_delete_fallback_to_local(self):
        self.archive.mute(2, True)
        self.client.delete_messages.side_effect = [
            errors.MessageDeleteForbiddenError(None), None
        ]
        await self.bot.incoming(event("text", sender_id=2, message_id=10))
        self.assertFalse(self.client.delete_messages.await_args.kwargs["revoke"])
        self.assertTrue(self.archive.get(10)["suppressed"])

    async def test_ambiguous_delete_cancellation_keeps_marker(self):
        self.archive.mute(2, True)
        self.client.delete_messages.side_effect = asyncio.CancelledError()
        with self.assertRaises(asyncio.CancelledError):
            await self.bot.incoming(event("text", sender_id=2, message_id=10))
        self.assertTrue(self.archive.get(10)["suppressed"])
        await self.bot.deleted(SimpleNamespace(chat_id=None, deleted_ids=[10]))
        self.client.send_message.assert_not_awaited()

    async def test_delete_without_chat_id_notifies_once(self):
        await self.bot.incoming(event("kept", sender_id=2, message_id=10))
        deleted = SimpleNamespace(chat_id=None, deleted_ids=[10, 999])
        await self.bot.deleted(SimpleNamespace(chat_id=-100123, deleted_ids=[10]))
        self.client.send_message.assert_not_awaited()
        await self.bot.deleted(deleted)
        await self.bot.deleted(deleted)
        self.assertEqual(self.client.send_message.await_count, 1)
        text = self.client.send_message.await_args.args[1]
        self.assertIn("Alice", text)
        self.assertIn("kept", text)

    async def test_edit_reports_old_and_new_without_own_notifications(self):
        await self.bot.incoming(event("before", sender_id=2, message_id=10))
        await self.bot.edited(event("after", sender_id=2, message_id=10))
        text = self.client.send_message.await_args.args[1]
        self.assertIn("Было:\nbefore", text)
        self.assertIn("Стало:\nafter", text)
        self.assertEqual(self.archive.get(10)["text"], "after")
        await self.bot.edited(event("ignored", sender_id=1, message_id=100))
        self.assertEqual(self.client.send_message.await_count, 1)

    async def test_photo_saved_and_restored_on_delete(self):
        incoming = event("caption", sender_id=2, message_id=10)
        incoming.message.photo = SimpleNamespace(id=50)
        incoming.message.media = SimpleNamespace(ttl_seconds=None)
        async def download(msg, file):
            Path(file).write_bytes(b"test photo")
            return file
        self.client.download_media.side_effect = download
        await self.bot.incoming(incoming)
        row = self.archive.get(10)
        self.assertTrue(Path(row["photo_path"]).is_file())
        await self.bot.deleted(SimpleNamespace(chat_id=None, deleted_ids=[10]))
        self.client.send_file.assert_awaited_once()

    async def test_photo_failure_preserves_text_and_does_not_delete(self):
        self.archive.mute(2, True)
        incoming = event("caption", sender_id=2, message_id=10)
        incoming.message.photo = SimpleNamespace(id=50)
        incoming.message.media = SimpleNamespace(ttl_seconds=None)
        self.client.download_media.side_effect = OSError("offline")
        with self.assertRaises(OSError):
            await self.bot.incoming(incoming)
        self.assertEqual(self.archive.get(10)["text"], "caption")
        self.client.delete_messages.assert_not_awaited()

    async def test_ttl_photo_is_never_downloaded(self):
        incoming = event("", sender_id=2, message_id=10)
        incoming.message.photo = SimpleNamespace(id=50)
        incoming.message.media = SimpleNamespace(ttl_seconds=1)
        await self.bot.incoming(incoming)
        self.client.download_media.assert_not_awaited()
        self.assertIsNone(self.archive.get(10)["photo_path"])

    async def test_long_notification_split_and_plain_text(self):
        await self.bot.notify("😀" * 4500)
        chunks = [call.args[1] for call in self.client.send_message.await_args_list]
        self.assertEqual("".join(chunks), "😀" * 4500)
        self.assertTrue(all(len(chunk.encode("utf-16-le")) // 2 <= 4096
                            for chunk in chunks))


if __name__ == "__main__":
    unittest.main()
