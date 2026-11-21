import asyncio
import html
import json
import re
import sqlite3
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import test_main as helpers
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import DeleteBusinessMessages

import interface as ui
from main import Archive, Veltrix


class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await helpers.VeltrixTests.asyncSetUp(self)
        self.bot.send_document = AsyncMock()

    async def asyncTearDown(self):
        await helpers.VeltrixTests.asyncTearDown(self)

    pending = helpers.VeltrixTests.pending
    second_owner = helpers.VeltrixTests.second_owner
    finish_jobs = helpers.VeltrixTests.finish_jobs
    finish_spam = helpers.VeltrixTests.finish_spam

    def query(self, data, owner=1, chat=None):
        return SimpleNamespace(data=data, from_user=helpers.user(owner),
                               message=helpers.private("panel", owner if chat is None else chat),
                               answer=AsyncMock())

    async def test_fast_mute_is_not_blocked_by_send_cooldown_or_photo_download(self):
        downloading, release, removed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.archive.mute(1, 2, True)
        self.app.flood_until = time.monotonic() + 3600
        async def download(path, destination):
            downloading.set()
            await release.wait()
            Path(destination).write_bytes(b"synthetic photo")
        async def delete(cid, ids):
            row = self.archive.get(1, 2, ids[0])
            self.assertEqual(row["text"], "before deletion")
            self.assertEqual(row["photo_id"], "photo-id")
            self.assertTrue(row["suppressed"])
            self.assertIsNone(row["photo_path"])
            removed.set()
        self.bot.download_file.side_effect = download
        self.bot.delete_business_messages.side_effect = delete
        await self.app.business_message(helpers.message("before deletion", photo=True))
        await asyncio.wait_for(asyncio.gather(downloading.wait(), removed.wait()), 2)
        self.assertFalse(release.is_set())
        self.assertTrue(self.archive.get(1, 2, 10)["deleted"])
        release.set()
        await self.finish_jobs()
        self.assertEqual(len(self.pending()), 0)

    async def test_photo_in_other_chat_does_not_delay_muted_text(self):
        started, release, removed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.archive.mute(1, 8, True)
        async def download(path, destination):
            started.set()
            await release.wait()
            Path(destination).write_bytes(b"synthetic")
        async def delete(cid, ids):
            self.assertEqual(ids, [11])
            removed.set()
        self.bot.download_file.side_effect = download
        self.bot.delete_business_messages.side_effect = delete
        await self.app.business_message(helpers.message(photo=True))
        await asyncio.wait_for(started.wait(), 2)
        await self.app.business_message(helpers.message("muted", chat=8, mid=11))
        await asyncio.wait_for(removed.wait(), 2)
        release.set()
        await self.finish_jobs()

    async def test_safe_mute_downloads_photo_before_delete(self):
        self.archive.preference(1, mode="safe")
        self.archive.mute(1, 2, True)
        started, release = asyncio.Event(), asyncio.Event()
        async def download(path, destination):
            started.set()
            await release.wait()
            Path(destination).write_bytes(b"synthetic")
        async def delete(cid, ids):
            self.assertTrue(Path(self.archive.get(1, 2, ids[0])["photo_path"]).is_file())
        self.bot.download_file.side_effect = download
        self.bot.delete_business_messages.side_effect = delete
        await self.app.business_message(helpers.message(photo=True))
        await asyncio.wait_for(started.wait(), 2)
        self.bot.delete_business_messages.assert_not_awaited()
        release.set()
        await self.finish_jobs()
        self.bot.delete_business_messages.assert_awaited_once()

    async def test_safe_mute_retains_original_after_three_photo_failures(self):
        self.archive.preference(1, mode="safe")
        self.archive.mute(1, 2, True)
        self.bot.download_file.side_effect = OSError("offline")
        await self.app.business_message(helpers.message("caption", photo=True))
        await self.finish_jobs()
        for _ in range(2):
            with self.archive.db:
                self.archive.db.execute("UPDATE messages SET photo_retry_at=0")
            await self.app.maintain_once()
            await self.finish_jobs()
        row = self.archive.get(1, 2, 10)
        self.assertEqual(row["photo_state"], "failed")
        self.assertFalse(row["delete_pending"])
        self.assertFalse(row["suppressed"])
        self.bot.delete_business_messages.assert_not_awaited()
        self.assertEqual(row["text"], "caption")

    async def test_user_deletion_before_our_rpc_is_not_mislabeled_as_own(self):
        self.archive.mute(1, 2, True)
        await self.app.business_message(helpers.message("kept"))
        # No event-loop yield: the deletion update precedes our scheduled API request.
        await self.app.deleted(helpers.deletion())
        await self.finish_jobs()
        self.bot.delete_business_messages.assert_not_awaited()
        self.assertEqual(len(self.pending()), 1)
        self.assertFalse(self.archive.get(1, 2, 10)["suppressed"])

    async def test_unmute_cancels_a_deletion_waiting_for_retry_after(self):
        self.archive.mute(1, 2, True)
        self.app.cooldowns["delete"] = time.monotonic() + 3600
        await self.app.business_message(helpers.message())
        await asyncio.sleep(0)
        await self.app.set_mute(1, 2, False)
        self.bot.delete_business_messages.assert_not_awaited()
        row = self.archive.get(1, 2, 10)
        self.assertFalse(row["delete_pending"])
        self.assertFalse(row["suppressed"])

    async def test_revoked_connection_does_not_turn_pending_mutes_into_error_spam(self):
        self.archive.mute(1, 2, True)
        self.app.cooldowns["delete"] = time.monotonic() + 3600
        await self.app.business_message(helpers.message())
        await asyncio.sleep(0)
        await self.app.connected(helpers.connection(enabled=False))
        await self.app.maintain_once()
        await self.finish_jobs()
        self.bot.delete_business_messages.assert_not_awaited()
        self.assertFalse(self.archive.get(1, 2, 10)["delete_pending"])
        self.assertEqual(len(self.pending()), 1)
        self.assertIn("отключено", self.pending()[0]["text"])

    async def test_cancelled_old_job_cannot_remove_replacement_tracking(self):
        self.archive.mute(1, 2, True)
        started = asyncio.Event()
        async def delete(cid, ids):
            started.set()
            await asyncio.Event().wait()
        self.bot.delete_business_messages.side_effect = delete
        await self.app.business_message(helpers.message())
        await asyncio.wait_for(started.wait(), 2)
        key = (1, 2, 10)
        old = self.app.mute_tasks[key]
        replacement = asyncio.create_task(asyncio.Event().wait())
        self.app.mute_tasks[key] = replacement
        self.app.delete_started[key] = replacement
        old.cancel()
        await asyncio.gather(old, return_exceptions=True)
        self.assertIs(self.app.mute_tasks[key], replacement)
        self.assertIs(self.app.delete_started[key], replacement)

    async def test_ambiguous_own_deletion_marker_survives_a_restart_retry_error(self):
        self.archive.mute(1, 2, True)
        self.archive.save_message(1, helpers.message("archived"))
        self.archive.suppress(1, 2, 10, True, pending=True)
        self.bot.delete_business_messages.side_effect = TelegramBadRequest(
            method=DeleteBusinessMessages(business_connection_id="c1", message_ids=[10]),
            message="message to delete not found")
        await self.app.maintain_once()
        await self.finish_jobs()
        self.assertTrue(self.archive.get(1, 2, 10)["suppressed"])
        await self.app.deleted(helpers.deletion())
        self.assertFalse(any(json.loads(row["payload"]).get("kind") == "deleted" for row in self.pending()))

    async def test_full_received_conversation_survives_batch_deletion(self):
        await self.app.business_message(helpers.message("incoming", mid=10))
        await self.app.business_message(helpers.message("my reply", sender=1, mid=11))
        await self.app.business_message(helpers.message("second incoming", mid=12))
        await self.app.deleted(helpers.deletion(ids=[10, 11, 12]))
        key = self.archive.known_chat(1, 2)["key"]
        chat, rows, revisions = self.archive.snapshot(1, key)
        self.assertEqual([row["text"] for row in rows], ["incoming", "my reply", "second incoming"])
        self.assertEqual([row["outgoing"] for row in rows], [0, 1, 0])
        self.assertTrue(all(row["deleted"] for row in rows))
        self.assertEqual(len(self.pending()), 1)
        payload = json.loads(self.pending()[0]["payload"])
        rendered = ui.notice(payload)
        self.assertIn("Удалено сообщений: 3", rendered)
        self.assertNotIn("удалил", rendered)
        await self.app.show_conversation(1, chat["key"], 0)
        shown = self.bot.send_message.await_args.args[1]
        self.assertIn("my reply", shown)
        self.assertIn("Вы", shown)

    async def test_outgoing_business_bot_message_is_archived_but_not_executed(self):
        actor = helpers.User(id=99, is_bot=True, first_name="Veltrix")
        await self.app.business_message(helpers.message(".mute", sender=1, sender_business_bot=actor))
        row = self.archive.get(1, 2, 10)
        self.assertTrue(row["outgoing"])
        self.assertFalse(self.archive.is_muted(1, 2))
        self.bot.edit_message_text.assert_not_awaited()

    async def test_outgoing_edit_history_is_kept_without_noisy_self_notice(self):
        await self.app.business_message(helpers.message("my original", sender=1))
        await self.app.edited(helpers.message("my edit", sender=1, edit_date=int(time.time())))
        rows = self.archive.db.execute("SELECT * FROM revisions WHERE owner_id=1").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["old_text"], "my original")
        self.assertEqual(len(self.pending()), 0)

    async def test_deleted_unknown_messages_do_not_fabricate_history(self):
        await self.app.deleted(helpers.deletion(ids=[999, 1000]))
        self.assertFalse(self.pending())
        self.assertIsNone(self.archive.get(1, 2, 999))

    async def test_settings_speed_is_per_user_and_accepts_zero(self):
        self.second_owner()
        await self.app.callback(self.query("speed:0"))
        self.assertEqual(self.app.delay_for(1), 0)
        self.assertIsNone(self.archive.user(3)["spam_delay"])
        await self.app.callback(self.query("mode:safe"))
        self.assertEqual(self.archive.user(1)["mute_mode"], "safe")
        self.assertEqual(self.archive.user(3)["mute_mode"], "fast")
        await self.app.business_message(helpers.message(".speed 0.03", sender=1))
        self.assertEqual(self.app.delay_for(1), 0.03)

    async def test_zero_delay_still_yields_and_can_be_stopped(self):
        self.archive.preference(1, delay=0)
        sent = asyncio.Event()
        count = 0
        async def send(*args, **kwargs):
            nonlocal count
            count += 1
            if count >= 2:
                sent.set()
        self.bot.send_message.side_effect = send
        self.app.launch_spam(1, 1, None, "test")
        await asyncio.wait_for(sent.wait(), 2)
        await asyncio.wait_for(self.app.stop_spam((1, "test", 1)), 2)
        recorded = count
        await asyncio.sleep(0)
        self.assertEqual(recorded, count)
        self.assertFalse(self.app.spam_tasks)

    async def test_wrong_owner_cannot_open_export_photo_or_mute_chat_key(self):
        self.second_owner()
        await self.app.business_message(helpers.message("other user's secret", cid="c3"))
        foreign = self.archive.known_chat(3, 2)["key"]
        for data in (f"chat:{foreign}:-1", f"export:{foreign}", f"photo:{foreign}:10", f"mute:{foreign}"):
            query = self.query(data)
            await self.app.callback(query)
            self.assertIn("недоступна", query.answer.await_args.kwargs["text"])
        self.bot.send_document.assert_not_awaited()
        self.bot.send_photo.assert_not_awaited()
        self.assertFalse(self.archive.is_muted(1, 2))

    async def test_callback_cannot_be_replayed_in_someone_elses_private_chat(self):
        self.second_owner()
        await self.app.callback(self.query("speed:0", owner=1, chat=3))
        self.assertIsNone(self.archive.user(1)["spam_delay"])
        self.assertIsNone(self.archive.user(3)["spam_delay"])

    async def test_legitimate_chat_callback_has_no_visible_technical_ids(self):
        await self.app.business_message(helpers.message("normal text"))
        key = self.archive.known_chat(1, 2)["key"]
        await self.app.callback(self.query(f"chat:{key}:-1"))
        shown = self.bot.edit_message_text.await_args.kwargs["text"]
        self.assertIn("normal text", shown)
        self.assertNotIn("ID", shown)
        self.assertNotIn("сообщение: 10", shown)
        buttons = self.bot.edit_message_text.await_args.kwargs["reply_markup"].inline_keyboard
        self.assertTrue(all(len(button.callback_data.encode()) <= 64 for row in buttons for button in row))

    async def test_notification_includes_sender_link_and_owned_chat_button(self):
        await self.app.business_message(helpers.message("saved <message>"))
        await self.app.deleted(helpers.deletion())
        await self.app.deliver_notifications()
        call = self.bot.send_message.await_args
        rendered = call.args[1]
        self.assertIn('tg://user?id=2', rendered)
        self.assertIn("User 2", rendered)
        self.assertIn("saved &lt;message&gt;", rendered)
        self.assertNotIn("(ID", rendered)
        self.assertNotIn("Чат:", rendered)
        key = self.archive.known_chat(1, 2)["key"]
        self.assertEqual(call.kwargs["reply_markup"].inline_keyboard[0][0].callback_data, f"chat:{key}:-1")

    async def test_notification_text_is_immediate_and_photo_follows_without_duplicate(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def download(path, destination):
            started.set()
            await release.wait()
            Path(destination).write_bytes(b"synthetic")
        self.bot.download_file.side_effect = download
        await self.app.business_message(helpers.message("caption", photo=True))
        await asyncio.wait_for(started.wait(), 2)
        await self.app.deleted(helpers.deletion())
        await self.app.deliver_notifications()
        self.bot.send_message.assert_awaited_once()
        self.bot.send_photo.assert_not_awaited()
        release.set()
        await self.finish_jobs()
        with self.archive.db:
            self.archive.db.execute("UPDATE notifications SET retry_at=0")
        await self.app.deliver_notifications()
        self.bot.send_message.assert_awaited_once()
        self.bot.send_photo.assert_awaited_once()

    async def test_pending_photos_do_not_starve_later_text_notifications(self):
        # Durable jobs can exist after restart, before the media worker starts.
        for mid in range(30):
            self.archive.save_message(1, helpers.message(mid=mid, photo=True), "Фото", "file")
            self.archive.delete_batch(1, 2, [mid])
        self.archive.enqueue(1, "later text")
        await self.app.deliver_notifications()
        await self.app.deliver_notifications()
        self.assertTrue(any("later text" in call.args[1] for call in self.bot.send_message.await_args_list))

    async def test_pending_photo_resumes_after_restart(self):
        started = asyncio.Event()
        async def blocked(path, destination):
            started.set()
            await asyncio.Event().wait()
        self.bot.download_file.side_effect = blocked
        await self.app.business_message(helpers.message(photo=True))
        await asyncio.wait_for(started.wait(), 2)
        await self.app.close()
        reopened = Archive(self.temp.name)
        app = Veltrix(self.bot, reopened)
        async def download(path, destination):
            Path(destination).write_bytes(b"recovered synthetic photo")
        self.bot.download_file.side_effect = download
        try:
            await app.maintain_once()
            await asyncio.wait_for(asyncio.gather(*app.media_tasks.values()), 2)
            row = reopened.get(1, 2, 10)
            self.assertEqual(row["photo_state"], "ready")
            self.assertTrue(Path(row["photo_path"]).is_file())
        finally:
            await app.close()
            reopened.close()

    async def test_offline_export_contains_both_sides_edits_and_local_photos(self):
        async def download(path, destination):
            Path(destination).write_bytes(b"synthetic photo")
        self.bot.download_file.side_effect = download
        await self.app.business_message(helpers.message('hello <script>alert("x")</script>'))
        await self.app.business_message(helpers.message("my reply", sender=1, mid=11))
        await self.app.edited(helpers.message("edited", edit_date=int(time.time())))
        await self.app.business_message(helpers.message("caption", photo=True, mid=12))
        await self.finish_jobs()
        await self.app.deleted(helpers.deletion(ids=[10, 11, 12]))
        key = self.archive.known_chat(1, 2)["key"]
        chat, rows, versions = self.archive.snapshot(1, key)
        path, photos = ui.build_export(self.archive.exports, 1, chat, rows, versions, self.archive.media)
        try:
            self.assertTrue(photos)
            with zipfile.ZipFile(path) as bundle:
                page = bundle.read("Переписка.html").decode()
                self.assertIn("my reply", page)
                self.assertIn("История изменений", page)
                self.assertIn("Сообщение удалено", page)
                self.assertIn("&lt;script&gt;", page)
                self.assertNotIn('<script>alert("x")</script>', page)
                self.assertEqual(bundle.read("media/12.jpg"), b"synthetic photo")
                self.assertTrue(all(not name.startswith(("/", "..")) for name in bundle.namelist()))
        finally:
            path.unlink()

    async def test_export_delivery_cleans_up_temporary_artifact(self):
        await self.app.business_message(helpers.message("export me"))
        key = self.archive.known_chat(1, 2)["key"]
        sent = []
        async def send(chat, document, **kwargs):
            self.assertEqual(chat, 1)
            self.assertTrue(Path(document.path).is_file())
            sent.append(Path(document.path))
        self.bot.send_document.side_effect = send
        await self.app.export_chat(1, key)
        self.assertEqual(len(sent), 1)
        self.assertFalse(sent[0].exists())

    async def test_large_media_export_falls_back_to_complete_html(self):
        await self.app.business_message(helpers.message("entire text"))
        key = self.archive.known_chat(1, 2)["key"]
        chat, rows, versions = self.archive.snapshot(1, key)
        picture = self.archive.media / "1_large.jpg"
        picture.write_bytes(b"x" * 100_000)
        rows[0]["photo_path"] = str(picture)
        path, photos = ui.build_export(self.archive.exports, 1, chat, rows, versions, self.archive.media, max_size=50_000)
        try:
            self.assertFalse(photos)
            self.assertEqual(path.suffix, ".html")
            self.assertIn("entire text", path.read_text(encoding="utf-8"))
            self.assertNotIn('<img ', path.read_text(encoding="utf-8"))
        finally:
            path.unlink()


class MigrationAndRenderingTests(unittest.TestCase):
    def test_old_database_is_migrated_without_losing_archive_or_mute(self):
        with tempfile.TemporaryDirectory() as directory:
            db = sqlite3.connect(Path(directory) / "veltrix.sqlite3")
            db.executescript("""
                CREATE TABLE users(owner_id INTEGER PRIMARY KEY,chat_id INTEGER NOT NULL,active INTEGER NOT NULL DEFAULT 1);
                INSERT INTO users VALUES(1,1,1);
                CREATE TABLE chats(owner_id INTEGER,chat_id INTEGER,title TEXT,last_seen INTEGER,PRIMARY KEY(owner_id,chat_id));
                INSERT INTO chats VALUES(1,2,'Old chat',123);
                CREATE TABLE muted(owner_id INTEGER,chat_id INTEGER,PRIMARY KEY(owner_id,chat_id));
                INSERT INTO muted VALUES(1,2);
                CREATE TABLE messages(owner_id INTEGER,chat_id INTEGER,message_id INTEGER,sender_id INTEGER,
                    sender_name TEXT,text TEXT,date INTEGER,media_note TEXT,photo_id TEXT,photo_path TEXT,
                    deleted INTEGER DEFAULT 0,suppressed INTEGER DEFAULT 0,PRIMARY KEY(owner_id,chat_id,message_id));
                INSERT INTO messages VALUES(1,2,10,2,'Peer','original archive',123,'',NULL,NULL,1,0);
            """)
            db.close()
            for _ in range(2):
                archive = Archive(directory)
                try:
                    self.assertEqual(archive.get(1, 2, 10)["text"], "original archive")
                    self.assertTrue(archive.get(1, 2, 10)["deleted"])
                    self.assertTrue(archive.is_muted(1, 2))
                    self.assertIsNotNone(archive.known_chat(1, 2)["key"])
                    self.assertEqual(archive.user(1)["mute_mode"], "fast")
                    self.assertEqual(archive.db.execute("PRAGMA user_version").fetchone()[0], 2)
                finally:
                    archive.close()

    def test_cards_escape_html_and_fit_telegram_utf16_limits(self):
        payload = dict(kind="edited", sender_id=2, sender_name="<b>" + "😀" * 300,
                       date=int(time.time()), old_text="😀" * 5000, new_text="😀" * 5000)
        rendered = ui.notice(payload)
        self.assertIn("&lt;b&gt;", rendered)
        plain = html.unescape(re.sub(r"<[^>]+>", "", rendered))
        self.assertLessEqual(len(plain.encode("utf-16-le")) // 2, 4096)
        rendered = ui.notice({}, "😀" * 5000)
        self.assertLessEqual(len(html.unescape(rendered).encode("utf-16-le")) // 2, 4096)

    def test_conversation_with_long_emoji_names_and_text_fits_limit(self):
        row = dict(sender_name="😀" * 300, outgoing=0, deleted=0, suppressed=0,
                   date=int(time.time()), text="😀" * 5000, photo_id=None, media_note="")
        rendered = ui.conversation({"title": "😀" * 300}, [row] * 4, 4, 0, 1)
        plain = html.unescape(re.sub(r"<[^>]+>", "", rendered))
        self.assertLessEqual(len(plain.encode("utf-16-le")) // 2, 4096)


if __name__ == "__main__":
    unittest.main()
