"""Local, tenant-scoped storage with migrations from the first Veltrix release."""

import json
import sqlite3
import time
from pathlib import Path


class Archive:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.media = self.directory / "media"
        self.media.mkdir(exist_ok=True)
        self.exports = self.directory / "exports"
        self.exports.mkdir(exist_ok=True)
        self.db = sqlite3.connect(self.directory / "veltrix.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                owner_id INTEGER PRIMARY KEY, chat_id INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1
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
            CREATE TABLE IF NOT EXISTS chat_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL, UNIQUE(owner_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS muted (
                owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, PRIMARY KEY(owner_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS messages (
                owner_id INTEGER NOT NULL, chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL, sender_name TEXT NOT NULL, text TEXT NOT NULL,
                date INTEGER NOT NULL, media_note TEXT NOT NULL, photo_id TEXT, photo_path TEXT,
                deleted INTEGER NOT NULL DEFAULT 0, suppressed INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(owner_id, chat_id, message_id)
            );
            CREATE TABLE IF NOT EXISTS revisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                old_text TEXT NOT NULL, new_text TEXT NOT NULL, changed_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT, owner_id INTEGER NOT NULL, text TEXT NOT NULL,
                photo_path TEXT, text_offset INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0
            );
        """)
        additions = {
            "users": {"spam_delay": "REAL", "mute_mode": "TEXT NOT NULL DEFAULT 'fast'"},
            "messages": {
                "outgoing": "INTEGER NOT NULL DEFAULT 0", "connection_id": "TEXT",
                "photo_state": "TEXT NOT NULL DEFAULT 'none'", "photo_attempts": "INTEGER NOT NULL DEFAULT 0",
                "photo_retry_at": "REAL NOT NULL DEFAULT 0", "delete_pending": "INTEGER NOT NULL DEFAULT 0",
            },
            "notifications": {"payload": "TEXT NOT NULL DEFAULT '{}'", "text_sent": "INTEGER NOT NULL DEFAULT 0"},
        }
        with self.db:
            for table, columns in additions.items():
                existing = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})")}
                for name, definition in columns.items():
                    if name not in existing:
                        self.db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")
            self.db.execute("INSERT OR IGNORE INTO chat_links(owner_id,chat_id) SELECT owner_id,chat_id FROM chats")
            if self.db.execute("PRAGMA user_version").fetchone()[0] < 2:
                self.db.execute("""UPDATE messages SET outgoing=(sender_id=owner_id), photo_state=
                    CASE WHEN photo_path IS NOT NULL THEN 'ready'
                         WHEN photo_id IS NOT NULL THEN 'pending' ELSE 'none' END""")
                self.db.execute("UPDATE notifications SET text_sent=1 WHERE text_offset>=LENGTH(text) AND text_offset>0")
                self.db.execute("PRAGMA user_version=2")
        self.db.executescript("""
            CREATE INDEX IF NOT EXISTS timeline ON messages(owner_id,chat_id,date,message_id);
            CREATE INDEX IF NOT EXISTS photo_jobs ON messages(photo_state,photo_retry_at);
            CREATE INDEX IF NOT EXISTS deletion_jobs ON messages(delete_pending);
            CREATE INDEX IF NOT EXISTS revisions_message ON revisions(owner_id,chat_id,message_id);
            CREATE INDEX IF NOT EXISTS notification_jobs ON notifications(delivered,retry_at,id);
        """)

    def user(self, owner):
        return self.db.execute("SELECT * FROM users WHERE owner_id=?", (owner,)).fetchone()

    def start_user(self, owner, chat_id):
        with self.db:
            self.db.execute("""INSERT INTO users(owner_id,chat_id,active) VALUES(?,?,1)
                ON CONFLICT(owner_id) DO UPDATE SET chat_id=excluded.chat_id,active=1""", (owner, chat_id))

    def pause_user(self, owner):
        with self.db:
            self.db.execute("UPDATE users SET active=0 WHERE owner_id=?", (owner,))

    def preference(self, owner, *, delay=None, mode=None):
        with self.db:
            if delay is not None:
                self.db.execute("UPDATE users SET spam_delay=? WHERE owner_id=?", (delay, owner))
            if mode is not None:
                if mode not in {"fast", "safe"}:
                    raise ValueError("Неизвестный режим мута")
                self.db.execute("UPDATE users SET mute_mode=? WHERE owner_id=?", (mode, owner))

    def connection(self, cid):
        row = self.db.execute("SELECT * FROM connections WHERE id=?", (cid,)).fetchone()
        return dict(row, rights=json.loads(row["rights"])) if row else None

    def active_connection(self, owner):
        row = self.db.execute("SELECT id FROM connections WHERE owner_id=? AND enabled=1", (owner,)).fetchone()
        return self.connection(row[0]) if row else None

    def save_connection(self, connection):
        with self.db:
            if connection.is_enabled:
                self.db.execute("UPDATE connections SET enabled=0 WHERE owner_id=?", (connection.user.id,))
            self.db.execute("""INSERT INTO connections VALUES(?,?,?,?) ON CONFLICT(id)
                DO UPDATE SET enabled=excluded.enabled,rights=excluded.rights""",
                (connection.id, connection.user.id, int(connection.is_enabled),
                 json.dumps(connection.rights.model_dump() if connection.rights else {})))

    def known_chat(self, owner, chat_id):
        return self.db.execute("""SELECT c.*,l.id AS key FROM chats c JOIN chat_links l
            USING(owner_id,chat_id) WHERE c.owner_id=? AND c.chat_id=?""", (owner, chat_id)).fetchone()

    def chat_by_key(self, owner, key):
        return self.db.execute("""SELECT c.*,l.id AS key FROM chats c JOIN chat_links l
            USING(owner_id,chat_id) WHERE l.owner_id=? AND l.id=?""", (owner, key)).fetchone()

    def touch_chat(self, owner, message):
        with self.db:
            self._touch_chat(owner, message)

    def _touch_chat(self, owner, message):
        title = message.chat.full_name or "Личная переписка"
        self.db.execute("""INSERT INTO chats VALUES(?,?,?,?) ON CONFLICT(owner_id,chat_id)
            DO UPDATE SET title=excluded.title,last_seen=MAX(last_seen,excluded.last_seen)""",
            (owner, message.chat.id, title, int(message.date.timestamp())))
        self.db.execute("INSERT OR IGNORE INTO chat_links(owner_id,chat_id) VALUES(?,?)", (owner, message.chat.id))

    def chats(self, owner, page=0, size=8):
        count = self.db.execute("SELECT COUNT(*) FROM chats WHERE owner_id=?", (owner,)).fetchone()[0]
        rows = self.db.execute("""SELECT c.*,l.id AS key,
            (SELECT COUNT(*) FROM messages m WHERE m.owner_id=c.owner_id AND m.chat_id=c.chat_id) AS total
            FROM chats c JOIN chat_links l USING(owner_id,chat_id)
            WHERE c.owner_id=? ORDER BY last_seen DESC,c.chat_id LIMIT ? OFFSET ?""",
            (owner, size, page * size)).fetchall()
        return rows, count

    def is_muted(self, owner, chat_id):
        return self.db.execute("SELECT 1 FROM muted WHERE owner_id=? AND chat_id=?", (owner, chat_id)).fetchone() is not None

    def mute(self, owner, chat_id, enabled):
        with self.db:
            if enabled:
                self.db.execute("INSERT OR IGNORE INTO muted VALUES(?,?)", (owner, chat_id))
            else:
                self.db.execute("DELETE FROM muted WHERE owner_id=? AND chat_id=?", (owner, chat_id))

    def get(self, owner, chat_id, mid):
        return self.db.execute("SELECT * FROM messages WHERE owner_id=? AND chat_id=? AND message_id=?",
                               (owner, chat_id, mid)).fetchone()

    def save_message(self, owner, message, note="", photo_id=None, path=None, *, edited=False, mute_pending=False):
        old = self.get(owner, message.chat.id, message.message_id)
        if old and old["photo_id"] == photo_id:
            path = path or old["photo_path"]
            state = "ready" if path else old["photo_state"]
        else:
            state = "ready" if path else ("pending" if photo_id else "none")
        sender = message.from_user
        text = message.text or message.caption or ""
        outgoing = int(sender.id == owner or bool(message.sender_business_bot))
        with self.db:
            self._touch_chat(owner, message)
            self.db.execute("""INSERT INTO messages
                (owner_id,chat_id,message_id,sender_id,sender_name,text,date,media_note,
                 photo_id,photo_path,outgoing,connection_id,photo_state,delete_pending)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(owner_id,chat_id,message_id) DO UPDATE SET
                text=excluded.text,sender_name=excluded.sender_name,media_note=excluded.media_note,
                photo_id=excluded.photo_id,photo_path=excluded.photo_path,outgoing=excluded.outgoing,
                connection_id=excluded.connection_id,photo_state=excluded.photo_state,
                delete_pending=CASE WHEN messages.deleted=0 AND excluded.delete_pending=1 THEN 1 ELSE messages.delete_pending END,
                photo_attempts=CASE WHEN messages.photo_id IS excluded.photo_id THEN messages.photo_attempts ELSE 0 END,
                photo_retry_at=CASE WHEN messages.photo_id IS excluded.photo_id THEN messages.photo_retry_at ELSE 0 END""",
                (owner, message.chat.id, message.message_id, sender.id, sender.full_name, text,
                 int(message.date.timestamp()), note, photo_id, path, outgoing,
                 message.business_connection_id, state, int(mute_pending)))
            if edited and old and old["text"] != text:
                changed_at = int(message.edit_date or time.time())
                self.db.execute("""INSERT INTO revisions(owner_id,chat_id,message_id,old_text,new_text,changed_at)
                    VALUES(?,?,?,?,?,?)""", (owner, message.chat.id, message.message_id, old["text"], text, changed_at))
                if not outgoing:
                    payload = {"kind": "edited", "chat_id": message.chat.id, "message_id": message.message_id,
                               "sender_id": sender.id, "sender_name": sender.full_name, "date": changed_at,
                               "old_text": old["text"], "new_text": text}
                    plain = f"✏️ Сообщение изменено\n{sender.full_name}\n\nБыло:\n{old['text']}\n\nСтало:\n{text}"
                    self._enqueue(owner, plain, payload=payload)
        return old

    def suppress(self, owner, chat_id, mid, enabled=True, *, pending=None):
        with self.db:
            self.db.execute("UPDATE messages SET suppressed=? WHERE owner_id=? AND chat_id=? AND message_id=?",
                            (int(enabled), owner, chat_id, mid))
            if pending is not None:
                self.db.execute("UPDATE messages SET delete_pending=? WHERE owner_id=? AND chat_id=? AND message_id=?",
                                (int(pending), owner, chat_id, mid))

    def _enqueue(self, owner, text, photo_path=None, payload=None):
        self.db.execute("INSERT INTO notifications(owner_id,text,photo_path,payload) VALUES(?,?,?,?)",
                        (owner, text, photo_path, json.dumps(payload or {}, ensure_ascii=False)))

    def enqueue(self, owner, text, photo_path=None, *, payload=None):
        with self.db:
            self._enqueue(owner, text, photo_path, payload)

    def delete_batch(self, owner, chat_id, message_ids):
        visible = []
        with self.db:
            for mid in dict.fromkeys(message_ids):
                row = self.get(owner, chat_id, mid)
                if not row or row["deleted"]:
                    continue
                self.db.execute("UPDATE messages SET deleted=1,delete_pending=0 WHERE owner_id=? AND chat_id=? AND message_id=?",
                                (owner, chat_id, mid))
                if not row["suppressed"]:
                    visible.append(dict(row))
            if visible:
                last = visible[-1]
                payload = {"kind": "deleted", "chat_id": chat_id, "message_ids": [row["message_id"] for row in visible],
                           "sender_id": last["sender_id"], "sender_name": last["sender_name"],
                           "outgoing": last["outgoing"], "date": last["date"], "preview": last["text"],
                           "has_photo": bool(last["photo_id"])}
                text = f"🗑 Сообщение удалено\n{last['sender_name']}\n\n{last['text']}"
                self._enqueue(owner, text, last["photo_path"] if len(visible) == 1 else None, payload)
        return len(visible)

    def mark_deleted(self, row):
        return self.delete_batch(row["owner_id"], row["chat_id"], [row["message_id"]])

    def pending_photos(self, size=4):
        return self.db.execute("""SELECT m.* FROM messages m JOIN users u USING(owner_id)
            WHERE u.active=1 AND m.photo_state IN ('pending','retry') AND m.photo_retry_at<=?
            ORDER BY m.date LIMIT ?""", (time.time(), size)).fetchall()

    def finish_photo(self, row, path=None):
        with self.db:
            if path:
                self.db.execute("""UPDATE messages SET photo_path=?,photo_state='ready',media_note='Фото'
                    WHERE owner_id=? AND chat_id=? AND message_id=? AND photo_id=?""",
                    (path, row["owner_id"], row["chat_id"], row["message_id"], row["photo_id"]))
            else:
                attempts = row["photo_attempts"] + 1
                state = "failed" if attempts >= 3 else "retry"
                self.db.execute("""UPDATE messages SET photo_state=?,photo_attempts=?,photo_retry_at=?,
                    media_note='Фото не удалось скачать' WHERE owner_id=? AND chat_id=? AND message_id=? AND photo_id=?""",
                    (state, attempts, time.time() + min(60, attempts * 5), row["owner_id"], row["chat_id"], row["message_id"], row["photo_id"]))

    def pending_deletions(self):
        return self.db.execute("""SELECT m.* FROM messages m JOIN users u USING(owner_id)
            WHERE m.delete_pending=1 AND m.deleted=0 AND u.active=1 LIMIT 100""").fetchall()

    def conversation(self, owner, key, page=None, size=4):
        chat = self.chat_by_key(owner, key)
        if not chat:
            raise ValueError("Переписка недоступна")
        total = self.db.execute("SELECT COUNT(*) FROM messages WHERE owner_id=? AND chat_id=?",
                                (owner, chat["chat_id"])).fetchone()[0]
        pages = max(1, (total + size - 1) // size)
        page = pages - 1 if page is None or page < 0 else min(page, pages - 1)
        rows = self.db.execute("""SELECT * FROM messages WHERE owner_id=? AND chat_id=?
            ORDER BY date,message_id LIMIT ? OFFSET ?""", (owner, chat["chat_id"], size, page * size)).fetchall()
        return chat, rows, total, page, pages

    def snapshot(self, owner, key):
        chat = self.chat_by_key(owner, key)
        if not chat:
            raise ValueError("Переписка недоступна")
        rows = [dict(row) for row in self.db.execute("SELECT * FROM messages WHERE owner_id=? AND chat_id=? ORDER BY date,message_id",
                                                   (owner, chat["chat_id"]))]
        revisions = [dict(row) for row in self.db.execute("SELECT * FROM revisions WHERE owner_id=? AND chat_id=? ORDER BY changed_at,id",
                                                        (owner, chat["chat_id"]))]
        return dict(chat), rows, revisions

    def erase(self, owner):
        for directory in (self.media, self.exports):
            for path in directory.glob(f"{int(owner)}_*"):
                if path.is_file():
                    path.unlink()
        with self.db:
            for table in ("messages", "muted", "chats", "chat_links", "revisions", "notifications", "connections"):
                self.db.execute(f"DELETE FROM {table} WHERE owner_id=?", (owner,))
            self.db.execute("UPDATE users SET active=0,spam_delay=NULL,mute_mode='fast' WHERE owner_id=?", (owner,))

    def close(self):
        self.db.close()
