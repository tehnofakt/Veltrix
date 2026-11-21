"""Telegram cards and an offline chat export; every user-supplied string is escaped."""

import html
import re
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

MENU = ReplyKeyboardMarkup(keyboard=[
    [KeyboardButton(text="💬 Переписки"), KeyboardButton(text="⚙️ Настройки")],
    [KeyboardButton(text="🔌 Подключение"), KeyboardButton(text="❔ Помощь")],
], resize_keyboard=True, input_field_placeholder="Выберите действие или введите команду")


def esc(value):
    return html.escape(str(value), quote=False)


def short(value, size=450):
    value = str(value or "")
    return value if len(value) <= size else value[:size].rstrip() + "…"


def person(name, uid=None):
    label = esc(short(name or "Собеседник", 80))
    return f'<a href="tg://user?id={int(uid)}">{label}</a>' if uid and int(uid) > 0 else label


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).strftime("%d.%m · %H:%M UTC")


def messages_count(value):
    if value % 10 == 1 and value % 100 != 11:
        noun = "сообщение"
    elif value % 10 in {2, 3, 4} and value % 100 not in {12, 13, 14}:
        noun = "сообщения"
    else:
        noun = "сообщений"
    return f"{value} {noun}"


def keyboard(rows):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=data) for label, data in row] for row in rows
    ])


def home(connected, active):
    state = "🟢 Подключён" if connected else "⚪️ Ожидает подключения"
    if not active:
        state = "⏸ Обработка приостановлена"
    return f"<b>Veltrix</b>\nВаши переписки — под контролем.\n\n{state}\n\nАрхив, мут и управление отправкой — в меню ниже."


CONNECT = """<b>Подключение за три шага</b>

<b>1.</b> Откройте Telegram → профиль / настройки → <b>Автоматизация чатов</b> или <b>Чат-боты</b>.
<b>2.</b> Выберите этого бота и личные чаты, к которым разрешаете доступ.
<b>3.</b> Разрешите ответы и удаление сообщений, затем вернитесь сюда.

Одного /start недостаточно. Номер, пароль и коды входа передавать не нужно.

После подключения обе стороны новой переписки сохраняются на сервере Veltrix. История до подключения недоступна. Владелец сервера имеет доступ к архиву.
Проверить подключение: /status."""

HELP = """<b>Как пользоваться Veltrix</b>

<b>В подключённой переписке</b>
<code>.mute</code> — скрывать новые входящие
<code>.unmute</code> — отключить мут
<code>.spam 10 привет</code> — отправить 10 сообщений
<code>.stopspam</code> — остановить отправку
<code>.speed 0.1</code> — пауза между сообщениями

<b>В этом боте</b>
💬 <b>Переписки</b> — история, фото и скачивание всего сохранённого чата.
⚙️ <b>Настройки</b> — скорость отправки и режим мута.

Непрерывный тест разрешён только здесь:
<code>.spam forever тест</code> → <code>.stopspam</code>.

/stop — приостановить обработку
/start — возобновить
/erase confirm — удалить свой серверный архив

Telegram определяет доступность чатов и ограничения отправки. Событие удаления не сообщает, кто именно удалил сообщение."""

PRIVACY = """<b>Ваши данные</b>

Архив хранится на сервере Veltrix, фотографии — в его локальной папке. В GitHub и сторонние хранилища переписки не отправляются. Уведомления и экспорт приходят в этот диалог.

Владелец сервера технически имеет доступ к файлам. Это не сквозное шифрование.

<code>/stop</code> приостанавливает обработку.
<code>/erase confirm</code> удаляет свои записи и файлы с сервера. Копии в Telegram и резервных копиях автоматически не стираются.
Отозвать доступ можно в настройках Telegram → Автоматизация чатов.

Архив начинается после подключения. Бот не может восстановить неизвестную ему старую переписку или гарантировать получение каждого события Telegram."""


def notice(payload, legacy=""):
    kind = payload.get("kind")
    if kind == "edited":
        return ("✏️ <b>Сообщение изменено</b>\n"
                f"{person(payload['sender_name'], payload.get('sender_id'))} · {stamp(payload['date'])}\n\n"
                f"<b>Было</b>\n<blockquote>{esc(short(payload['old_text'], 650) or 'Без текста')}</blockquote>\n"
                f"<b>Стало</b>\n<blockquote>{esc(short(payload['new_text'], 650) or 'Без текста')}</blockquote>")
    if kind == "deleted":
        count = len(payload.get("message_ids", []))
        heading = "Сообщение удалено" if count == 1 else f"Удалено сообщений: {count}"
        sender = "Вы" if payload.get("outgoing") else payload.get("sender_name", "Собеседник")
        caption = "Последнее из сохранённых" if count > 1 else "Отправитель"
        return (f"🗑 <b>{heading}</b>\n{caption}: {person(sender, payload.get('sender_id'))}\n"
                f"<i>{stamp(payload['date'])}</i>\n\n"
                f"<blockquote>{esc(short(payload.get('preview'), 900) or 'Без текста')}</blockquote>"
                + ("\n📷 Фотография" if payload.get("has_photo") else ""))
    # Old undelivered notices keep their content but lose technical metadata.
    text = re.sub(r"(?m)^Чат: -?\d+[^\n]*\n?", "", legacy)
    text = re.sub(r"\(ID -?\d+\)", "", text)
    return esc(short(text, 1200))


def conversation(chat, rows, total, page, pages):
    parts = [f"<b>{esc(short(chat['title'], 80))}</b>",
             f"Сохранено: {messages_count(total)} · {page + 1}/{pages}", ""]
    for row in rows:
        sender = "Вы" if row["outgoing"] else row["sender_name"]
        flag = " · 🔇 скрыто мутом" if row["deleted"] and row["suppressed"] else (
            " · удалено" if row["deleted"] else "")
        parts.append(f"<b>{esc(short(sender, 70))}</b> · <i>{stamp(row['date'])}{flag}</i>")
        if row["text"]:
            parts.append(f"<blockquote>{esc(short(row['text'], 350))}</blockquote>")
        if row["photo_id"]:
            label = {"ready": "📷 Фото", "failed": "📷 Фото не удалось скачать"}.get(row["photo_state"], "📷 Фото загружается")
            parts.append(label)
        elif row["media_note"]:
            parts.append(esc(short(row["media_note"], 100)))
        parts.append("")
    if not rows:
        parts.append("Пока нет сохранённых сообщений.")
    parts.append("Полный текст и история изменений — в скачиваемом архиве.")
    return "\n".join(parts)


def build_export(directory, owner, chat, rows, revisions, media_root, max_size=45 * 1024 * 1024):
    """Return a ZIP with offline HTML/photos, or complete HTML if media is too large."""
    directory, media_root = Path(directory), Path(media_root).resolve()
    prefix = f"{int(owner)}_{int(chat['key'])}_{uuid.uuid4().hex}"
    changes = defaultdict(list)
    for revision in revisions:
        changes[revision["message_id"]].append(revision)
    files = {}
    for row in rows:
        if row["photo_path"]:
            path = Path(row["photo_path"]).resolve()
            if path.is_relative_to(media_root) and path.is_file():
                files[row["message_id"]] = path

    def render(with_photos):
        bubbles = []
        for row in rows:
            who = "Вы" if row["outgoing"] else row["sender_name"]
            badge = "Скрыто мутом" if row["deleted"] and row["suppressed"] else (
                "Сообщение удалено" if row["deleted"] else "")
            photo = ""
            if row["message_id"] in files:
                if with_photos:
                    photo = f'<a href="media/{row["message_id"]}.jpg"><img loading="lazy" src="media/{row["message_id"]}.jpg" alt="Фото"></a>'
                else:
                    photo = '<p class="hint">Фотография доступна в диалоге с Veltrix.</p>'
            elif row["media_note"]:
                photo = f'<p class="hint">{esc(row["media_note"])}</p>'
            versions = "".join(f'<section><small>{stamp(v["changed_at"])}</small><pre>{esc(v["old_text"])}</pre>'
                               f'<p>После изменения</p><pre>{esc(v["new_text"])}</pre></section>'
                               for v in changes[row["message_id"]])
            history = f'<details><summary>История изменений</summary>{versions}</details>' if versions else ""
            css = "message outgoing" if row["outgoing"] else "message"
            bubbles.append(f'<article class="{css}"><header><b>{esc(who)}</b><time>{stamp(row["date"])}</time></header>'
                           f'<pre>{esc(row["text"])}</pre>{photo}{history}<footer>{esc(badge)}</footer></article>')
        return ("<!doctype html><html lang=ru><meta charset=utf-8><meta name=viewport content='width=device-width, initial-scale=1'>"
                "<meta http-equiv=Content-Security-Policy content=\"default-src 'none'; style-src 'unsafe-inline'; img-src 'self'\">"
                f"<title>{esc(chat['title'])} — Veltrix</title><style>"
                "*{box-sizing:border-box}body{margin:0;background:#f2f5fa;color:#1e293b;font:16px/1.6 system-ui,sans-serif;overflow-wrap:anywhere}"
                "main{max-width:900px;margin:auto;padding:36px 20px}h1{font-size:30px;line-height:1.2}.intro{margin-bottom:30px;color:#64748b}"
                ".message{background:white;border:1px solid #e2e8f0;border-radius:18px;padding:18px 22px;margin:14px 10% 14px 0;box-shadow:0 4px 12px #14213d05}"
                ".outgoing{margin:14px 0 14px 10%;background:#eaf3ff;border-color:#d3e5ff}header{display:flex;flex-wrap:wrap;justify-content:space-between;gap:10px}"
                "time,small,.hint,footer{font-size:12px;color:#64748b}pre{font:inherit;white-space:pre-wrap;overflow-wrap:anywhere;margin:10px 0}"
                "img{display:block;max-width:100%;max-height:600px;border-radius:10px}details{border-top:1px solid #dce4ee;padding-top:10px;margin-top:12px}"
                "summary{cursor:pointer;color:#4062b9}details section{padding:12px;background:#ffffff80;margin-top:10px;border-radius:8px}"
                "@media(max-width:600px){main{padding:20px 12px}.message,.outgoing{margin:12px 0;padding:15px}}"
                f"</style><main><h1>{esc(chat['title'])}</h1><p class=intro>Veltrix · {messages_count(len(rows))} · время UTC<br>"
                "Только события, полученные после подключения бота. Удаление оригинала не удаляет эту копию.</p>"
                + "".join(bubbles) + "</main></html>").encode("utf-8")

    page = render(True)
    if len(page) + sum(path.stat().st_size for path in files.values()) > max_size:
        target = directory / f"{prefix}.html"
        page = render(False)
        if len(page) > max_size:
            raise ValueError("Архив слишком большой для одного файла Telegram. Переписка доступна постранично в боте.")
        target.write_bytes(page)
        return target, False
    target = directory / f"{prefix}.zip"
    try:
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_STORED) as bundle:
            bundle.writestr("Переписка.html", page)
            for mid, path in files.items():
                bundle.write(path, f"media/{mid}.jpg")
        return target, True
    except BaseException:
        target.unlink(missing_ok=True)
        raise
