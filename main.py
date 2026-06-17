#!/usr/bin/env python3
"""
👁 DialogTrackerX Bot
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

_bot_message_ids: set[tuple] = set()

_MSK = timezone(timedelta(hours=3))

def _now_str() -> str:
    return datetime.now(_MSK).strftime("%d.%m.%Y в %H:%M:%S")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    BusinessConnection, BotCommand,
    FSInputFile, InputMediaPhoto, URLInputFile,
    MessageReactionUpdated,
)
from aiogram.utils.media_group import MediaGroupBuilder

from database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# КОНФИГ
# ══════════════════════════════════════════════

BOT_TOKEN        = os.getenv("BOT_TOKEN", "8970898437:AAH0eThholB_n3EzxYwatiHAYBVRW6J04dI")
START_PHOTO_URL  = os.getenv("START_PHOTO_URL", "")
_data_dir     = os.getenv("DATA_DIR", os.getenv("DB_PATH_DIR", "/app/data"))
DB_PATH       = os.getenv("DB_PATH", os.path.join(_data_dir, "shadowwatch.db"))
ADMIN_IDS     = [int(x) for x in os.getenv("ADMIN_IDS", "7965055989").split(",") if x.strip()]
BOT_USERNAME  = "DialogTrackerXbot"
BOT_NAME      = "DialogTrackerX"
MEDIA_DIR     = Path(os.getenv("MEDIA_DIR", "/app/data/media"))

# Папка для БД должна существовать ДО создания Database(), иначе sqlite3.connect упадёт
Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
db = Database(DB_PATH)

# ══════════════════════════════════════════════
# БАЗА ДАННЫХ
# ══════════════════════════════════════════════

def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def _init_db_sync():
    c = _conn()
    cur = c.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER PRIMARY KEY,
        username   TEXT,
        first_name TEXT,
        registered TEXT DEFAULT (datetime('now')),
        last_seen  TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS message_cache (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id     INTEGER NOT NULL,
        message_id  INTEGER NOT NULL,
        owner_id    INTEGER,
        user_id     INTEGER,
        username    TEXT,
        first_name  TEXT,
        text        TEXT,
        media_type  TEXT,
        file_id     TEXT,
        is_view_once INTEGER DEFAULT 0,
        is_outgoing  INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now')),
        UNIQUE(chat_id, message_id)
    );
    CREATE TABLE IF NOT EXISTS user_settings (
        user_id              INTEGER PRIMARY KEY,
        notify_delete        INTEGER DEFAULT 1,
        notify_edit          INTEGER DEFAULT 1,
        notify_self_destruct INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS business_connections (
        connection_id TEXT PRIMARY KEY,
        owner_id      INTEGER NOT NULL,
        connected_at  TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS kv_store (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS targets (
        target_user_id  INTEGER PRIMARY KEY,
        set_by          INTEGER NOT NULL,
        set_at          TEXT DEFAULT (datetime('now')),
        notify_messages INTEGER DEFAULT 1,
        notify_deleted  INTEGER DEFAULT 1,
        notify_edited   INTEGER DEFAULT 1,
        notify_viewonce INTEGER DEFAULT 1
    );
    """)
    # Migrations
    try:
        c.execute("ALTER TABLE users ADD COLUMN ever_connected INTEGER DEFAULT 0")
        c.commit()
    except Exception:
        pass
    for col, default in [
        ("notify_messages", 1), ("notify_deleted", 1),
        ("notify_edited", 1),   ("notify_viewonce", 1)
    ]:
        try:
            c.execute(f"ALTER TABLE targets ADD COLUMN {col} INTEGER DEFAULT {default}")
            c.commit()
        except Exception:
            pass
    try:
        c.execute("ALTER TABLE message_cache ADD COLUMN is_outgoing INTEGER DEFAULT 0")
        c.commit()
    except Exception:
        pass
    c.commit()
    c.close()

async def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    await asyncio.get_event_loop().run_in_executor(None, _init_db_sync)

def _kv_set(key: str, value: str):
    c = _conn()
    c.execute("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", (key, value))
    c.commit()
    c.close()

def _kv_get(key: str):
    c = _conn()
    row = c.execute("SELECT value FROM kv_store WHERE key=?", (key,)).fetchone()
    c.close()
    return row["value"] if row else None

def _load_section_photo_cache():
    for key in ("help", "main", "settings", "setup", "expired"):
        fid = _kv_get(f"section_photo:{key}")
        if fid:
            _section_photo_cache[key] = fid

def _run(fn):
    return asyncio.get_event_loop().run_in_executor(None, fn)

# ── Business connections ──

_biz_owners: dict = {}

def has_biz_connection(uid: int) -> bool:
    return uid in _biz_owners.values()

async def save_biz_connection(connection_id: str, owner_id: int):
    _biz_owners[connection_id] = owner_id
    def _f():
        c = _conn()
        c.execute("""INSERT INTO business_connections (connection_id, owner_id)
            VALUES (?, ?) ON CONFLICT(connection_id) DO UPDATE SET owner_id=excluded.owner_id""",
            (connection_id, owner_id))
        c.execute("UPDATE users SET ever_connected=1 WHERE user_id=?", (owner_id,))
        c.commit(); c.close()
    await _run(_f)

async def remove_biz_connection(connection_id: str):
    _biz_owners.pop(connection_id, None)
    def _f():
        c = _conn()
        c.execute("DELETE FROM business_connections WHERE connection_id=?", (connection_id,))
        c.commit(); c.close()
    await _run(_f)

async def restore_biz_connections():
    def _f():
        c = _conn()
        rows = c.execute("SELECT connection_id, owner_id FROM business_connections").fetchall()
        c.close()
        return [(r["connection_id"], r["owner_id"]) for r in rows]
    pairs = await _run(_f)
    for conn_id, owner_id in pairs:
        _biz_owners[conn_id] = owner_id
    logger.info(f"Восстановлено {len(pairs)} business-подключений из БД")

def get_biz_owner(bc_id: str | None) -> int | None:
    if not bc_id: return None
    return _biz_owners.get(bc_id)

async def resolve_biz_owner(bc_id: str | None, bot: Bot) -> int | None:
    if not bc_id: return None
    owner_id = _biz_owners.get(bc_id)
    if owner_id: return owner_id
    try:
        bc = await bot.get_business_connection(bc_id)
        if bc and bc.user:
            owner_id = bc.user.id
            await upsert_user(owner_id, bc.user.username, bc.user.first_name)
            await save_biz_connection(bc_id, owner_id)
            return owner_id
    except Exception as ex:
        logger.warning(f"resolve_biz_owner: {bc_id}: {ex}")
    return None

# ── Targets ──

_targets: set = set()

async def add_target(target_uid: int, set_by: int):
    _targets.add(target_uid)
    def _f():
        c = _conn()
        c.execute("""INSERT INTO targets
            (target_user_id, set_by, notify_messages, notify_deleted, notify_edited, notify_viewonce)
            VALUES (?, ?, 1, 1, 1, 1)
            ON CONFLICT(target_user_id) DO UPDATE SET
            set_by=excluded.set_by, set_at=datetime('now')""",
            (target_uid, set_by))
        c.commit(); c.close()
    await _run(_f)

async def get_target(target_uid: int) -> dict | None:
    def _f():
        c = _conn()
        row = c.execute("""SELECT t.*, u.username, u.first_name FROM targets t
            LEFT JOIN users u ON u.user_id=t.target_user_id
            WHERE t.target_user_id=?""", (target_uid,)).fetchone()
        c.close()
        return dict(row) if row else None
    return await _run(_f)

async def toggle_target_setting(target_uid: int, field: str):
    if field not in {"notify_messages","notify_deleted","notify_edited","notify_viewonce"}: return
    def _f():
        c = _conn()
        c.execute(f"UPDATE targets SET {field}=1-{field} WHERE target_user_id=?", (target_uid,))
        c.commit(); c.close()
    await _run(_f)

async def get_target_settings(target_uid: int) -> dict:
    t = await get_target(target_uid)
    if not t: return {"notify_messages":1,"notify_deleted":1,"notify_edited":1,"notify_viewonce":1}
    return t

async def remove_target(target_uid: int):
    _targets.discard(target_uid)
    def _f():
        c = _conn()
        c.execute("DELETE FROM targets WHERE target_user_id=?", (target_uid,))
        c.commit(); c.close()
    await _run(_f)

async def restore_targets():
    def _f():
        c = _conn()
        rows = c.execute("SELECT target_user_id FROM targets").fetchall()
        c.close()
        return [r["target_user_id"] for r in rows]
    uids = await _run(_f)
    for uid in uids:
        _targets.add(uid)
    logger.info(f"Восстановлено {len(uids)} targets из БД")

async def get_all_targets():
    def _f():
        c = _conn()
        rows = c.execute("""SELECT t.*, u.username, u.first_name FROM targets t
            LEFT JOIN users u ON u.user_id=t.target_user_id
            ORDER BY t.set_at DESC""").fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

def is_target(uid: int) -> bool:
    return uid in _targets

# ── Пользователи ──

async def upsert_user(uid, username=None, first_name=None):
    def _f():
        c = _conn()
        c.execute("""INSERT INTO users (user_id, username, first_name, last_seen)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_seen=datetime('now')""", (uid, username, first_name))
        c.commit(); c.close()
    await _run(_f)

async def get_all_users():
    def _f():
        c = _conn()
        rows = c.execute("SELECT * FROM users ORDER BY registered DESC").fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

async def cache_message(chat_id, message_id, user_id, username, first_name,
                        text=None, media_type=None, file_id=None,
                        owner_id=None, is_view_once=False, is_outgoing=False):
    def _f():
        c = _conn()
        c.execute("""INSERT OR REPLACE INTO message_cache
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, is_view_once, is_outgoing)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (chat_id, message_id, owner_id, user_id, username, first_name,
             text, media_type, file_id, int(is_view_once), int(is_outgoing)))
        c.commit(); c.close()
    await _run(_f)

async def get_cached_message(chat_id, message_id):
    def _f():
        c = _conn()
        row = c.execute("SELECT * FROM message_cache WHERE chat_id=? AND message_id=?",
                        (chat_id, message_id)).fetchone()
        c.close()
        return dict(row) if row else None
    return await _run(_f)

async def delete_cached_message(chat_id, message_id):
    def _f():
        c = _conn()
        c.execute("DELETE FROM message_cache WHERE chat_id=? AND message_id=?",
                  (chat_id, message_id))
        c.commit(); c.close()
    await _run(_f)

async def get_user_settings(uid) -> dict:
    def _f():
        c = _conn()
        c.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (uid,))
        c.commit()
        row = c.execute("SELECT * FROM user_settings WHERE user_id=?", (uid,)).fetchone()
        c.close()
        return dict(row)
    return await _run(_f)

async def toggle_user_setting(uid, field):
    if field not in {"notify_delete","notify_edit","notify_self_destruct"}: return
    def _f():
        c = _conn()
        c.execute(f"""INSERT INTO user_settings (user_id, {field}) VALUES (?, 1)
            ON CONFLICT(user_id) DO UPDATE SET {field}=1-{field}""", (uid,))
        c.commit(); c.close()
    await _run(_f)

# ══════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════

def is_admin(uid): return uid in ADMIN_IDS or db.is_extra_admin(uid)

def user_link(uid, first_name, username=None):
    name = first_name or "Пользователь"
    uname = f" (@{username})" if username else ""
    return f'<a href="tg://user?id={uid}">{name}</a>{uname}'

def trim(t, n=None):
    if not t: return "<i>пусто</i>"
    return t

def extract_media(msg: Message):
    if msg.photo:      return "фото",          msg.photo[-1].file_id
    if msg.video:      return "видео",          msg.video.file_id
    if msg.video_note: return "видеосообщение", msg.video_note.file_id
    if msg.voice:      return "голосовое",      msg.voice.file_id
    if msg.audio:      return "аудио",          msg.audio.file_id
    if msg.document:   return "документ",       msg.document.file_id
    if msg.sticker:    return "стикер",         msg.sticker.file_id
    if msg.animation:  return "анимация",       msg.animation.file_id
    return None, None

def is_view_once_msg(msg: Message) -> bool:
    has_any_media = msg.photo or msg.video or msg.video_note or msg.voice
    if has_any_media:
        ttl = None
        spoiler = getattr(msg, "has_media_spoiler", None)
        protect = getattr(msg, "protect_content", None)
        if msg.photo:
            ttl     = getattr(msg.photo[-1], "ttl_seconds", None)
            spoiler = spoiler or getattr(msg.photo[-1], "has_media_spoiler", None)
        elif msg.video:
            ttl     = getattr(msg.video, "ttl_seconds", None)
            spoiler = spoiler or getattr(msg.video, "has_media_spoiler", None)
        elif msg.video_note:
            ttl     = getattr(msg.video_note, "ttl_seconds", None)
        elif msg.voice:
            ttl     = getattr(msg.voice, "ttl_seconds", None)
        if ttl:     return True
        if spoiler: return True
        if protect: return True
    return False

MEDIA_EMOJI = {
    "фото": "🖼", "видео": "🎬", "видеосообщение": "⭕",
    "голосовое": "🎤", "аудио": "🎵", "документ": "📄",
    "стикер": "🎭", "анимация": "🎞",
}

async def notify_admins(bot: Bot, text: str, **kwargs):
    for admin_id in ADMIN_IDS:
        try: await bot.send_message(admin_id, text, parse_mode="HTML", **kwargs)
        except Exception as ex: logger.warning(f"notify admin {admin_id}: {ex}")

async def safe_edit(call: CallbackQuery, text: str, **kwargs):
    msg = call.message
    try:
        if msg.photo or msg.document or msg.video:
            await msg.edit_caption(caption=text, parse_mode="HTML", **kwargs)
        else:
            await msg.edit_text(text, parse_mode="HTML", **kwargs)
    except Exception:
        try:
            _bot_message_ids.add((msg.chat.id, msg.message_id))
            await msg.delete()
        except Exception:
            _bot_message_ids.discard((msg.chat.id, msg.message_id))
        try:
            sent = await msg.answer(text, parse_mode="HTML", **kwargs)
            if sent:
                _bot_message_ids.add((sent.chat.id, sent.message_id))
        except: pass

_section_photo_cache: dict[str, str | None] = {
    "help":     None,
    "main":     None,
    "settings": None,
    "setup":    None,
    "expired":  None,
}
_SECTION_PHOTO_FILES = {
    "help":     "help_image.jpg",
    "main":     "cabinet_image.jpg",
    "settings": "notifications_image.jpg",
    "setup":    "start_image.jpg",
    "expired":  "expired_image.jpg",
}

async def send_with_explosion(call: CallbackQuery, section: str, text: str, kb, bot: Bot = None):
    msg = call.message
    photo_path = Path(__file__).parent / _SECTION_PHOTO_FILES.get(section, "start_image.jpg")

    cached_fid = _section_photo_cache.get(section)
    photo_source = None
    use_cached = False
    if cached_fid:
        photo_source = cached_fid
        use_cached = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)

    try:
        _bot_message_ids.add((msg.chat.id, msg.message_id))
        await msg.delete()
    except Exception:
        _bot_message_ids.discard((msg.chat.id, msg.message_id))

    try:
        if photo_source is not None:
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=kb,
                parse_mode="HTML"
            )
            if not use_cached and sent.photo:
                _section_photo_cache[section] = sent.photo[-1].file_id
            _bot_message_ids.add((sent.chat.id, sent.message_id))
        else:
            sent = await msg.answer(text, reply_markup=kb, parse_mode="HTML")
            _bot_message_ids.add((sent.chat.id, sent.message_id))
    except Exception as ex:
        logger.warning(f"send_with_explosion [{section}] error: {ex}")
        try:
            await msg.answer(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass

# ══════════════════════════════════════════════
# КЛАВИАТУРЫ
# ══════════════════════════════════════════════

def start_kb(uid: int = None):
    buttons = [
        [InlineKeyboardButton(text="⚡️ Перейти в Автоматизацию", url="tg://settings/edit")],
        [InlineKeyboardButton(text="❓ Как работает бот", callback_data="u:help")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Уведомления",  callback_data="u:settings")],
        [InlineKeyboardButton(text="❓ Как работает бот", callback_data="u:help")],
        [InlineKeyboardButton(text="◀️ Назад",       callback_data="u:back_start")],
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
    ])

def admin_kb(is_root: bool = False):
    rows = [
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm:users")],
        [InlineKeyboardButton(text="🔗 Подключения",   callback_data="adm:connections")],
        [InlineKeyboardButton(text="🎯 Таргеты",       callback_data="adm:targets")],
        [InlineKeyboardButton(text="📊 Статистика",    callback_data="adm:stats")],
        [InlineKeyboardButton(text="📢 Рассылка",      callback_data="adm:broadcast")],
    ]
    # Кнопка управления админами — только для корневых админов из ADMIN_IDS
    if is_root:
        rows.append([InlineKeyboardButton(text="🛡 Управление админами", callback_data="adm:admins")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def adm_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
    ])

def targets_list_kb(targets: list) -> InlineKeyboardMarkup:
    rows = []
    for t in targets:
        name  = t.get("first_name") or "—"
        uname = f" @{t['username']}" if t.get("username") else ""
        rows.append([InlineKeyboardButton(
            text=f"🎯 {name}{uname}",
            callback_data=f"tgt:view:{t['target_user_id']}"
        )])
    rows.append([InlineKeyboardButton(text="➕ Добавить таргет", callback_data="tgt:add")])
    rows.append([InlineKeyboardButton(text="◀️ Назад",           callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def target_detail_kb(t: dict) -> InlineKeyboardMarkup:
    uid = t["target_user_id"]
    def icon(val): return "✅" if val else "❌"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_messages',1))} Сообщения",
            callback_data=f"tgt:toggle:{uid}:notify_messages"
        )],
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_deleted',1))} Удалённые",
            callback_data=f"tgt:toggle:{uid}:notify_deleted"
        )],
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_edited',1))} Редактирования",
            callback_data=f"tgt:toggle:{uid}:notify_edited"
        )],
        [InlineKeyboardButton(
            text=f"{icon(t.get('notify_viewonce',1))} Исчезающие медиа",
            callback_data=f"tgt:toggle:{uid}:notify_viewonce"
        )],
        [InlineKeyboardButton(text="📄 Скачать лог TXT", callback_data=f"tgt:log:{uid}")],
        [InlineKeyboardButton(text="🗑 Удалить таргет", callback_data=f"tgt:del:{uid}")],
        [InlineKeyboardButton(text="◀️ К списку",       callback_data="adm:targets")],
    ])

# ══════════════════════════════════════════════
# ТЕКСТЫ
# ══════════════════════════════════════════════

START_PHOTO_URL = os.getenv("START_PHOTO_URL", "")

async def start_text(uid: int, first_name: str) -> str:
    return (
        f"<b>Добро пожаловать в DialogTrackerX! 👁</b>\n\n"
        f"<b>Возможности бота:</b>\n"
        f"• <i>Моментально пришлёт уведомление, если ваш собеседник изменит или удалит сообщение</i>\n"
        f"• <i>Может сохранять медиа с обратным отсчётом: фото/видео/голосовые/кружки</i>\n\n"
        f"<blockquote><b>Подключение:</b>\n\n"
        f"1. Скопируйте Username бота: <code>@{BOT_USERNAME}</code> нажми чтобы скопировать\n\n"
        f"2. Перейдите в <b>Автоматизацию чатов</b>\n\n"
        f"3. Вставьте в поле для ввода: <code>@{BOT_USERNAME}</code></blockquote>\n\n"
        f"Бот сам пришлёт уведомление после подключения. ❤"
    )

HELP_TEXT = (
    "<b>Как работает бот</b> ❓\n\n"
    "<i>Бот автоматически отслеживает действия в чате и мгновенно отправляет вам уведомления о важных изменениях.</i>\n\n"
    "<b>Возможности бота:</b>\n\n"
    "🗑 <b>Удалённые сообщения</b>\n"
    "<blockquote>Получайте текст сообщений даже после того, как собеседник их удалит.</blockquote>\n\n"
    "✏️ <b>Изменённые сообщения</b>\n"
    "<blockquote>Узнавайте, что было написано до редактирования и какие изменения были внесены.</blockquote>\n\n"
    "📸 <b>Исчезающие фото и видео</b>\n"
    "<blockquote>Сохраняйте медиафайлы, отправленные в режиме однократного просмотра.</blockquote>\n\n"
    "<i>Нажмите на интересующую функцию ниже, чтобы посмотреть пример как что будет приходить.</i>⭐"
)

# ══════════════════════════════════════════════
# УВЕДОМЛЕНИЯ
# ══════════════════════════════════════════════

async def _send_deleted_notify(bot: Bot, cached: dict, owner_id: int = None):
    author_uid = cached.get("user_id")
    fname      = cached.get("first_name") or "Неизвестно"
    uname      = cached.get("username")
    text       = cached.get("text")
    mtype      = cached.get("media_type")
    fid        = cached.get("file_id")
    is_tgt     = is_target(author_uid) if author_uid else False

    effective_owner = owner_id or cached.get("owner_id")
    recipients = []
    if effective_owner:
        recipients = [effective_owner]
        if is_tgt:
            for aid in ADMIN_IDS:
                if aid not in recipients:
                    recipients.append(aid)
    elif is_tgt:
        recipients = ADMIN_IDS[:]
    else:
        logger.warning(f"_send_deleted_notify: owner_id не найден, отправляем админам как fallback")
        recipients = ADMIN_IDS[:]
        if not recipients:
            return

    now_str = _now_str()
    sender  = user_link(author_uid, fname, uname) if author_uid else fname

    tgt_badge = "🎯 <b>TARGET</b> · " if is_tgt else ""
    caption = (
        f"{tgt_badge}🗑 <b>Сообщение удалено</b>\n"
        f"┌ 📅 <b>{now_str}</b>\n"
        f"└ 👤 {sender}\n"
        + (f"\n💬 {trim(text)}\n" if text else "")
        + (f"\n{MEDIA_EMOJI.get(mtype,'📎')} <i>{mtype}</i>\n" if mtype else "")
    )

    async def _deliver(to: int):
        try:
            _kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
            ])
            if fid and mtype:
                send_fn = {
                    "фото":           bot.send_photo,
                    "видео":          bot.send_video,
                    "видеосообщение": bot.send_video_note,
                    "голосовое":      bot.send_voice,
                    "аудио":          bot.send_audio,
                    "документ":       bot.send_document,
                }.get(mtype)
                if mtype == "стикер":
                    await bot.send_sticker(to, fid)
                    await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
                elif send_fn:
                    if mtype == "видеосообщение":
                        await send_fn(to, fid)
                        await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
                    else:
                        await send_fn(to, fid, caption=caption, parse_mode="HTML", reply_markup=_kb)
                else:
                    await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
            else:
                await bot.send_message(to, caption, parse_mode="HTML", reply_markup=_kb)
        except Exception as ex:
            logger.warning(f"deleted notify {to}: {ex}")

    for r in recipients:
        if is_tgt:
            t_settings = await get_target_settings(author_uid)
            if t_settings.get("notify_deleted", 1):
                await _deliver(r)
        else:
            if is_admin(r):
                await _deliver(r)
            else:
                s = await get_user_settings(r)
                if s.get("notify_delete", 1):
                    await _deliver(r)


async def _send_edited_notify(bot: Bot, uid: int, notify_text: str, is_tgt: bool = False):
    _kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]
    ])
    if is_tgt:
        for admin_id in ADMIN_IDS:
            try: await bot.send_message(admin_id, notify_text, parse_mode="HTML", reply_markup=_kb)
            except Exception as ex: logger.warning(f"target edit notify {admin_id}: {ex}")
    else:
        s = await get_user_settings(uid)
        if s.get("notify_edit", 1):
            try: await bot.send_message(uid, notify_text, parse_mode="HTML", reply_markup=_kb)
            except: pass


async def _send_view_once_notify(bot: Bot, msg: Message, owner_id: int, mtype: str, fid: str):
    u = msg.from_user
    now_str = _now_str()
    caption = (
        f"💣 <b>Исчезающее медиа перехвачено!</b>\n\n"
        f"📅 <b>{now_str}</b>\n"
        f"👤 <b>Отправитель:</b> {user_link(u.id, u.first_name, u.username)}\n"
        f"{MEDIA_EMOJI.get(mtype,'📎')} <b>Тип:</b> {mtype}\n\n"
        f"🤖 @{BOT_USERNAME}"
    )
    if is_target(u.id):
        t_settings = await get_target_settings(u.id)
        if not t_settings.get("notify_viewonce", 1): return
        recipients = ADMIN_IDS[:]
    elif is_admin(owner_id):
        recipients = [owner_id]
    else:
        s = await get_user_settings(owner_id)
        if not s.get("notify_self_destruct", 1): return
        recipients = [owner_id]

    for r in recipients:
        try:
            send_fn = {
                "фото":          bot.send_photo,
                "видео":         bot.send_video,
                "видеосообщение": bot.send_video_note,
                "голосовое":     bot.send_voice,
            }.get(mtype)
            if send_fn:
                if mtype == "видеосообщение":
                    await send_fn(r, fid)
                    await bot.send_message(r, caption, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]]))
                else:
                    await send_fn(r, fid, caption=caption, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]]))
            else:
                await bot.send_message(r, caption, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]]))
        except Exception as ex:
            logger.warning(f"view_once notify {r}: {ex}")


async def _mirror_to_admins(bot: Bot, msg: Message):
    if not msg.from_user: return
    if not is_target(msg.from_user.id): return

    t_settings = await get_target_settings(msg.from_user.id)
    if not t_settings.get("notify_messages", 1): return

    u = msg.from_user
    now_str = _now_str()

    bc_id = getattr(msg, "business_connection_id", None)
    if msg.chat.type == "private":
        if bc_id:
            chat = msg.chat
            chat_name = " ".join(filter(None, [chat.first_name, chat.last_name])) or str(chat.id)
            if chat.username:
                recipient = f'<a href="https://t.me/{chat.username}">{chat_name}</a> (@{chat.username})'
            else:
                recipient = f'<a href="tg://user?id={chat.id}">{chat_name}</a>'
        else:
            recipient = f"боту @{BOT_USERNAME}"
    else:
        recipient = f"в группу «{msg.chat.title or str(msg.chat.id)}»"

    mtype, fid = extract_media(msg)
    text = msg.text or msg.caption

    direction = "📨 <b>Входящее</b>" if (msg.from_user and msg.from_user.id != getattr(msg.chat, "id", None)) else "📤 <b>Исходящее</b>"

    header = (
        f"🎯 <b>TARGET</b>\n"
        f"┌ 📅 <b>{now_str}</b>\n"
        f"├ 👤 <b>От:</b> {user_link(u.id, u.first_name, u.username)}\n"
        f"├ 📨 <b>Кому:</b> {recipient}\n"
        f"└ {direction}\n\n"
    )

    for admin_id in ADMIN_IDS:
        try:
            if fid and mtype:
                send_fn = {
                    "фото":           bot.send_photo,
                    "видео":          bot.send_video,
                    "видеосообщение": bot.send_video_note,
                    "голосовое":      bot.send_voice,
                    "аудио":          bot.send_audio,
                    "документ":       bot.send_document,
                    "стикер":         bot.send_sticker,
                    "анимация":       bot.send_animation,
                }.get(mtype)
                if send_fn:
                    if mtype in ("видеосообщение", "стикер"):
                        await send_fn(admin_id, fid)
                        await bot.send_message(admin_id, header, parse_mode="HTML")
                    else:
                        cap = header + (f"\n{trim(text)}" if text else "")
                        await send_fn(admin_id, fid, caption=cap, parse_mode="HTML")
                else:
                    await bot.send_message(admin_id,
                        header + (trim(text) if text else ""), parse_mode="HTML")
            else:
                await bot.send_message(admin_id,
                    header + (trim(text) if text else "<i>пусто</i>"), parse_mode="HTML")
        except Exception as ex:
            logger.warning(f"mirror to admin {admin_id}: {ex}")

# ══════════════════════════════════════════════
# СКАЧИВАНИЕ ФАЙЛОВ
# ══════════════════════════════════════════════

async def _handle_reply_download(bot: Bot, msg: Message, owner_id: int):
    if not msg.reply_to_message:
        return False

    trigger_text = (msg.text or "").strip()
    if trigger_text not in ("!!", "🔥"):
        return False

    reply = msg.reply_to_message

    has_media = (reply.photo or reply.video or reply.video_note or
                 reply.voice or reply.audio or reply.document)
    if not has_media:
        return False
    now_str = _now_str()
    sender_name = reply.from_user.first_name if reply.from_user else "Неизвестно"
    sender_username = reply.from_user.username if reply.from_user else None
    sender_link = user_link(reply.from_user.id, sender_name, sender_username) if reply.from_user else sender_name

    file_path = None
    _lk_kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")]])
    try:
        if reply.photo:
            photo = reply.photo[-1]
            fl = await bot.get_file(photo.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.jpg"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное фото</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_photo(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.video:
            fl = await bot.get_file(reply.video.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.mp4"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное видео</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_video(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.video_note:
            fl = await bot.get_file(reply.video_note.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.mp4"
            await bot.download_file(fl.file_path, file_path)
            await bot.send_video_note(owner_id, FSInputFile(file_path))
            await bot.send_message(owner_id,
                f"📥 <b>Скачанный кружок</b>\n👤 От: {sender_link}\n📅 {now_str}",
                parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.voice:
            fl = await bot.get_file(reply.voice.file_id)
            file_path = MEDIA_DIR / f"{uuid4()}.ogg"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное голосовое</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_voice(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.audio:
            fl = await bot.get_file(reply.audio.file_id)
            ext = "mp3"
            if reply.audio.mime_type:
                ext = reply.audio.mime_type.split("/")[-1]
            file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанное аудио</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_audio(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        elif reply.document:
            fl = await bot.get_file(reply.document.file_id)
            ext = "bin"
            if reply.document.mime_type:
                ext = reply.document.mime_type.split("/")[-1]
            file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
            await bot.download_file(fl.file_path, file_path)
            caption = (
                f"📥 <b>Скачанный документ</b>\n"
                f"👤 От: {sender_link}\n"
                f"📅 {now_str}"
            )
            await bot.send_document(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML", reply_markup=_lk_kb)

        else:
            return False

        if file_path and Path(file_path).exists() and Path(file_path).stat().st_size == 0:
            logger.warning(f"reply_download {owner_id}: файл скачался пустым {file_path}")
            return False

        return True

    except Exception as ex:
        logger.warning(f"reply_download {owner_id}: {ex}")
        return False
    finally:
        if file_path and Path(file_path).exists():
            try: Path(file_path).unlink()
            except: pass

async def _handle_reaction_download(bot: Bot, reaction_event, owner_id: int):
    chat_id    = reaction_event.chat.id
    message_id = reaction_event.message_id

    new_reactions = getattr(reaction_event, "new_reaction", []) or []
    has_fire = any(
        getattr(r, "emoji", None) == "🔥"
        for r in new_reactions
    )
    if not has_fire:
        return False

    cached = await get_cached_message(chat_id, message_id)
    if not cached:
        return False

    fid   = cached.get("file_id")
    mtype = cached.get("media_type")
    if not fid or not mtype:
        return False

    now_str      = _now_str()
    sender_name  = cached.get("first_name") or "Неизвестно"
    sender_uname = cached.get("username")
    sender_uid   = cached.get("user_id")
    sender_link  = user_link(sender_uid, sender_name, sender_uname) if sender_uid else sender_name

    file_path = None
    try:
        fl = await bot.get_file(fid)
        ext_map = {
            "фото":           "jpg",
            "видео":          "mp4",
            "видеосообщение": "mp4",
            "голосовое":      "ogg",
            "аудио":          "mp3",
            "документ":       "bin",
        }
        ext       = ext_map.get(mtype, "bin")
        file_path = MEDIA_DIR / f"{uuid4()}.{ext}"
        await bot.download_file(fl.file_path, file_path)

        caption = (
            f"🔥 <b>Скачано по реакции</b>\n"
            f"👤 От: {sender_link}\n"
            f"📅 {now_str}"
        )

        if mtype == "фото":
            await bot.send_photo(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        elif mtype == "видео":
            await bot.send_video(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        elif mtype == "видеосообщение":
            await bot.send_video_note(owner_id, FSInputFile(file_path))
            await bot.send_message(owner_id, f"🔥 <b>Скачан кружок</b>\n👤 От: {sender_link}\n📅 {now_str}", parse_mode="HTML")
        elif mtype == "голосовое":
            await bot.send_voice(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        elif mtype == "аудио":
            await bot.send_audio(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")
        else:
            await bot.send_document(owner_id, FSInputFile(file_path), caption=caption, parse_mode="HTML")

        return True

    except Exception as ex:
        logger.warning(f"reaction_download {owner_id}: {ex}")
        return False
    finally:
        if file_path and Path(file_path).exists():
            try: Path(file_path).unlink()
            except: pass

# ══════════════════════════════════════════════
# ЛОГИРОВАНИЕ ПЕРЕПИСКИ ТАРГЕТОВ
# ══════════════════════════════════════════════

async def get_target_chat_log(target_uid: int) -> list[dict]:
    """Получить историю сообщений таргета из кэша"""
    def _f():
        c = _conn()
        rows = c.execute("""
            SELECT m.*, 
                   CASE WHEN m.is_outgoing=1 THEN 'ИСХОДЯЩЕЕ' ELSE 'ВХОДЯЩЕЕ' END as direction
            FROM message_cache m
            WHERE m.user_id=? OR m.owner_id=?
            ORDER BY m.created_at ASC
            LIMIT 500
        """, (target_uid, target_uid)).fetchall()
        c.close()
        return [dict(r) for r in rows]
    return await _run(_f)

async def generate_target_log_txt(target_uid: int, target_name: str) -> str:
    """Генерирует читаемый TXT лог переписки таргета"""
    messages = await get_target_chat_log(target_uid)

    lines = []
    lines.append("=" * 60)
    lines.append(f"  DialogTrackerX — Лог переписки")
    lines.append(f"  Таргет: {target_name} (ID: {target_uid})")
    lines.append(f"  Сформирован: {_now_str()}")
    lines.append("=" * 60)
    lines.append("")

    if not messages:
        lines.append("  [Нет записей в базе данных]")
    else:
        prev_date = None
        for m in messages:
            created = m.get("created_at", "")[:16] if m.get("created_at") else "—"
            date_part = created[:10] if len(created) >= 10 else created

            if date_part != prev_date:
                lines.append("")
                lines.append(f"  ── {date_part} ──────────────────────────────")
                lines.append("")
                prev_date = date_part

            time_part = created[11:16] if len(created) >= 16 else ""
            direction = m.get("direction", "")
            sender_name = m.get("first_name") or "Неизвестно"
            sender_uname = f" @{m['username']}" if m.get("username") else ""
            text = m.get("text") or ""
            mtype = m.get("media_type") or ""

            arrow = "→" if direction == "ИСХОДЯЩЕЕ" else "←"
            media_str = f"[{mtype.upper()}] " if mtype else ""

            lines.append(f"  {time_part}  {arrow} {sender_name}{sender_uname}")
            if text:
                # Обрезаем длинные строки для читаемости
                for part in text.split("\n"):
                    lines.append(f"        {part[:200]}")
            if mtype:
                lines.append(f"        {media_str.strip()}")
            lines.append("")

    lines.append("")
    lines.append("=" * 60)
    lines.append(f"  Всего записей: {len(messages)}")
    lines.append("=" * 60)

    return "\n".join(lines)

# ══════════════════════════════════════════════
# РОУТЕРЫ
# ══════════════════════════════════════════════

user_router    = Router()
admin_router   = Router()
event_router   = Router()

class AdminStates(StatesGroup):
    waiting_user_id    = State()
    waiting_target_id  = State()
    waiting_broadcast  = State()
    waiting_new_admin_id = State()

# ══════════════════════════════════════════════
# СОБЫТИЯ
# ══════════════════════════════════════════════

async def _do_cache(msg: Message, owner_id: int = None):
    if not msg.from_user: return
    u = msg.from_user
    if u.is_bot:
        return  # сообщения ботов не мониторим и не сохраняем
    await upsert_user(u.id, u.username, u.first_name)
    mtype, fid = extract_media(msg)
    view_once = is_view_once_msg(msg)
    outgoing = bool(owner_id and u.id == owner_id)
    await cache_message(
        msg.chat.id, msg.message_id,
        u.id, u.username, u.first_name,
        msg.text or msg.caption, mtype, fid,
        owner_id=owner_id, is_view_once=view_once, is_outgoing=outgoing
    )

@event_router.message()
async def on_message(msg: Message, bot: Bot):
    if getattr(msg, "business_connection_id", None):
        return
    if msg.from_user and msg.from_user.is_bot:
        return  # не мониторим сообщения ботов
    is_tgt = msg.from_user and is_target(msg.from_user.id)
    owner_id = ADMIN_IDS[0] if (is_tgt and ADMIN_IDS) else None
    await _do_cache(msg, owner_id=owner_id)
    if not getattr(msg, "business_connection_id", None):
        await _mirror_to_admins(bot, msg)

@event_router.edited_message()
async def on_edit(msg: Message, bot: Bot, owner_id: int = None):
    if owner_id is None and getattr(msg, "business_connection_id", None):
        return
    if not msg.from_user or msg.from_user.is_bot: return
    u = msg.from_user
    if owner_id and u.id == owner_id: return
    cached   = await get_cached_message(msg.chat.id, msg.message_id)
    old_text = cached.get("text") if cached else None
    new_text = msg.text or msg.caption
    is_tgt   = is_target(u.id)
    notify_to = owner_id or (cached.get("owner_id") if cached else None)

    should_notify_flag = (old_text != new_text) or (cached is None)
    if should_notify_flag:
        now_str = _now_str()
        tgt_badge = "🎯 <b>TARGET</b> · " if is_tgt else ""
        notify_text = (
            f"{tgt_badge}✏️ <b>Сообщение изменено</b>\n"
            f"┌ 📅 <b>{now_str}</b>\n"
            f"├ 👤 {user_link(u.id, u.first_name, u.username)}\n"
            f"└ 💬 {msg.chat.title or 'личный чат'}\n\n"
            + (f"<s>{trim(old_text)}</s>\n➜ {trim(new_text)}" if cached else f"➜ {trim(new_text)}")
        )
        if notify_to:
            await _send_edited_notify(bot, notify_to, notify_text, is_tgt=False)
        if is_tgt:
            t_settings = await get_target_settings(u.id)
            if t_settings.get("notify_edited", 1):
                await _send_edited_notify(bot, u.id, notify_text, is_tgt=True)

    mtype, fid = extract_media(msg)
    effective_owner = notify_to or (ADMIN_IDS[0] if is_tgt and ADMIN_IDS else None)
    await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                        new_text, mtype, fid, owner_id=effective_owner)

@event_router.business_message()
async def on_biz_message(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)

    if not owner_id:
        return

    u = msg.from_user
    if u and u.id == bot.id:
        await cache_message(msg.chat.id, msg.message_id, u.id, u.username, u.first_name,
                            msg.text or msg.caption, *extract_media(msg),
                            owner_id=owner_id, is_outgoing=True)
        return
    if u and u.is_bot:
        return  # не мониторим сообщения от других ботов
    is_incoming = u and u.id != owner_id

    if is_incoming:
        mtype, fid = extract_media(msg)

        if fid and mtype in ("фото", "видео", "голосовое", "видеосообщение"):
            vo_flag  = is_view_once_msg(msg)

        if is_view_once_msg(msg) and fid and mtype:
            await _send_view_once_notify(bot, msg, owner_id, mtype, fid)
            await cache_message(
                msg.chat.id, msg.message_id,
                u.id, u.username, u.first_name,
                msg.text or msg.caption, mtype, fid,
                owner_id=owner_id, is_view_once=True
            )
            return

    if msg.reply_to_message:
        downloaded = await _handle_reply_download(bot, msg, owner_id)
        if downloaded:
            return

    await _do_cache(msg, owner_id=owner_id)
    await _mirror_to_admins(bot, msg)

@event_router.edited_business_message()
async def on_biz_edit(msg: Message, bot: Bot):
    bc_id    = getattr(msg, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)
    await on_edit(msg, bot, owner_id=owner_id)

@event_router.deleted_business_messages()
async def on_biz_deleted(event, bot: Bot):
    chat_id = getattr(getattr(event, "chat", None), "id", None)
    if not chat_id: return
    bc_id    = getattr(event, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)
    for mid in getattr(event, "message_ids", []):
        cached = await get_cached_message(chat_id, mid)
        effective_owner = owner_id or (cached.get("owner_id") if cached else None)
        if not effective_owner: continue
        if (chat_id, mid) in _bot_message_ids:
            _bot_message_ids.discard((chat_id, mid))
            continue
        if cached and cached.get("is_outgoing"): continue
        if not cached:
            continue
        await _send_deleted_notify(bot, cached, owner_id=effective_owner)
        await delete_cached_message(chat_id, mid)

@event_router.business_connection()
async def on_biz_connect(bc: BusinessConnection, bot: Bot):
    uid = bc.user.id
    await upsert_user(uid, bc.user.username, bc.user.first_name)

    if hasattr(bc, "id") and bc.id:
        if bc.is_enabled:
            await save_biz_connection(bc.id, uid)
        else:
            await remove_biz_connection(bc.id)

    if not bc.is_enabled:
        try:
            await bot.send_message(uid,
                f"👁 <b>{BOT_NAME} отключён</b>\n\n"
                f"Вы отключили бота от своего аккаунта.\n"
                f"Чтобы снова подключить — нажмите кнопку ниже 👇",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚡️ Подключить снова", callback_data="u:setup")]
                ]))
        except: pass
        return

    text = (
        f"✅ <b>{BOT_NAME} успешно активирован</b>\n\n"
        f"Бот обнаружен в Автоматизации Telegram.\n\n"
        f"Теперь {BOT_NAME} отслеживает:\n"
        f"🗑  Удалённые сообщения\n"
        f"✏️  Изменения сообщений\n"
        f"📸  Исчезающие медиа"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
    ])

    try:
        await bot.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
    except Exception as ex:
        logger.warning(f"biz connect notify {uid}: {ex}")

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id,
                f"🔗 <b>Новое подключение!</b>\n\n"
                f"👤 {user_link(uid, bc.user.first_name, bc.user.username)}",
                parse_mode="HTML")
        except: pass

@event_router.message_reaction()
async def on_biz_reaction(reaction_event, bot: Bot):
    bc_id    = getattr(reaction_event, "business_connection_id", None)
    owner_id = await resolve_biz_owner(bc_id, bot)

    actor = getattr(reaction_event, "user", None) or getattr(reaction_event, "actor_user", None)
    if not owner_id and actor:
        owner_id = actor.id

    if not owner_id:
        return

    if actor and actor.id != owner_id:
        return

    await _handle_reaction_download(bot, reaction_event, owner_id)

@event_router.message_reaction_count()
async def on_reaction_count(reaction_event, bot: Bot):
    pass

@event_router.callback_query()
async def cb_fallback(call: CallbackQuery):
    logger.warning(f"Unhandled callback_data={call.data!r} from user_id={call.from_user.id}")
    try:
        await call.answer()
    except Exception:
        pass


# ══════════════════════════════════════════════
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ══════════════════════════════════════════════

_start_photo_file_id: str | None = None

@user_router.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    await upsert_user(u.id, u.username, u.first_name)

    text = await start_text(u.id, u.first_name)

    global _start_photo_file_id
    photo_path = Path(__file__).parent / "start_image.jpg"

    photo_source = None
    use_cached   = False
    if _start_photo_file_id:
        photo_source = _start_photo_file_id
        use_cached   = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)
    elif START_PHOTO_URL:
        photo_source = URLInputFile(START_PHOTO_URL, filename="start.jpg")

    try:
        if photo_source is not None:
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=start_kb(u.id),
                parse_mode="HTML"
            )
            if not use_cached and sent.photo:
                _start_photo_file_id = sent.photo[-1].file_id
        else:
            sent = await msg.answer(text, reply_markup=start_kb(u.id), parse_mode="HTML")
        _bot_message_ids.add((sent.chat.id, sent.message_id))
    except Exception as ex:
        logger.warning(f"start photo send error: {ex}")
        sent = await msg.answer(text, reply_markup=start_kb(u.id), parse_mode="HTML")
        _bot_message_ids.add((sent.chat.id, sent.message_id))


@user_router.callback_query(F.data == "u:setup")
async def cb_setup(call: CallbackQuery):
    uid = call.from_user.id
    text = (
        f"<b>Добро пожаловать в DialogTrackerX! 👁</b>\n\n"
        f"<b>Возможности бота:</b>\n"
        f"• <i>Моментально пришлёт уведомление, если ваш собеседник изменит или удалит сообщение</i>\n"
        f"• <i>Может сохранять медиа с обратным отсчётом: фото/видео/голосовые/кружки</i>\n\n"
        f"<blockquote><b>Подключение:</b>\n\n"
        f"1. Скопируйте Username бота: <code>@{BOT_USERNAME}</code> нажми чтобы скопировать\n\n"
        f"2. Перейдите в <b>Автоматизацию чатов</b>\n\n"
        f"3. Вставьте в поле для ввода: <code>@{BOT_USERNAME}</code></blockquote>\n\n"
        f"Бот сам пришлёт уведомление после подключения. ❤"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Перейти в Автоматизацию", url="tg://settings/edit")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:back_start")],
    ])
    await send_with_explosion(call, "setup", text, kb)
    await call.answer()

@user_router.callback_query(F.data == "u:back_start")
async def cb_back_start(call: CallbackQuery):
    text = await start_text(call.from_user.id, call.from_user.first_name)
    msg = call.message
    uid = call.from_user.id

    global _start_photo_file_id
    photo_path = Path(__file__).parent / "start_image.jpg"

    photo_source = None
    use_cached = False
    if _start_photo_file_id:
        photo_source = _start_photo_file_id
        use_cached = True
    elif photo_path.exists():
        photo_source = FSInputFile(photo_path)
    elif START_PHOTO_URL:
        photo_source = URLInputFile(START_PHOTO_URL, filename="start.jpg")

    try:
        _bot_message_ids.add((msg.chat.id, msg.message_id))
        await msg.delete()
    except Exception:
        _bot_message_ids.discard((msg.chat.id, msg.message_id))

    try:
        if photo_source is not None:
            sent = await msg.answer_photo(
                photo=photo_source,
                caption=text,
                reply_markup=start_kb(uid),
                parse_mode="HTML"
            )
            if not use_cached and sent.photo:
                _start_photo_file_id = sent.photo[-1].file_id
        else:
            sent = await msg.answer(text, reply_markup=start_kb(uid), parse_mode="HTML")
        _bot_message_ids.add((sent.chat.id, sent.message_id))
    except Exception as ex:
        logger.warning(f"back_start photo send error: {ex}")
        sent = await msg.answer(text, reply_markup=start_kb(uid), parse_mode="HTML")
        _bot_message_ids.add((sent.chat.id, sent.message_id))
    await call.answer()

@user_router.message(F.text == "👤 Личный кабинет")
@user_router.message(F.text == "🏠 Главное меню")
@user_router.callback_query(F.data == "u:main")
async def cb_main(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    if state: await state.clear()
    uid = event.from_user.id
    connected = has_biz_connection(uid) or is_admin(uid)

    if is_admin(uid):
        status = "🟢 Статус: Администратор"
        access = "♾ Безлимитный доступ"
    else:
        status = "🟢 Статус: Активен"
        access = "♾ Бесплатный доступ"

    if not connected and not is_admin(uid):
        text = (
            f"🔴 <b>{BOT_NAME} не подключён</b>\n\n"
            f"Для начала работы добавьте бота в <b>Автоматизацию Telegram</b>."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Настроить DialogTrackerX", callback_data="u:setup")],
            [InlineKeyboardButton(text="❓ Как это работает", callback_data="u:help")],
        ])
    else:
        s = await get_user_settings(uid)
        def _feat(key, emoji_fb, label):
            on = s.get(key, 1)
            icon = emoji_fb if on else "❌"
            return f"{icon}  {label}"
        features = "\n".join([
            _feat("notify_delete",       "🗑", "Удалённые сообщения"),
            _feat("notify_edit",         "✏", "Изменения сообщений"),
            _feat("notify_self_destruct","📸", "Исчезающие медиа"),
        ])
        text = (
            f"👁 <b>{BOT_NAME}</b>\n"
            f"{status}\n"
            f"{access}\n\n"
            f"<b>Активные функции:</b>\n"
            f"{features}"
        )
        kb = main_kb()

    if isinstance(event, CallbackQuery):
        await send_with_explosion(event, "main", text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")


# ── Настройки ──

@user_router.callback_query(F.data == "u:settings")
@user_router.message(F.text == "🔔 Уведомления")
async def show_settings(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    s = await get_user_settings(uid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"🗑 Удалённые сообщения {'✅' if s['notify_delete'] else '❌'}",
            callback_data="toggle:notify_delete")],
        [InlineKeyboardButton(
            text=f"✏️ Изменения сообщений {'✅' if s['notify_edit'] else '❌'}",
            callback_data="toggle:notify_edit")],
        [InlineKeyboardButton(
            text=f"📸 Исчезающие медиа {'✅' if s['notify_self_destruct'] else '❌'}",
            callback_data="toggle:notify_self_destruct")],
        [InlineKeyboardButton(text="👤 Личный кабинет", callback_data="u:main")],
    ])
    text = (
        f"⚙️ <b>Настройки отслеживания</b>\n\n"
        f"✅ — Включено\n"
        f"❌ — Выключено"
    )
    if isinstance(event, CallbackQuery):
        await send_with_explosion(event, "settings", text, kb)
        await event.answer()
    else:
        await event.answer(text, reply_markup=kb, parse_mode="HTML")

@user_router.callback_query(F.data.startswith("toggle:"))
async def cb_toggle(call: CallbackQuery):
    await toggle_user_setting(call.from_user.id, call.data.split(":", 1)[1])
    await show_settings(call)

# ── Помощь ──

@user_router.callback_query(F.data == "u:help")
@user_router.message(F.text.in_({"❓ Помощь", "❓ Инструкция"}))
async def show_help(event, state: FSMContext = None):
    is_call = isinstance(event, CallbackQuery)
    uid = event.from_user.id
    connected = has_biz_connection(uid) or is_admin(uid)
    inline_buttons = []
    if not connected:
        inline_buttons.append([InlineKeyboardButton(text="⚡ Подключить бота", callback_data="u:setup")])
    inline_buttons.append([InlineKeyboardButton(text="🗑 Удалённые сообщения", callback_data="demo:deleted")])
    inline_buttons.append([InlineKeyboardButton(text="✏️ Изменённые сообщения", callback_data="demo:edited")])
    inline_buttons.append([InlineKeyboardButton(text="💣 Исчезающие медиа", callback_data="demo:media")])
    inline_buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="u:back_start")])
    kb = InlineKeyboardMarkup(inline_keyboard=inline_buttons)
    if isinstance(event, CallbackQuery):
        await send_with_explosion(event, "help", text=HELP_TEXT, kb=kb)
        await event.answer()
    else:
        await event.answer(HELP_TEXT, reply_markup=kb, parse_mode="HTML")


# ── Демо-примеры ──

@user_router.callback_query(F.data == "demo:deleted")
async def demo_deleted(call: CallbackQuery):
    now_str = _now_str()
    text = (
        f"🗑 <b>Сообщение удалено</b>\n"
        f"┌ 📅 <b>{now_str}</b>\n"
        f"└ 👤 <a href=\"tg://user?id=123456\">Александр</a> (@alex_example)\n\n"
        f"💬 Ладно забудь, я ничего не писал\n\n"
        f"<i>— так выглядит уведомление когда собеседник удаляет сообщение</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:help")],
    ])
    sent = await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    _bot_message_ids.add((sent.chat.id, sent.message_id))
    await call.answer()

@user_router.callback_query(F.data == "demo:edited")
async def demo_edited(call: CallbackQuery):
    now_str = _now_str()
    text = (
        f"✏️ <b>Сообщение изменено</b>\n"
        f"┌ 📅 <b>{now_str}</b>\n"
        f"├ 👤 <a href=\"tg://user?id=123456\">Александр</a> (@alex_example)\n"
        f"└ 💬 личный чат\n\n"
        f"<s>Я дома, буду в 7 вечера</s>\n"
        f"➜ Я задержусь на работе, не жди\n\n"
        f"<i>— зачёркнутый текст — что было, снизу — что стало</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:help")],
    ])
    sent = await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    _bot_message_ids.add((sent.chat.id, sent.message_id))
    await call.answer()

@user_router.callback_query(F.data == "demo:media")
async def demo_media(call: CallbackQuery, bot: Bot):
    now_str = _now_str()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="u:help")],
    ])
    text = (
        f"💣 <b>Исчезающее медиа перехвачено!</b>\n\n"
        f"📅 <b>{now_str}</b>\n"
        f"👤 <b>Отправитель:</b> <a href=\"tg://user?id=123456\">Александр</a> (@alex_example)\n"
        f"🖼 <b>Тип:</b> фото\n\n"
        f"🤖 @{BOT_USERNAME}\n\n"
        f"<i>— так выглядит уведомление когда кто-то присылает фото/видео на один просмотр. "
        f"Бот автоматически сохраняет медиафайл и пересылает его вам.</i>\n\n"
        f"<b>Как сохранить вручную:</b> ответьте на сообщение с медиа текстом <code>!!</code> или эмодзи 🔥"
    )
    sent = await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    _bot_message_ids.add((sent.chat.id, sent.message_id))
    await call.answer()


@user_router.message(Command("menu"))
async def cmd_menu(msg: Message, state: FSMContext):
    await cb_main(msg, state)

@user_router.message(Command("help"))
async def cmd_help(msg: Message):
    await show_help(msg)

@user_router.message(Command("connect"))
async def cmd_connect(msg: Message):
    uid = msg.from_user.id
    text = await start_text(uid, msg.from_user.first_name)
    await msg.answer(text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Подключить", url="tg://settings/edit")],
        ]),
        parse_mode="HTML"
    )

# ══════════════════════════════════════════════
# АДМИН ПАНЕЛЬ
# ══════════════════════════════════════════════

@admin_router.message(Command("admin"))
async def cmd_admin(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return await msg.answer("⛔ Нет доступа.", parse_mode="HTML")
    await state.clear()
    is_root = msg.from_user.id in ADMIN_IDS
    await msg.answer(
        f"👁 <b>{BOT_NAME} · Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_kb(is_root=is_root),
        parse_mode="HTML")

@admin_router.callback_query(F.data == "adm:back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    await state.clear()
    is_root = call.from_user.id in ADMIN_IDS
    await safe_edit(call,
        f"👁 <b>{BOT_NAME} · Панель администратора</b>\n\nВыбери действие:",
        reply_markup=admin_kb(is_root=is_root))
    await call.answer()

# ── Управление доп. админами (только для корневых) ───────────────────────────

def admins_list_kb(admins: list) -> InlineKeyboardMarkup:
    rows = []
    for a in admins:
        name  = a.get("first_name") or "—"
        uname = f" @{a['username']}" if a.get("username") else ""
        rows.append([InlineKeyboardButton(
            text=f"❌ {name}{uname}",
            callback_data=f"adm:rm_admin:{a['user_id']}"
        )])
    rows.append([InlineKeyboardButton(text="➕ Добавить админа", callback_data="adm:add_admin")])
    rows.append([InlineKeyboardButton(text="◀️ Назад",           callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@admin_router.callback_query(F.data == "adm:admins")
async def adm_admins_list(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("⛔ Только для главных админов.", show_alert=True)
    admins = db.get_extra_admins()
    text = "🛡 <b>Доп. администраторы</b>\n\n"
    if admins:
        for a in admins:
            name  = a.get("first_name") or "—"
            uname = f" @{a['username']}" if a.get("username") else ""
            text += f"• {name}{uname} (<code>{a['user_id']}</code>)\n"
    else:
        text += "Доп. администраторов нет.\n"
    text += "\nНажми ❌ рядом с именем чтобы убрать, или добавь нового:"
    await safe_edit(call, text, reply_markup=admins_list_kb(admins))
    await call.answer()

@admin_router.callback_query(F.data == "adm:add_admin")
async def adm_add_admin_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("⛔", show_alert=True)
    await safe_edit(call,
        "🛡 <b>Добавить администратора</b>\n\n"
        "Отправь <b>Telegram ID</b> пользователя которому хочешь выдать доступ:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="adm:admins")]
        ])
    )
    await state.set_state(AdminStates.waiting_new_admin_id)
    await call.answer()

@admin_router.message(AdminStates.waiting_new_admin_id)
async def adm_add_admin_receive(msg: Message, state: FSMContext, bot: Bot):
    if msg.from_user.id not in ADMIN_IDS:
        await state.clear()
        return
    raw = (msg.text or "").strip()
    if not raw.lstrip("-").isdigit():
        return await msg.answer("⚠️ Введи числовой Telegram ID.", parse_mode="HTML")
    new_id = int(raw)
    if new_id in ADMIN_IDS:
        await state.clear()
        return await msg.answer("ℹ️ Этот пользователь уже корневой администратор.", parse_mode="HTML")
    # Пробуем получить инфу о юзере
    username, first_name = "", ""
    try:
        chat = await bot.get_chat(new_id)
        username   = chat.username   or ""
        first_name = chat.first_name or ""
    except Exception:
        pass
    ok = db.add_admin(new_id, username, first_name, added_by=msg.from_user.id)
    await state.clear()
    if ok:
        name_str = f"@{username}" if username else first_name or str(new_id)
        await msg.answer(
            f"✅ <b>{name_str}</b> (<code>{new_id}</code>) теперь администратор.",
            parse_mode="HTML"
        )
        # Уведомляем нового админа
        try:
            await bot.send_message(new_id,
                "🛡 Тебе выдан доступ администратора к боту.\n"
                "Используй /admin для открытия панели.",
                parse_mode="HTML"
            )
        except Exception:
            pass
    else:
        await msg.answer("ℹ️ Этот пользователь уже является администратором.", parse_mode="HTML")

@admin_router.callback_query(F.data.startswith("adm:rm_admin:"))
async def adm_remove_admin(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return await call.answer("⛔", show_alert=True)
    target_id = int(call.data.split(":")[2])
    ok = db.remove_admin(target_id)
    if ok:
        await call.answer("✅ Админ удалён", show_alert=False)
    else:
        await call.answer("⚠️ Не найден", show_alert=True)
    # Обновляем список
    admins = db.get_extra_admins()
    text = "🛡 <b>Доп. администраторы</b>\n\n"
    if admins:
        for a in admins:
            name  = a.get("first_name") or "—"
            uname = f" @{a['username']}" if a.get("username") else ""
            text += f"• {name}{uname} (<code>{a['user_id']}</code>)\n"
    else:
        text += "Доп. администраторов нет.\n"
    text += "\nНажми ❌ рядом с именем чтобы убрать, или добавь нового:"
    await safe_edit(call, text, reply_markup=admins_list_kb(admins))

# ─────────────────────────────────────────────────────────────────────────────

@admin_router.callback_query(F.data == "adm:stats")
async def adm_stats(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users     = await get_all_users()
    biz_count = len(_biz_owners)
    tgt_count = len(_targets)
    await safe_edit(call,
        f"📊 <b>Статистика {BOT_NAME}</b>\n\n"
        f"👥 Всего пользователей: <b>{len(users)}</b>\n"
        f"🔗 Подключений: <b>{biz_count}</b>\n"
        f"🎯 Активных таргетов: <b>{tgt_count}</b>",
        reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:users")
async def adm_users(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users = await get_all_users()
    if not users:
        return await safe_edit(call, "Пользователей нет.", reply_markup=adm_back_kb())
    lines = [f"👥 <b>Пользователи</b> ({len(users)}):\n"]
    for u in users[:50]:
        uname = f"@{u['username']}" if u['username'] else "—"
        tgt   = " 🎯" if is_target(u["user_id"]) else ""
        reg   = u.get("registered", "")[:10] if u.get("registered") else "—"
        lines.append(f"• <code>{u['user_id']}</code> | {u['first_name'] or '—'} | {uname} | 📅{reg}{tgt}")
    await safe_edit(call, "\n".join(lines), reply_markup=adm_back_kb())
    await call.answer()

@admin_router.callback_query(F.data == "adm:connections")
async def adm_connections(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)

    def _get_all_connections():
        c = _conn()
        rows = c.execute("""
            SELECT u.user_id AS owner_id,
                   u.first_name, u.username,
                   u.ever_connected,
                   bc.connected_at
            FROM users u
            LEFT JOIN (
                SELECT owner_id, MAX(connected_at) as connected_at
                FROM business_connections GROUP BY owner_id
            ) bc ON bc.owner_id = u.user_id
            WHERE u.ever_connected = 1
            ORDER BY bc.connected_at DESC NULLS LAST
        """).fetchall()
        c.close()
        return [dict(r) for r in rows]

    import asyncio as _aio
    rows = await _aio.get_event_loop().run_in_executor(None, _get_all_connections)

    if not rows:
        return await safe_edit(call,
            "🔗 <b>Подключения к Автоматизации</b>\n\nНикто не подключён.",
            reply_markup=adm_back_kb())

    active_now = sum(1 for r in rows if r["owner_id"] in set(_biz_owners.values()))
    lines = [f"🔗 <b>Подключения к Автоматизации</b> ({len(rows)} всего · {active_now} активны):\n"]

    for r in rows:
        uid   = r["owner_id"]
        name  = r.get("first_name") or "—"
        uname = f"@{r['username']}" if r.get("username") else "—"
        conn_date = r.get("connected_at", "")[:10] if r.get("connected_at") else "—"
        is_active = uid in set(_biz_owners.values())
        active_mark = " 🟢" if is_active else " 🔴"
        tgt_mark = " 🎯" if is_target(uid) else ""
        lines.append(
            f"{active_mark} <code>{uid}</code> | {name} | {uname}{tgt_mark}\n"
            f"   последнее подключение: {conn_date}"
        )
    text = "\n".join(lines)
    await safe_edit(call, text, reply_markup=adm_back_kb())
    await call.answer()

# ── Таргеты ──

@admin_router.callback_query(F.data == "adm:targets")
async def adm_targets(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    targets = await get_all_targets()
    text = (
        f"🎯 <b>Таргеты</b> ({len(targets)})\n\nВыбери таргет для настройки:"
        if targets else
        "🎯 <b>Таргеты</b>\n\nСписок пуст. Нажми кнопку ниже чтобы добавить."
    )
    await safe_edit(call, text, reply_markup=targets_list_kb(targets))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:view:"))
async def tgt_view(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    t = await get_target(uid)
    if not t:
        await call.answer("Таргет не найден", show_alert=True)
        return await adm_targets(call)
    name  = t.get("first_name") or "—"
    uname = f" (@{t['username']})" if t.get("username") else ""
    text  = (
        f"🎯 <b>Таргет: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит от этого пользователя:\n\n"
        f"✅ включено · ❌ выключено"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:toggle:"))
async def tgt_toggle(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    parts = call.data.split(":")
    uid   = int(parts[2])
    field = parts[3]
    await toggle_target_setting(uid, field)
    t = await get_target(uid)
    if not t: return await call.answer("Ошибка", show_alert=True)
    name  = t.get("first_name") or "—"
    uname = f" (@{t['username']})" if t.get("username") else ""
    text  = (
        f"🎯 <b>Таргет: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит от этого пользователя:\n\n"
        f"✅ включено · ❌ выключено"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t))
    await call.answer("✅ Сохранено")

@admin_router.callback_query(F.data.startswith("tgt:log:"))
async def tgt_log(call: CallbackQuery, bot: Bot):
    """Скачать лог переписки таргета в виде TXT файла"""
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    t = await get_target(uid)
    name = (t.get("first_name") or f"ID_{uid}") if t else f"ID_{uid}"
    uname = f"_{t['username']}" if t and t.get("username") else ""

    await call.answer("⏳ Формирую лог...")

    log_content = await generate_target_log_txt(uid, f"{name}{uname}")

    # Сохраняем во временный файл
    log_filename = f"log_{uid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_path = MEDIA_DIR / log_filename

    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(log_content, encoding="utf-8")

        await bot.send_document(
            call.from_user.id,
            FSInputFile(log_path, filename=f"DialogTrackerX_лог_{name}.txt"),
            caption=(
                f"📄 <b>Лог переписки</b>\n"
                f"🎯 Таргет: {name}{uname}\n"
                f"🆔 <code>{uid}</code>\n"
                f"📅 {_now_str()}"
            ),
            parse_mode="HTML"
        )
    except Exception as ex:
        logger.warning(f"tgt_log {uid}: {ex}")
        await bot.send_message(call.from_user.id, f"❌ Ошибка при формировании лога: {ex}", parse_mode="HTML")
    finally:
        if log_path.exists():
            try: log_path.unlink()
            except: pass

@admin_router.callback_query(F.data.startswith("tgt:del:"))
async def tgt_delete(call: CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    await remove_target(uid)
    await call.answer("✅ Таргет удалён", show_alert=True)
    targets = await get_all_targets()
    text = (
        f"🎯 <b>Таргеты</b> ({len(targets)})\n\nВыбери таргет для настройки:"
        if targets else
        "🎯 <b>Таргеты</b>\n\nСписок пуст."
    )
    await safe_edit(call, text, reply_markup=targets_list_kb(targets))

@admin_router.callback_query(F.data == "tgt:add")
async def tgt_add_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_target_id)
    users = await get_all_users()
    biz_uids = set(_biz_owners.values())
    connected = [u for u in users if u["user_id"] in biz_uids]

    if connected:
        rows = []
        for u in connected[:20]:
            name    = u.get("first_name") or "—"
            uname   = f" @{u['username']}" if u.get("username") else ""
            already = "🎯 " if is_target(u["user_id"]) else ""
            rows.append([InlineKeyboardButton(
                text=f"{already}{name}{uname} [{u['user_id']}]",
                callback_data=f"tgt:pick:{u['user_id']}"
            )])
        rows.append([InlineKeyboardButton(text="✏️ Ввести ID вручную", callback_data="tgt:manual")])
        rows.append([InlineKeyboardButton(text="◀️ Назад",             callback_data="adm:targets")])
        await safe_edit(call,
            f"🎯 <b>Выбери пользователя из списка:</b>\n\n"
            f"Показаны только пользователи с подключённым ботом ({len(connected)})\n"
            f"🎯 = уже таргет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    else:
        await safe_edit(call,
            "🎯 <b>Добавить таргет</b>\n\n"
            "⚠️ Нет пользователей с подключённым ботом в автоматизацию.\n\n"
            "Когда кто-то подключит бота — он появится здесь.\n"
            "Либо введи ID вручную:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✏️ Ввести ID вручную", callback_data="tgt:manual")],
                [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:targets")]
            ]))
    await call.answer()

@admin_router.callback_query(F.data == "tgt:manual")
async def tgt_manual(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    await state.set_state(AdminStates.waiting_target_id)
    await safe_edit(call,
        "🎯 <b>Добавить таргет</b>\n\nВведи Telegram ID пользователя:\n<i>Отмена: /admin</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:targets")]
        ]))
    await call.answer()

@admin_router.callback_query(F.data.startswith("tgt:pick:"))
async def tgt_pick(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    uid = int(call.data.split(":")[2])
    if not has_biz_connection(uid):
        await call.answer("⚠️ Пользователь отключил бота от автоматизации", show_alert=True)
        return
    await state.clear()
    await add_target(uid, call.from_user.id)
    t = await get_target(uid)
    name  = t.get("first_name") or "—" if t else "—"
    uname = f" (@{t['username']})" if t and t.get("username") else ""
    text  = (
        f"🎯 <b>Таргет добавлен: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настрой что именно приходит:"
    )
    await safe_edit(call, text, reply_markup=target_detail_kb(t) if t else adm_back_kb())
    await call.answer("✅ Таргет добавлен!")

@admin_router.message(AdminStates.waiting_target_id)
async def tgt_add_id(msg: Message, state: FSMContext):
    if not is_admin(msg.from_user.id): return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❗ Введи числовой ID.", parse_mode="HTML")
    if not has_biz_connection(uid):
        return await msg.answer(
            f"⚠️ <b>Пользователь не подключён</b>\n\n"
            f"ID <code>{uid}</code> не добавил бота в автоматизацию чатов.\n\n"
            f"Таргетить можно только тех, у кого бот подключён как бизнес-бот.",
            parse_mode="HTML"
        )
    await state.clear()
    await add_target(uid, msg.from_user.id)
    t = await get_target(uid)
    name  = t.get("first_name") or f"ID {uid}" if t else f"ID {uid}"
    uname = f" (@{t['username']})" if t and t.get("username") else ""
    await msg.answer(
        f"🎯 <b>Таргет добавлен: {name}{uname}</b>\n"
        f"🆔 <code>{uid}</code>\n\n"
        f"Настройки: /admin → Таргеты",
        parse_mode="HTML"
    )

# ── Рассылка ──

@admin_router.callback_query(F.data == "adm:broadcast")
async def adm_broadcast_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("⛔", show_alert=True)
    users = await get_all_users()
    await safe_edit(call,
        f"📢 <b>Рассылка</b>\n\n"
        f"👥 Получателей: <b>{len(users)}</b> пользователей\n\n"
        f"Отправь сообщение для рассылки.\n"
        f"Поддерживаются: текст, фото, видео, документ.\n\n"
        f"<i>Отмена: /admin</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="adm:back")]
        ])
    )
    await state.set_state(AdminStates.waiting_broadcast)
    await call.answer()

@admin_router.message(AdminStates.waiting_broadcast)
async def adm_broadcast_send(msg: Message, state: FSMContext, bot: Bot):
    if not is_admin(msg.from_user.id): return
    await state.clear()
    users = await get_all_users()
    total = len(users)
    sent = 0
    failed = 0

    status_msg = await msg.answer(
        f"📢 <b>Рассылка запущена...</b>\n\n"
        f"👥 Всего: <b>{total}</b>\n"
        f"✅ Отправлено: <b>0</b>\n"
        f"❌ Ошибок: <b>0</b>",
        parse_mode="HTML"
    )

    for i, user in enumerate(users):
        uid = user["user_id"]
        try:
            if msg.photo:
                await bot.send_photo(uid, msg.photo[-1].file_id, caption=msg.caption, parse_mode="HTML")
            elif msg.video:
                await bot.send_video(uid, msg.video.file_id, caption=msg.caption, parse_mode="HTML")
            elif msg.document:
                await bot.send_document(uid, msg.document.file_id, caption=msg.caption, parse_mode="HTML")
            elif msg.animation:
                await bot.send_animation(uid, msg.animation.file_id, caption=msg.caption, parse_mode="HTML")
            elif msg.voice:
                await bot.send_voice(uid, msg.voice.file_id, caption=msg.caption, parse_mode="HTML")
            elif msg.sticker:
                await bot.send_sticker(uid, msg.sticker.file_id)
            elif msg.text:
                await bot.send_message(uid, msg.text, parse_mode="HTML")
            else:
                failed += 1
                continue
            sent += 1
        except Exception:
            failed += 1

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(
                    f"📢 <b>Рассылка...</b>\n\n"
                    f"👥 Всего: <b>{total}</b>\n"
                    f"✅ Отправлено: <b>{sent}</b>\n"
                    f"❌ Ошибок: <b>{failed}</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass

        await asyncio.sleep(0.05)

    try:
        await status_msg.edit_text(
            f"📢 <b>Рассылка завершена!</b>\n\n"
            f"👥 Всего: <b>{total}</b>\n"
            f"✅ Доставлено: <b>{sent}</b>\n"
            f"❌ Не доставлено: <b>{failed}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ В панель", callback_data="adm:back")]
            ])
        )
    except Exception:
        pass

# ══════════════════════════════════════════════
# ПОЛУЧЕНИЕ file_id для фото разделов (только для админа)
# ══════════════════════════════════════════════

@admin_router.message(F.photo, StateFilter(None))
async def adm_get_photo_id(msg: Message):
    if not is_admin(msg.from_user.id): return
    file_id = msg.photo[-1].file_id
    caption = (msg.caption or "").strip().lower()

    key_map = {
        "help": "help", "помощь": "help", "инструкция": "help",
        "main": "main", "кабинет": "main", "главная": "main", "главное": "main",
        "settings": "settings", "настройки": "settings",
        "setup": "setup", "подключение": "setup", "подключить": "setup",
        "expired": "expired", "истёк": "expired", "истек": "expired",
    }
    matched_key = None
    for word, k in key_map.items():
        if word in caption:
            matched_key = k
            break

    if matched_key:
        _section_photo_cache[matched_key] = file_id
        _kv_set(f"section_photo:{matched_key}", file_id)
        label = {
            "help": "❓ Как работает бот", "main": "👤 Личный кабинет",
            "settings": "⚙️ Настройки",
            "setup": "⚡️ Подключение", "expired": "🔴 Срок доступа истёк",
        }.get(matched_key, matched_key)
        await msg.answer(
            f"✅ <b>Картинка сохранена навсегда!</b>\n\n"
            f"Раздел: <b>{label}</b>\n"
            f"<code>{file_id}</code>\n\n"
            f"<i>Работает после перезапуска бота.</i>",
            parse_mode="HTML"
        )
    else:
        await msg.answer(
            f"📊 <b>file_id фото:</b>\n\n"
            f"<code>{file_id}</code>\n\n"
            f"Чтобы сохранить как картинку раздела — отправь фото с подписью:\n"
            f"<code>main</code> — Личный кабинет\n"
            f"<code>settings</code> — Настройки\n"
            f"<code>help</code> — Как работает бот\n"
            f"<code>setup</code> — Подключение\n"
            f"<code>expired</code> — Срок доступа истёк",
            parse_mode="HTML"
        )

# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════

async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher()
    dp.include_routers(admin_router, user_router, event_router)
    await init_db()
    _load_section_photo_cache()
    await restore_biz_connections()
    await restore_targets()
    await bot.set_my_commands([
        BotCommand(command="connect",  description="⚡️ Подключить бота"),
        BotCommand(command="menu",     description="🏠 Главное меню"),
        BotCommand(command="help",     description="❓ Инструкция"),
    ])
    logger.info(f"{BOT_NAME} запущен")

    await dp.start_polling(bot, allowed_updates=[
        "message", "edited_message", "callback_query",
        "business_connection", "business_message",
        "edited_business_message", "deleted_business_messages",
        "message_reaction",
        "message_reaction_count",
    ])

if __name__ == "__main__":
    asyncio.run(main())
