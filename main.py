import asyncio
import csv
import io
import logging
import math
import os
import random
import re
import sqlite3
import time
from collections import defaultdict
from datetime import date

from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel, InputPeerChannel, InputPeerUser, User

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("adder")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
DATA_DIR = os.environ.get("DATA_DIR", "/data")
DAILY_LIMIT = int(os.environ.get("DAILY_LIMIT", "40"))  # حداکثر اد موفق در روز
MIN_DELAY = int(os.environ.get("MIN_DELAY", "40"))  # ثانیه بین هر تلاش
MAX_DELAY = int(os.environ.get("MAX_DELAY", "90"))
AUTO_HOURS = float(os.environ.get("AUTO_COLLECT_HOURS", "6"))
SENDERS_ENV = os.environ.get("SENDERS", "")  # مثلاً: mozhdeh,@other
ATTEMPTS_FACTOR = 3  # حداکثر تلاش در روز = DAILY_LIMIT × این عدد (شامل ناموفق‌ها)

os.makedirs(DATA_DIR, exist_ok=True)
db = sqlite3.connect(os.path.join(DATA_DIR, "data.db"), check_same_thread=False)
db.executescript(
    """
CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY, hash INTEGER, title TEXT, username TEXT);
CREATE TABLE IF NOT EXISTS sources(id INTEGER PRIMARY KEY, hash INTEGER, title TEXT);
CREATE TABLE IF NOT EXISTS senders(id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE IF NOT EXISTS keywords(word TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS raw_ids(token TEXT PRIMARY KEY, source TEXT, status TEXT DEFAULT 'pending', ts TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS candidates(user_id INTEGER PRIMARY KEY, hash INTEGER, username TEXT, name TEXT, source TEXT, priority INTEGER DEFAULT 0, ts TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS results(user_id INTEGER, channel_id INTEGER, status TEXT, ts TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(user_id, channel_id));
CREATE TABLE IF NOT EXISTS daily(day TEXT PRIMARY KEY, n INTEGER);
CREATE TABLE IF NOT EXISTS daily_attempts(day TEXT PRIMARY KEY, n INTEGER);
CREATE TABLE IF NOT EXISTS settings(k TEXT PRIMARY KEY, v TEXT);
"""
)
db.commit()

user = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
bot = TelegramClient(StringSession(), API_ID, API_HASH)
ME_ID = None
collect_lock = asyncio.Lock()

STATUS_FA = {
    "added": "اد شد",
    "already": "از قبل عضو بود",
    "privacy": "حریم خصوصی",
    "too_many_channels": "عضو کانال‌های زیاد",
    "banned_in_channel": "بن‌شده در کانال",
    "invalid": "نامعتبر",
}

# ---------------------------------------------------------------- helpers


def setting(k, default=None):
    r = db.execute("SELECT v FROM settings WHERE k=?", (k,)).fetchone()
    return r[0] if r else default


def set_setting(k, v):
    db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (k, str(v)))
    db.commit()


def _count(table):  # table همیشه ثابت داخلی است، نه ورودی کاربر
    r = db.execute(f"SELECT n FROM {table} WHERE day=?", (str(date.today()),)).fetchone()
    return r[0] if r else 0


def _bump(table):
    d = str(date.today())
    db.execute(f"INSERT OR IGNORE INTO {table} VALUES(?,0)", (d,))
    db.execute(f"UPDATE {table} SET n=n+1 WHERE day=?", (d,))
    db.commit()


def today_count():
    return _count("daily")


def today_attempts():
    return _count("daily_attempts")


def bump_today():
    _bump("daily")


def bump_attempts():
    _bump("daily_attempts")


USERNAME_RE = re.compile(r"(?:(?<![\w.])@|t\.me/|telegram\.me/)([A-Za-z][A-Za-z0-9_]{3,31})")
NUM_RE = re.compile(r"(?<![\w@/+.])(\d{6,12})(?![\w])")
RESERVED = {"joinchat", "addlist", "addstickers", "share", "proxy", "socks", "login", "boost"}


def parse_tokens(text):
    """@username، لینک t.me و آیدی عددی را از متن بیرون می‌کشد (بدون تکرار، به ترتیب)."""
    out = []
    for m in USERNAME_RE.finditer(text or ""):
        u = m.group(1).lower()
        if u not in RESERVED:
            out.append(u)
    out += NUM_RE.findall(text or "")
    return list(dict.fromkeys(out))


def enqueue(tokens, source):
    n = 0
    for t in tokens:
        cur = db.execute("INSERT OR IGNORE INTO raw_ids(token, source) VALUES(?,?)", (t.lower(), source))
        n += cur.rowcount
    db.commit()
    return n


def add_candidate(ent, source, priority=0):
    name = " ".join(x for x in [ent.first_name, ent.last_name] if x)
    cur = db.execute(
        "INSERT OR IGNORE INTO candidates(user_id, hash, username, name, source, priority) VALUES(?,?,?,?,?,?)",
        (ent.id, ent.access_hash, ent.username, name, source, priority),
    )
    if cur.rowcount == 0 and priority > 0:
        db.execute("UPDATE candidates SET priority=MAX(priority, ?) WHERE user_id=?", (priority, ent.id))
    db.commit()
    return cur.rowcount


def usable(u):
    return bool(isinstance(u, User) and not u.bot and not u.deleted and u.access_hash and u.id != ME_ID)


def next_job():
    # هر کاربر در هر کانال فقط یک‌بار: کلید یکتای (user_id, channel_id) در جدول results
    return db.execute(
        """SELECT c.user_id, c.hash, ch.id, ch.hash
           FROM candidates c CROSS JOIN channels ch
           LEFT JOIN results r ON r.user_id=c.user_id AND r.channel_id=ch.id
           WHERE r.user_id IS NULL
           ORDER BY c.priority DESC, c.rowid, ch.id LIMIT 1"""
    ).fetchone()


def record(uid, cid, status):
    db.execute("INSERT OR IGNORE INTO results(user_id, channel_id, status) VALUES(?,?,?)", (uid, cid, status))
    db.commit()


async def notify(text):
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        log.exception("notify failed")


async def resolve(text):
    text = text.strip()
    if text.lstrip("-").isdigit():
        text = int(text)
    return await user.get_entity(text)


# ------------------------------------------------- reading IDs from the PV


@user.on(events.NewMessage(incoming=True))
async def on_private(event):
    try:
        if not event.is_private:
            return
        if not db.execute("SELECT 1 FROM senders WHERE id=?", (event.sender_id,)).fetchone():
            return
        tokens = parse_tokens(event.raw_text)
        contact = getattr(event.message, "contact", None)
        if contact and getattr(contact, "user_id", None):
            tokens.append(str(contact.user_id))
        n = enqueue(tokens, f"pv:{event.sender_id}")
        if n:
            await notify(f"📥 {n} آیدی جدید از فرستنده {event.sender_id} ثبت شد.")
    except Exception:
        log.exception("on_private failed")


async def resolve_token(token):
    try:
        ent = await user.get_entity(int(token) if token.isdigit() else token)
    except errors.FloodWaitError:
        raise
    except (ValueError, errors.UsernameNotOccupiedError, errors.UsernameInvalidError, errors.PeerIdInvalidError):
        return "not_found", None
    except Exception:
        log.exception("resolve error")
        return "error", None
    if not usable(ent):
        return "not_user", None
    return "resolved", ent


async def resolve_one():
    """یک آیدی خام را به کاربر واقعی تبدیل می‌کند. خروجی: ثانیه‌ی انتظار تا کار بعدی."""
    row = db.execute("SELECT token, source FROM raw_ids WHERE status='pending' ORDER BY rowid LIMIT 1").fetchone()
    if not row:
        return 5
    token, source = row
    try:
        status, ent = await resolve_token(token)
    except errors.FloodWaitError as e:
        return e.seconds + 3  # آیدی در حالت pending می‌ماند و بعداً دوباره امتحان می‌شود
    if ent is not None:
        add_candidate(ent, source, 1 if source.startswith(("pv:", "manual")) else 0)
    db.execute("UPDATE raw_ids SET status=? WHERE token=?", (status, token))
    db.commit()
    return random.uniform(2, 5)


async def resolver():
    while True:
        try:
            delay = await resolve_one()
        except Exception:
            log.exception("resolver error")
            delay = 10
        await asyncio.sleep(delay)


# ------------------------------------------------------ auto collecting


async def collect_all():
    """جمع‌کردن کاربر از گروه‌های منبع، (اختیاری) همه‌ی گروه‌های عضو‌شده، و جستجوی کلیدواژه."""
    new = 0
    groups = {}
    for sid, shash in db.execute("SELECT id, hash FROM sources").fetchall():
        groups[sid] = InputPeerChannel(sid, shash)
    if setting("allgroups", "0") == "1":
        targets = {c for (c,) in db.execute("SELECT id FROM channels")}
        async for d in user.iter_dialogs():
            e = d.entity
            if isinstance(e, Channel) and e.megagroup and e.id not in targets:
                groups[e.id] = InputPeerChannel(e.id, e.access_hash)
    for gid, peer in groups.items():
        try:
            async for u in user.iter_participants(peer, limit=3000):
                if usable(u):
                    new += add_candidate(u, f"group:{gid}")
        except errors.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 300))
        except Exception:
            log.exception("collect group %s failed", gid)
        await asyncio.sleep(3)
    for (word,) in db.execute("SELECT word FROM keywords").fetchall():
        try:
            res = await user(SearchRequest(q=word, limit=50))
            for u in res.users:
                if usable(u):
                    new += add_candidate(u, f"search:{word}")
        except errors.FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 300))
        except Exception:
            log.exception("search %s failed", word)
        await asyncio.sleep(3)
    return new


async def run_collect():
    if collect_lock.locked():
        return None
    async with collect_lock:
        return await collect_all()


async def auto_loop():
    while True:
        await asyncio.sleep(60)
        try:
            if setting("auto", "1") != "1":
                continue
            if time.time() - float(setting("last_collect", "0")) < AUTO_HOURS * 3600:
                continue
            set_setting("last_collect", time.time())
            n = await run_collect()
            if n:
                await notify(f"🔎 جمع‌آوری خودکار: {n} کاربر جدید به صف اضافه شد.")
        except Exception:
            log.exception("auto loop error")


# ------------------------------------------------------------- adding


async def invite(uid, uhash, cid, chash):
    try:
        res = await user(InviteToChannelRequest(InputPeerChannel(cid, chash), [InputPeerUser(uid, uhash)]))
        if getattr(res, "missing_invitees", None):
            return "privacy"
        return "added"
    except errors.UserAlreadyParticipantError:
        return "already"
    except (errors.UserPrivacyRestrictedError, errors.UserNotMutualContactError):
        return "privacy"
    except errors.UserChannelsTooMuchError:
        return "too_many_channels"
    except errors.UserBannedInChannelError:
        return "banned_in_channel"
    except (errors.UserIdInvalidError, errors.InputUserDeactivatedError):
        return "invalid"


async def process_one():
    """یک کار از صف را انجام می‌دهد. خروجی: ثانیه‌ی انتظار تا کار بعدی."""
    if setting("running", "0") != "1":
        return 5
    if today_count() >= DAILY_LIMIT or today_attempts() >= DAILY_LIMIT * ATTEMPTS_FACTOR:
        return 600
    job = next_job()
    if not job:
        return 30
    uid, uhash, cid, chash = job
    try:
        status = await invite(uid, uhash, cid, chash)
    except errors.FloodWaitError as e:
        await notify(f"⏳ FloodWait: {e.seconds} ثانیه صبر می‌کنم.")
        return e.seconds + 5
    except errors.PeerFloodError:
        set_setting("running", "0")
        await notify("🛑 تلگرام اکانت رو محدود کرده (PeerFlood). کار متوقف شد؛ چند روز استراحت بده.")
        return 5
    except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError, errors.ChannelPrivateError):
        set_setting("running", "0")
        await notify(f"🛑 روی کانال {cid} دسترسی ادمین (افزودن عضو) نداره. متوقف شد.")
        return 5
    except errors.RPCError as e:
        # خطای ناشناخته‌ی تلگرام برای این جفت: ثبت می‌شود تا صف روی آن گیر نکند
        log.warning("rpc error for user %s channel %s: %r", uid, cid, e)
        status = f"error:{type(e).__name__}"
    except (ConnectionError, OSError, asyncio.TimeoutError):
        return 30  # مشکل شبکه: ثبت نمی‌کنیم، بعداً دوباره
    except Exception as e:
        log.exception("unexpected error for user %s channel %s", uid, cid)
        status = f"error:{type(e).__name__}"
    record(uid, cid, status)
    bump_attempts()
    if status == "added":
        bump_today()
    return random.randint(MIN_DELAY, MAX_DELAY)


async def worker():
    while True:
        try:
            delay = await process_one()
        except Exception:
            log.exception("worker error")
            delay = 30
        await asyncio.sleep(delay)


# ------------------------------------------------------- bot (manager)

HELP = """📋 دستورها

کانال‌ها:
/addchannel @کانال — افزودن (اکانت باید ادمین باشه)
/delchannel @کانال — حذف
/channels — لیست + تعداد اد شده

فرستنده‌ها (پیوی):
/addsender @مژده — آیدی‌هایی که این فرد تو پیوی می‌فرسته خودکار برداشته می‌شه
/delsender @نام    /senders

جمع‌آوری خودکار:
/addsource @گروه — گروهی که اکانت عضوشه    /delsource    /sources
/addkeyword کلمه — جستجوی کاربر با کلیدواژه    /delkeyword    /keywords
/allgroups on|off — استفاده از همه‌ی گروه‌های عضو‌شده
/auto on|off — جمع‌آوری خودکار دوره‌ای
/collect — الان جمع کن
/adduser @a @b ... — دستی (با اولویت)

آمار و بررسی:
/stats — آمار کلی
/users [صفحه] — لیست کاربران جمع‌شده
/ids [@کانال] — آیدی‌های اد شده
/allids — فایل CSV از همه‌ی آیدی‌های جمع‌شده از ابتدا

کنترل: /run  /pause"""


def arg_of(event):
    parts = event.raw_text.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def onoff(arg):
    return arg.strip().lower() in ("on", "1", "روشن", "yes")


async def reply_long(event, lines, empty="خالیه."):
    buf = ""
    for ln in lines:
        if len(buf) + len(ln) > 3500:
            await event.respond(buf)
            buf = ""
        buf += ln + "\n"
    await event.respond(buf or empty)


def stats_text():
    cands = db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
    raw = dict(db.execute("SELECT status, COUNT(*) FROM raw_ids GROUP BY status").fetchall())
    res = dict(db.execute("SELECT status, COUNT(*) FROM results GROUP BY status").fetchall())
    remaining = db.execute(
        """SELECT COUNT(*) FROM candidates c CROSS JOIN channels ch
           LEFT JOIN results r ON r.user_id=c.user_id AND r.channel_id=ch.id WHERE r.user_id IS NULL"""
    ).fetchone()[0]
    lines = [
        f"وضعیت: {'🟢 روشن' if setting('running', '0') == '1' else '🔴 خاموش'}",
        f"جمع‌آوری خودکار: {'روشن' if setting('auto', '1') == '1' else 'خاموش'}",
        f"امروز: {today_count()}/{DAILY_LIMIT} اد موفق | {today_attempts()}/{DAILY_LIMIT * ATTEMPTS_FACTOR} تلاش",
        "",
        f"کاربران جمع‌شده: {cands}",
        f"آیدی خام در انتظار تحلیل: {raw.get('pending', 0)}",
        f"آیدی پیدا‌نشده/نامعتبر: {raw.get('not_found', 0) + raw.get('not_user', 0) + raw.get('error', 0)}",
        f"تلاش‌های باقی‌مانده (کاربر×کانال): {remaining}",
        "",
        "نتایج:",
    ]
    for k, v in sorted(res.items(), key=lambda x: -x[1]):
        lines.append(f"  {STATUS_FA.get(k, k)}: {v}")
    lines.append("")
    lines.append("کانال‌ها:")
    for cid, title in db.execute("SELECT id, title FROM channels").fetchall():
        n = db.execute("SELECT COUNT(*) FROM results WHERE channel_id=? AND status='added'", (cid,)).fetchone()[0]
        lines.append(f"  {title}: {n} اد شده")
    return "\n".join(lines)


def build_csv():
    ch_title = dict(db.execute("SELECT id, title FROM channels").fetchall())
    per_user = defaultdict(list)
    for uid, cid, st in db.execute("SELECT user_id, channel_id, status FROM results").fetchall():
        per_user[uid].append(f"{ch_title.get(cid, cid)}:{st}")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id", "username", "name", "source", "collected_at", "results"])
    for uid, un, name, src, ts in db.execute("SELECT user_id, username, name, source, ts FROM candidates ORDER BY rowid"):
        w.writerow([uid, un or "", name or "", src, ts, "; ".join(per_user.get(uid, []))])
    for token, src, st, ts in db.execute("SELECT token, source, status, ts FROM raw_ids WHERE status!='resolved' ORDER BY rowid"):
        w.writerow(["", token, "", src, ts, f"raw:{st}"])
    data = io.BytesIO(("﻿" + buf.getvalue()).encode("utf-8"))
    data.name = "all_ids.csv"
    return data


@bot.on(events.NewMessage(from_users=ADMIN_ID, incoming=True, pattern=r"^/"))
async def on_cmd(event):
    cmd = event.raw_text.split()[0].split("@")[0].lower()
    arg = arg_of(event)
    try:
        if cmd in ("/start", "/help"):
            await event.respond(HELP)

        # ---- channels
        elif cmd == "/addchannel":
            ent = await resolve(arg)
            if not isinstance(ent, Channel):
                return await event.respond("این کانال/سوپرگروه نیست.")
            db.execute("INSERT OR REPLACE INTO channels VALUES(?,?,?,?)", (ent.id, ent.access_hash, ent.title, ent.username))
            db.commit()
            await event.respond(f"✅ کانال اضافه شد: {ent.title} ({ent.id})")
        elif cmd == "/delchannel":
            ent = await resolve(arg)
            db.execute("DELETE FROM channels WHERE id=?", (ent.id,))
            db.commit()
            await event.respond("🗑 کانال حذف شد. (سابقه‌ی افزودن‌ها نگه داشته می‌شه تا تکرار نشه)")
        elif cmd == "/channels":
            lines = []
            for cid, t, u in db.execute("SELECT id, title, username FROM channels").fetchall():
                n = db.execute("SELECT COUNT(*) FROM results WHERE channel_id=? AND status='added'", (cid,)).fetchone()[0]
                lines.append(f"{t} | @{u or '-'} | {cid} | اد شده: {n}")
            await reply_long(event, lines, "کانالی ثبت نشده.")

        # ---- senders (PV)
        elif cmd == "/addsender":
            ent = await resolve(arg)
            if not isinstance(ent, User):
                return await event.respond("این یک کاربر نیست.")
            name = " ".join(x for x in [ent.first_name, ent.last_name] if x)
            db.execute("INSERT OR REPLACE INTO senders VALUES(?,?)", (ent.id, name))
            db.commit()
            await event.respond(f"✅ فرستنده ثبت شد: {name} ({ent.id}). از این به بعد آیدی‌های پیوی‌اش برداشته می‌شه.")
        elif cmd == "/delsender":
            ent = await resolve(arg)
            db.execute("DELETE FROM senders WHERE id=?", (ent.id,))
            db.commit()
            await event.respond("🗑 فرستنده حذف شد.")
        elif cmd == "/senders":
            rows = db.execute("SELECT id, name FROM senders").fetchall()
            await reply_long(event, [f"{n} | {i}" for i, n in rows], "فرستنده‌ای ثبت نشده.")

        # ---- sources / keywords
        elif cmd == "/addsource":
            ent = await resolve(arg)
            if not isinstance(ent, Channel):
                return await event.respond("فقط سوپرگروه/کانال پشتیبانی می‌شه.")
            db.execute("INSERT OR REPLACE INTO sources VALUES(?,?,?)", (ent.id, ent.access_hash, ent.title))
            db.commit()
            await event.respond(f"✅ منبع اضافه شد: {ent.title}")
        elif cmd == "/delsource":
            ent = await resolve(arg)
            db.execute("DELETE FROM sources WHERE id=?", (ent.id,))
            db.commit()
            await event.respond("🗑 منبع حذف شد.")
        elif cmd == "/sources":
            rows = db.execute("SELECT id, title FROM sources").fetchall()
            await reply_long(event, [f"{t} | {i}" for i, t in rows], "منبعی ثبت نشده.")
        elif cmd == "/addkeyword":
            if not arg:
                return await event.respond("مثال: /addkeyword ali")
            db.execute("INSERT OR IGNORE INTO keywords VALUES(?)", (arg.lower(),))
            db.commit()
            await event.respond("✅ کلیدواژه اضافه شد.")
        elif cmd == "/delkeyword":
            db.execute("DELETE FROM keywords WHERE word=?", (arg.lower(),))
            db.commit()
            await event.respond("🗑 حذف شد.")
        elif cmd == "/keywords":
            await reply_long(event, [w for (w,) in db.execute("SELECT word FROM keywords")], "کلیدواژه‌ای نیست.")
        elif cmd == "/allgroups":
            set_setting("allgroups", "1" if onoff(arg) else "0")
            await event.respond("✅ انجام شد: " + ("همه‌ی گروه‌های عضو‌شده" if onoff(arg) else "فقط منابع ثبت‌شده"))
        elif cmd == "/auto":
            set_setting("auto", "1" if onoff(arg) else "0")
            await event.respond("✅ جمع‌آوری خودکار " + ("روشن شد." if onoff(arg) else "خاموش شد."))
        elif cmd == "/collect":
            await event.respond("در حال جمع‌آوری...")
            n = await run_collect()
            await event.respond("از قبل در حال اجراست." if n is None else f"✅ {n} کاربر جدید به صف اضافه شد.")
        elif cmd == "/adduser":
            tokens = parse_tokens(arg) or [t.strip().lstrip("@").lower() for t in arg.split() if t.strip()]
            n = enqueue(tokens, "manual")
            await event.respond(f"✅ {n} مورد جدید به صف تحلیل رفت (اولویت بالا).")

        # ---- stats / review
        elif cmd in ("/stats", "/status"):
            await event.respond(stats_text())
        elif cmd == "/users":
            page = max(1, int(arg)) if arg.isdigit() else 1
            total = db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]
            pages = max(1, math.ceil(total / 30))
            rows = db.execute(
                """SELECT c.user_id, c.username, c.source,
                          (SELECT COUNT(*) FROM results r WHERE r.user_id=c.user_id AND r.status='added')
                   FROM candidates c ORDER BY c.rowid DESC LIMIT 30 OFFSET ?""",
                ((page - 1) * 30,),
            ).fetchall()
            lines = [f"کاربران (صفحه {page}/{pages}) — مجموع {total}", ""]
            lines += [f"{('@' + u) if u else '-'} | {i} | {s} | اد در {n} کانال" for i, u, s, n in rows]
            await reply_long(event, lines)
        elif cmd == "/ids":
            q = "SELECT user_id, channel_id FROM results WHERE status='added'"
            params = ()
            if arg:
                ent = await resolve(arg)
                q += " AND channel_id=?"
                params = (ent.id,)
            rows = db.execute(q, params).fetchall()
            await reply_long(event, [f"{u} → {c}" for u, c in rows], "هنوز کسی اد نشده.")
        elif cmd == "/allids":
            await bot.send_file(event.chat_id, build_csv(), caption="همه‌ی آیدی‌های جمع‌شده از ابتدا")

        # ---- control
        elif cmd == "/run":
            set_setting("running", "1")
            await event.respond("▶️ شروع شد.")
        elif cmd == "/pause":
            set_setting("running", "0")
            await event.respond("⏸ متوقف شد.")
        else:
            await event.respond(HELP)
    except Exception as e:
        log.exception("cmd error")
        await event.respond(f"❌ خطا: {e}")


async def main():
    global ME_ID
    await user.connect()
    if not await user.is_user_authorized():
        raise SystemExit("SESSION_STRING نامعتبر یا منقضی است. دوباره با gen_session.py بساز.")
    ME_ID = (await user.get_me()).id
    for s in [x.strip().lstrip("@") for x in SENDERS_ENV.split(",") if x.strip()]:
        try:
            ent = await resolve(s)
            if isinstance(ent, User):
                db.execute(
                    "INSERT OR REPLACE INTO senders VALUES(?,?)",
                    (ent.id, " ".join(x for x in [ent.first_name, ent.last_name] if x)),
                )
                db.commit()
        except Exception:
            log.exception("sender %s from env failed", s)
    await bot.start(bot_token=BOT_TOKEN)
    await notify("✅ سرویس بالا آمد. /help")
    await asyncio.gather(
        worker(), resolver(), auto_loop(), bot.run_until_disconnected(), user.run_until_disconnected()
    )


if __name__ == "__main__":
    asyncio.run(main())
