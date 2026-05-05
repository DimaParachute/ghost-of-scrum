import html
import json
import logging
import os
import random
import shutil
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# ============================================================
# НАСТРОЙКИ
# ============================================================
TOKEN = os.environ["TG_BOT_TOKEN"]
TIMEZONE = "Europe/Moscow"

# Дефолты для нового чата (могут быть переопределены /setteamsize и /setsprintstart).
DEFAULT_TEAM_SIZE = 6
DEFAULT_SPRINT_START_DATE = "2026-04-29"

# Время, в которое раз в 2 недели присылается голосование за спринт
# (всегда в пятницу второй недели спринта)
SPRINT_POLL_TIME = "12:00"

# Время голосования по тестовой среде (тот же день, через минуту)
ENV_POLL_TIME = "12:01"

# Файл с персистентным состоянием. Можно переопределить через STATE_PATH в env.
STATE_PATH = os.environ.get("STATE_PATH", "state.json")
STATE_VERSION = 1

# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

DAYS_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

# Настройки по чатам — каждый рабочий чат регистрируется через /setchat.
# {chat_id: {"team_size": int, "sprint_start": "YYYY-MM-DD"}}
chat_configs = {}


def get_chat_config(chat_id: int) -> dict:
    """Возвращает запись конфига чата (создаёт с дефолтами, если ещё нет)."""
    return chat_configs.setdefault(chat_id, {
        "team_size": DEFAULT_TEAM_SIZE,
        "sprint_start": DEFAULT_SPRINT_START_DATE,
    })

# Хранилище голосований за спринт.
# {poll_id: {"chat_id", "message_id", "votes": {user_id: (name, score)}, "closed"}}
polls = {}
poll_counter = 0

# Список фасилитаторов дейли по чатам.
# {chat_id: [{"user_id", "username", "full_name"}, ...]}
team_members = {}

# Активные сообщения с выбором фасилитатора.
# {(chat_id, message_id): {"current": user_id, "declined": set(user_id), "confirmed": bool}}
daily_picks = {}

# Тестировщики по чатам (можно несколько).
# {chat_id: [{"user_id", "username", "full_name"}, ...]}
testers = {}

# Активные голосования по тестовой среде.
# {(chat_id, message_id): {"votes": {user_id: (name, score)}, "closed": bool}}
env_polls = {}

# Скрам-мастер по чатам (один на чат).
# {chat_id: {"user_id", "username", "full_name"}}
scrum_masters = {}

# Дни рождения по чатам.
# {chat_id: {user_id: {"username", "full_name", "month", "day"}}}
birthdays = {}

# Пользовательские напоминания (/daily, /weekly, /biweekly).
# {job_id: {"type", "chat_id", "day"|None, "hour", "minute", "text"}}
user_reminders = {}


# ============================================================
# Персистентность (JSON)
# ============================================================
def _serialize_state() -> dict:
    """Собирает текущее in-memory состояние в сериализуемую структуру."""
    return {
        "_version": STATE_VERSION,
        "chat_configs": {str(cid): cfg for cid, cfg in chat_configs.items()},
        "team_members": {str(cid): members for cid, members in team_members.items()},
        "testers": {str(cid): tlist for cid, tlist in testers.items()},
        "scrum_masters": {str(cid): sm for cid, sm in scrum_masters.items()},
        "birthdays": {
            str(cid): {str(uid): b for uid, b in chat_b.items()}
            for cid, chat_b in birthdays.items()
        },
        "polls": [
            {
                "poll_id": pid,
                "chat_id": p["chat_id"],
                "message_id": p["message_id"],
                "votes": [[uid, name, score] for uid, (name, score) in p["votes"].items()],
                "closed": p["closed"],
            }
            for pid, p in polls.items()
        ],
        "poll_counter": poll_counter,
        "env_polls": [
            {
                "chat_id": cid,
                "message_id": mid,
                "votes": [[uid, name, score] for uid, (name, score) in p["votes"].items()],
                "closed": p["closed"],
            }
            for (cid, mid), p in env_polls.items()
        ],
        "daily_picks": [
            {
                "chat_id": cid,
                "message_id": mid,
                "current": p["current"],
                "declined": list(p["declined"]),
                "confirmed": p["confirmed"],
            }
            for (cid, mid), p in daily_picks.items()
        ],
        "user_reminders": list(user_reminders.values()),
    }


def save_state():
    """Атомарная запись state в JSON: сначала во временный файл, затем os.replace."""
    try:
        data = _serialize_state()
        directory = os.path.dirname(os.path.abspath(STATE_PATH)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=".state.", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, STATE_PATH)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
    except Exception as e:
        log.exception("save_state failed: %s", e)


# Миграции схемы. Ключ — текущая версия, значение — функция, повышающая на +1.
# Пока пусто; первая запись появится, когда понадобится breaking-change.
MIGRATIONS: dict = {}


def _migrate(data: dict) -> dict:
    v = data.get("_version", 1)
    if v > STATE_VERSION:
        raise RuntimeError(
            f"State version {v} новее, чем поддерживает код ({STATE_VERSION}). Обнови бота."
        )
    while v < STATE_VERSION:
        if v not in MIGRATIONS:
            raise RuntimeError(f"Нет миграции с версии {v} на {v + 1}.")
        backup_path = f"{STATE_PATH}.v{v}.bak.json"
        try:
            shutil.copy2(STATE_PATH, backup_path)
            log.info("State backup saved to %s", backup_path)
        except Exception as e:
            log.warning("Couldn't make backup before migration: %s", e)
        data = MIGRATIONS[v](data)
        v += 1
        data["_version"] = v
    return data


def load_state():
    """Читает state.json и заполняет глобальные структуры. Тихо игнорирует отсутствие файла."""
    global poll_counter
    if not os.path.exists(STATE_PATH):
        log.info("No state file found at %s — starting fresh", STATE_PATH)
        return
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.exception("Cannot read %s: %s", STATE_PATH, e)
        return

    data = _migrate(data)

    chat_configs.clear()
    for cid, cfg in data.get("chat_configs", {}).items():
        chat_configs[int(cid)] = {
            "team_size": cfg.get("team_size", DEFAULT_TEAM_SIZE),
            "sprint_start": cfg.get("sprint_start", DEFAULT_SPRINT_START_DATE),
        }

    team_members.clear()
    for cid, members in data.get("team_members", {}).items():
        team_members[int(cid)] = list(members)

    testers.clear()
    for cid, tlist in data.get("testers", {}).items():
        testers[int(cid)] = list(tlist)

    scrum_masters.clear()
    for cid, sm in data.get("scrum_masters", {}).items():
        scrum_masters[int(cid)] = sm

    birthdays.clear()
    for cid, chat_b in data.get("birthdays", {}).items():
        birthdays[int(cid)] = {int(uid): b for uid, b in chat_b.items()}

    polls.clear()
    for p in data.get("polls", []):
        polls[p["poll_id"]] = {
            "chat_id": p["chat_id"],
            "message_id": p["message_id"],
            "votes": {uid: (name, score) for uid, name, score in p.get("votes", [])},
            "closed": p.get("closed", False),
        }

    poll_counter = data.get("poll_counter", 0)

    env_polls.clear()
    for p in data.get("env_polls", []):
        env_polls[(p["chat_id"], p["message_id"])] = {
            "votes": {uid: (name, score) for uid, name, score in p.get("votes", [])},
            "closed": p.get("closed", False),
        }

    daily_picks.clear()
    for p in data.get("daily_picks", []):
        daily_picks[(p["chat_id"], p["message_id"])] = {
            "current": p["current"],
            "declined": set(p.get("declined", [])),
            "confirmed": p.get("confirmed", False),
        }

    user_reminders.clear()
    for r in data.get("user_reminders", []):
        user_reminders[r["job_id"]] = r

    log.info(
        "State loaded: %d chats, %d polls, %d env polls, %d daily picks, %d user reminders",
        len(chat_configs), len(polls), len(env_polls), len(daily_picks), len(user_reminders),
    )


def restore_user_reminders(app):
    """Воссоздаёт пользовательские напоминания (/daily, /weekly, /biweekly) из user_reminders."""
    for r in user_reminders.values():
        try:
            if r["type"] == "daily":
                trigger = CronTrigger(
                    day_of_week="mon-fri",
                    hour=r["hour"], minute=r["minute"], timezone=TIMEZONE,
                )
            elif r["type"] == "weekly":
                trigger = CronTrigger(
                    day_of_week=r["day"],
                    hour=r["hour"], minute=r["minute"], timezone=TIMEZONE,
                )
            elif r["type"] == "biweekly":
                first_run = next_biweekly_run(r["day"], r["hour"], r["minute"], r["chat_id"])
                trigger = IntervalTrigger(weeks=2, start_date=first_run)
            else:
                log.warning("Unknown reminder type: %s", r.get("type"))
                continue
            scheduler.add_job(
                send_message,
                trigger=trigger,
                args=[app, r["chat_id"], f"⏰ {r['text']}"],
                id=r["job_id"],
                replace_existing=True,
            )
        except Exception as e:
            log.warning("Failed to restore reminder %s: %s", r.get("job_id"), e)


def parse_time(s: str):
    h, m = s.split(":")
    return int(h), int(m)


def now_tz() -> datetime:
    return datetime.now(ZoneInfo(TIMEZONE))


async def is_chat_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """True, если команду вызвал админ или создатель чата (в личке — всегда True)."""
    chat = update.effective_chat
    user = update.effective_user
    if chat.type == "private":
        return True
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        return member.status in ("administrator", "creator")
    except Exception as e:
        log.warning("get_chat_member failed: %s", e)
        return False


def reply_target_user(update: Update):
    """Возвращает telegram.User, на чьё сообщение сделан reply, или None."""
    msg = update.message
    if not msg or not msg.reply_to_message:
        return None
    return msg.reply_to_message.from_user


def parse_birthday(s: str):
    """Принимает ДД.ММ / ДД-ММ / ДД/ММ. Возвращает (month, day)."""
    s = s.strip().replace("-", ".").replace("/", ".")
    parts = s.split(".")
    if len(parts) != 2:
        raise ValueError("expected DD.MM")
    day = int(parts[0])
    month = int(parts[1])
    if not (1 <= month <= 12):
        raise ValueError("month out of range")
    # Допускаем 29.02 (год не известен, в невисокосный поздравим 28.02).
    from calendar import monthrange
    max_day = 29 if month == 2 else monthrange(2024, month)[1]
    if not (1 <= day <= max_day):
        raise ValueError("day out of range")
    return month, day


# ============================================================
# /start
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 Привет! Я скрам-помощник команды.\n\n"
        "Что я умею:\n"
        "1) Напоминать о встречах (дейли, ретро и др.)\n"
        "2) Раз в 2 недели присылать голосование за спринт (0–10) и считать среднее\n\n"
        "📌 В каждом рабочем чате (если у тебя их несколько — у каждой команды свой) "
        "выполни /setchat — тогда я начну присылать туда голосование за спринт.\n"
        "Снять чат с роли рабочего: /unsetchat (для админов).\n"
        "Каждый чат имеет свои team_size и sprint_start.\n\n"
        "🛎 Команды напоминаний:\n"
        "/daily ЧЧ:ММ Текст — каждый будний день\n"
        "/weekly ДЕНЬ ЧЧ:ММ Текст — раз в неделю\n"
        "/biweekly ДЕНЬ ЧЧ:ММ Текст — раз в 2 недели (отсчёт от sprint_start)\n"
        "ДЕНЬ: mon, tue, wed, thu, fri, sat, sun\n"
        "/list — список напоминаний этого чата\n"
        "/remove ID — удалить напоминание по ID\n\n"
        "📊 Голосование за спринт (пт 12:00 МСК, раз в 2 недели):\n"
        "/startpoll — запустить голосование вручную прямо сейчас\n"
        "/closepoll — досрочно закрыть голосование и показать среднее\n\n"
        "🧪 Оценка тестовой среды (пт 12:01 МСК, раз в 2 недели, голосуют тестировщики, считается среднее):\n"
        "/registertester — добавить себя в список тестировщиков\n"
        "/unregistertester — убрать себя из списка\n"
        "/addtester (reply, для админов) — добавить того, на чьё сообщение отвечаешь\n"
        "/removetester (reply, для админов) — удалить того, на чьё сообщение отвечаешь\n"
        "/testers — показать список тестировщиков\n"
        "/startenvpoll — запустить голосование по тестовой среде вручную\n"
        "/closeenvpoll — досрочно закрыть голосование и показать среднее\n\n"
        "🎲 Случайный фасилитатор дейли (каждый будний день в 11:59 МСК):\n"
        "/joindaily — добавить себя в список фасилитаторов\n"
        "/leavedaily — убрать себя из списка\n"
        "/addfacilitator (reply, для админов) — добавить того, на чьё сообщение отвечаешь\n"
        "/removefacilitator (reply, для админов) — удалить того, на чьё сообщение отвечаешь\n"
        "/dailymembers — показать список\n"
        "/picknow — выбрать фасилитатора прямо сейчас (для теста)\n\n"
        "🧭 Скрам-мастер (один на команду, ведёт планирование в пн):\n"
        "В пятницу в конце спринта вместо случайного фасилитатора тегается скрам-мастер.\n"
        "/registerscrum — зарегистрироваться скрам-мастером\n"
        "/unregisterscrum — снять регистрацию\n"
        "/scrum — показать зарегистрированного скрам-мастера\n"
        "/setscrum (reply, для админов) — назначить того, на чьё сообщение отвечаешь\n"
        "/unsetscrum (для админов) — снять текущего скрам-мастера\n\n"
        "🎂 Дни рождения (поздравление в чат в 09:00 МСК):\n"
        "/setbirthday ДД.ММ — поставить себе (или в reply на сообщение — поставить другому, для админов)\n"
        "/removebirthday — удалить свой (или в reply — чужой, для админов)\n"
        "/birthdays — показать список\n"
        "/checkbirthdays — проверить и поздравить прямо сейчас (для теста)\n\n"
        "⚙️ Настройки (применяются к текущему чату):\n"
        "/settings — показать настройки этого чата\n"
        "/setteamsize N — размер команды\n"
        "/setsprintstart YYYY-MM-DD — дата начала спринта\n\n"
        "Примеры:\n"
        "  /daily 10:00 Дейли-стендап\n"
        "  /weekly fri 16:00 Демо\n"
        "  /biweekly thu 15:00 Ретро"
    )
    await update.message.reply_text(text)


# ============================================================
# /setchat
# ============================================================
async def setchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    is_new = chat_id not in chat_configs
    cfg = get_chat_config(chat_id)
    schedule_sprint_poll(context.application, chat_id)
    schedule_env_poll(context.application, chat_id)
    save_state()
    if is_new:
        await update.message.reply_text(
            f"✅ Этот чат добавлен в рабочие (ID: {chat_id}).\n"
            f"Голосование за спринт и тестовую среду будет приходить раз в 2 недели.\n"
            f"Текущие настройки: team_size={cfg['team_size']}, "
            f"sprint_start={cfg['sprint_start']}."
        )
    else:
        await update.message.reply_text(
            f"Чат уже рабочий (ID: {chat_id}). Расписание пересчитано."
        )


async def unsetchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    chat_id = update.effective_chat.id
    if chat_id not in chat_configs:
        await update.message.reply_text("Этот чат не был рабочим.")
        return
    del chat_configs[chat_id]
    for job_id in (f"sprint_poll__{chat_id}", f"env_poll__{chat_id}"):
        job = scheduler.get_job(job_id)
        if job:
            job.remove()
    save_state()
    await update.message.reply_text(
        f"🗑 Чат снят с роли рабочего (ID: {chat_id}). "
        f"Голосования за спринт и тестовую среду больше не будут приходить автоматически."
    )


# ============================================================
# Напоминания
# ============================================================
def make_job_id(prefix: str, chat_id: int, suffix: str) -> str:
    return f"{prefix}__{chat_id}__{suffix}"


async def daily_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /daily ЧЧ:ММ Текст")
        return
    try:
        hour, minute = parse_time(context.args[0])
    except Exception:
        await update.message.reply_text("Неверный формат времени, нужно ЧЧ:ММ")
        return
    text = " ".join(context.args[1:])
    chat_id = update.effective_chat.id
    suffix = f"{hour:02d}{minute:02d}_{abs(hash(text)) % 100000}"
    job_id = make_job_id("daily", chat_id, suffix)
    scheduler.add_job(
        send_message,
        trigger=CronTrigger(
            day_of_week="mon-fri", hour=hour, minute=minute, timezone=TIMEZONE
        ),
        args=[context.application, chat_id, f"⏰ {text}"],
        id=job_id,
        replace_existing=True,
    )
    user_reminders[job_id] = {
        "job_id": job_id, "type": "daily", "chat_id": chat_id,
        "day": None, "hour": hour, "minute": minute, "text": text,
    }
    save_state()
    await update.message.reply_text(
        f"✅ Дейли (пн–пт в {hour:02d}:{minute:02d}) добавлен.\nID: {job_id}"
    )


async def weekly_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 3:
        await update.message.reply_text(
            "Формат: /weekly ДЕНЬ ЧЧ:ММ Текст\nДЕНЬ: mon,tue,wed,thu,fri,sat,sun"
        )
        return
    day = context.args[0].lower()
    if day not in DAYS_MAP:
        await update.message.reply_text("Неверный день. Используй mon,tue,wed,thu,fri,sat,sun")
        return
    try:
        hour, minute = parse_time(context.args[1])
    except Exception:
        await update.message.reply_text("Неверный формат времени, нужно ЧЧ:ММ")
        return
    text = " ".join(context.args[2:])
    chat_id = update.effective_chat.id
    suffix = f"{day}_{hour:02d}{minute:02d}_{abs(hash(text)) % 100000}"
    job_id = make_job_id("weekly", chat_id, suffix)
    scheduler.add_job(
        send_message,
        trigger=CronTrigger(day_of_week=day, hour=hour, minute=minute, timezone=TIMEZONE),
        args=[context.application, chat_id, f"⏰ {text}"],
        id=job_id,
        replace_existing=True,
    )
    user_reminders[job_id] = {
        "job_id": job_id, "type": "weekly", "chat_id": chat_id,
        "day": day, "hour": hour, "minute": minute, "text": text,
    }
    save_state()
    await update.message.reply_text(
        f"✅ Еженедельно ({day} {hour:02d}:{minute:02d}) добавлено.\nID: {job_id}"
    )


async def biweekly_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 3:
        await update.message.reply_text(
            "Формат: /biweekly ДЕНЬ ЧЧ:ММ Текст\nДЕНЬ: mon,tue,wed,thu,fri,sat,sun"
        )
        return
    day = context.args[0].lower()
    if day not in DAYS_MAP:
        await update.message.reply_text("Неверный день. Используй mon,tue,wed,thu,fri,sat,sun")
        return
    try:
        hour, minute = parse_time(context.args[1])
    except Exception:
        await update.message.reply_text("Неверный формат времени, нужно ЧЧ:ММ")
        return
    text = " ".join(context.args[2:])
    chat_id = update.effective_chat.id

    first_run = next_biweekly_run(day, hour, minute, chat_id)
    suffix = f"{day}_{hour:02d}{minute:02d}_{abs(hash(text)) % 100000}"
    job_id = make_job_id("biweekly", chat_id, suffix)
    scheduler.add_job(
        send_message,
        trigger=IntervalTrigger(weeks=2, start_date=first_run),
        args=[context.application, chat_id, f"⏰ {text}"],
        id=job_id,
        replace_existing=True,
    )
    user_reminders[job_id] = {
        "job_id": job_id, "type": "biweekly", "chat_id": chat_id,
        "day": day, "hour": hour, "minute": minute, "text": text,
    }
    save_state()
    await update.message.reply_text(
        f"✅ Раз в 2 недели ({day} {hour:02d}:{minute:02d}) добавлено.\n"
        f"Первый запуск: {first_run.strftime('%Y-%m-%d %H:%M')}\nID: {job_id}"
    )


def next_biweekly_run(day: str, hour: int, minute: int, chat_id: int) -> datetime:
    """Ближайшая дата нужного дня недели начиная от sprint_start чата, не раньше now.
    Если у чата нет конфига — используется DEFAULT_SPRINT_START_DATE."""
    cfg = chat_configs.get(chat_id)
    sprint_start_str = cfg["sprint_start"] if cfg else DEFAULT_SPRINT_START_DATE
    sprint_start = datetime.strptime(sprint_start_str, "%Y-%m-%d")
    target_weekday = DAYS_MAP[day]
    delta = (target_weekday - sprint_start.weekday()) % 7
    run = sprint_start + timedelta(days=delta)
    run = run.replace(hour=hour, minute=minute, tzinfo=ZoneInfo(TIMEZONE))
    while run < now_tz():
        run += timedelta(days=14)
    return run


async def list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    marker = f"__{chat_id}__"
    jobs = [j for j in scheduler.get_jobs() if marker in j.id]
    if not jobs:
        await update.message.reply_text("В этом чате нет активных напоминаний.")
        return
    lines = ["📋 Напоминания этого чата:"]
    for j in jobs:
        text = j.args[2] if len(j.args) >= 3 else ""
        nxt = j.next_run_time.strftime("%Y-%m-%d %H:%M") if j.next_run_time else "—"
        lines.append(f"• {j.id}\n   {text}\n   следующий: {nxt}")
    await update.message.reply_text("\n".join(lines))


async def remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /remove ID (взять из /list)")
        return
    job_id = context.args[0]
    job = scheduler.get_job(job_id)
    if not job:
        await update.message.reply_text("Не нашёл напоминание с таким ID.")
        return
    job.remove()
    user_reminders.pop(job_id, None)
    save_state()
    await update.message.reply_text(f"🗑 Удалено: {job_id}")


# ============================================================
# Голосование за спринт
# ============================================================
def build_poll_keyboard(poll_id: int) -> InlineKeyboardMarkup:
    rows, row = [], []
    for i in range(0, 11):
        row.append(InlineKeyboardButton(str(i), callback_data=f"vote:{poll_id}:{i}"))
        if len(row) == 6:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def poll_team_size(poll: dict) -> int:
    cfg = chat_configs.get(poll["chat_id"])
    return cfg["team_size"] if cfg else DEFAULT_TEAM_SIZE


def poll_text(poll_id: int) -> str:
    p = polls[poll_id]
    return (
        "📊 Оценка команды за спринт\n"
        "Поставь оценку от 0 до 10.\n\n"
        f"Проголосовало: {len(p['votes'])}/{poll_team_size(p)}"
    )


async def send_sprint_poll(app, chat_id: int):
    global poll_counter
    poll_counter += 1
    poll_id = poll_counter
    polls[poll_id] = {
        "chat_id": chat_id,
        "message_id": None,
        "votes": {},
        "closed": False,
    }
    msg = await app.bot.send_message(
        chat_id=chat_id,
        text=poll_text(poll_id),
        reply_markup=build_poll_keyboard(poll_id),
    )
    polls[poll_id]["message_id"] = msg.message_id
    save_state()


async def vote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split(":")
    if len(parts) != 3 or parts[0] != "vote":
        await query.answer()
        return
    poll_id = int(parts[1])
    score = int(parts[2])
    poll = polls.get(poll_id)
    if not poll:
        await query.answer("Голосование не найдено", show_alert=True)
        return
    if poll["closed"]:
        await query.answer("Голосование уже закрыто", show_alert=True)
        return

    user = query.from_user
    is_new = user.id not in poll["votes"]
    poll["votes"][user.id] = (user.full_name, score)
    save_state()
    await query.answer("Голос принят")

    try:
        await query.edit_message_text(
            text=poll_text(poll_id),
            reply_markup=build_poll_keyboard(poll_id),
        )
    except Exception as e:
        log.warning("edit_message_text failed: %s", e)

    if is_new and len(poll["votes"]) >= poll_team_size(poll):
        await close_poll(context.application, poll_id)


async def close_poll(app, poll_id: int):
    poll = polls.get(poll_id)
    if not poll or poll["closed"]:
        return
    poll["closed"] = True
    save_state()
    votes = poll["votes"]
    if not votes:
        text = "📊 Голосование закрыто. Никто не проголосовал."
    else:
        scores = [v[1] for v in votes.values()]
        avg = sum(scores) / len(scores)
        lines = [f"• {name}: {score}" for name, score in votes.values()]
        text = (
            "📊 Оценка команды за спринт — итог\n"
            f"Проголосовало: {len(votes)}/{poll_team_size(poll)}\n"
            f"Средняя: {avg:.2f}\n\n" + "\n".join(lines)
        )
    try:
        await app.bot.edit_message_text(
            chat_id=poll["chat_id"], message_id=poll["message_id"], text=text
        )
    except Exception as e:
        log.warning("close_poll edit failed: %s", e)
        await app.bot.send_message(chat_id=poll["chat_id"], text=text)


async def startpoll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_sprint_poll(context.application, update.effective_chat.id)


async def closepoll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    open_polls = [
        pid for pid, p in polls.items() if p["chat_id"] == chat_id and not p["closed"]
    ]
    if not open_polls:
        await update.message.reply_text("В этом чате нет активных голосований.")
        return
    await close_poll(context.application, max(open_polls))


# ============================================================
# Случайный фасилитатор дейли
# ============================================================
def member_mention(m: dict) -> str:
    """HTML-упоминание члена команды."""
    if m.get("username"):
        return f"@{m['username']}"
    name = html.escape(m["full_name"])
    return f'<a href="tg://user?id={m["user_id"]}">{name}</a>'


def build_pick_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Смогу", callback_data="dpick:ok"),
        InlineKeyboardButton("❌ Не смогу", callback_data="dpick:no"),
    ]])


async def joindaily_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    members = team_members.setdefault(chat_id, [])
    if any(m["user_id"] == user.id for m in members):
        # Обновим имя/username на свежие на случай изменения
        for m in members:
            if m["user_id"] == user.id:
                m["username"] = user.username
                m["full_name"] = user.full_name
        save_state()
        await update.message.reply_text("Ты уже в списке фасилитаторов дейли (профиль обновлён).")
        return
    members.append({
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
    })
    save_state()
    await update.message.reply_text(
        f"✅ {user.full_name} добавлен в список фасилитаторов дейли. Сейчас в списке: {len(members)}."
    )


async def addfacilitator_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    target = reply_target_user(update)
    if not target:
        await update.message.reply_text(
            "Используй reply: ответь этой командой на сообщение того, кого хочешь добавить."
        )
        return
    if target.is_bot:
        await update.message.reply_text("Бота добавить в фасилитаторы нельзя.")
        return
    chat_id = update.effective_chat.id
    members = team_members.setdefault(chat_id, [])
    for m in members:
        if m["user_id"] == target.id:
            m["username"] = target.username
            m["full_name"] = target.full_name
            save_state()
            await update.message.reply_text(
                f"{target.full_name} уже в списке фасилитаторов (профиль обновлён)."
            )
            return
    members.append({
        "user_id": target.id,
        "username": target.username,
        "full_name": target.full_name,
    })
    save_state()
    await update.message.reply_text(
        f"✅ {target.full_name} добавлен в список фасилитаторов. Сейчас в списке: {len(members)}."
    )


async def removefacilitator_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    target = reply_target_user(update)
    if not target:
        await update.message.reply_text(
            "Используй reply: ответь этой командой на сообщение того, кого хочешь удалить."
        )
        return
    chat_id = update.effective_chat.id
    members = team_members.get(chat_id, [])
    new_members = [m for m in members if m["user_id"] != target.id]
    if len(new_members) == len(members):
        await update.message.reply_text(f"{target.full_name} не было в списке фасилитаторов.")
        return
    team_members[chat_id] = new_members
    save_state()
    await update.message.reply_text(
        f"🗑 {target.full_name} удалён из списка фасилитаторов."
    )


async def leavedaily_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    members = team_members.get(chat_id, [])
    new_members = [m for m in members if m["user_id"] != user.id]
    if len(new_members) == len(members):
        await update.message.reply_text("Тебя и не было в списке.")
        return
    team_members[chat_id] = new_members
    save_state()
    await update.message.reply_text("🗑 Удалил тебя из списка фасилитаторов дейли.")


async def dailymembers_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    members = team_members.get(chat_id, [])
    if not members:
        await update.message.reply_text(
            "Список фасилитаторов пуст. Каждый, кто проводит дейли, пусть напишет /joindaily в этом чате."
        )
        return
    lines = ["👥 Фасилитаторы дейли:"]
    for m in members:
        suffix = f" (@{m['username']})" if m.get("username") else ""
        lines.append(f"• {m['full_name']}{suffix}")
    await update.message.reply_text("\n".join(lines))


async def send_daily_pick(app, chat_id: int):
    # В пятницу в конце спринта — следующий дейли это планирование, ведёт скрам-мастер.
    if is_sprint_last_friday(chat_id):
        sm = scrum_masters.get(chat_id)
        if sm:
            await app.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🧭 Следующий дейли — планирование нового спринта.\n"
                    f"Ведёт скрам-мастер: {member_mention(sm)}"
                ),
                parse_mode="HTML",
            )
            return
        log.info("Sprint last friday in chat %s, but no scrum-master — falling back to random pick", chat_id)

    members = team_members.get(chat_id, [])
    if not members:
        log.info("Skipping daily pick for chat %s — no members", chat_id)
        return
    chosen = random.choice(members)
    text = (
        f"🎲 Следующий дейли проводит: {member_mention(chosen)}\n\n"
        f"Сможешь? Нажми кнопку ниже."
    )
    msg = await app.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=build_pick_keyboard(),
        parse_mode="HTML",
    )
    daily_picks[(chat_id, msg.message_id)] = {
        "current": chosen["user_id"],
        "declined": set(),
        "confirmed": False,
    }
    save_state()


async def daily_pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split(":")
    if len(parts) != 2 or parts[0] != "dpick":
        await query.answer()
        return
    action = parts[1]
    chat_id = query.message.chat_id
    key = (chat_id, query.message.message_id)
    pick = daily_picks.get(key)
    if not pick:
        await query.answer("Это сообщение больше не активно.", show_alert=True)
        return
    if pick["confirmed"]:
        await query.answer("Уже подтверждено.", show_alert=True)
        return
    if query.from_user.id != pick["current"]:
        await query.answer("Эту кнопку нажимает тот, кого выбрали.", show_alert=True)
        return

    members = team_members.get(chat_id, [])
    current_member = next((m for m in members if m["user_id"] == pick["current"]), None)

    if action == "ok":
        pick["confirmed"] = True
        save_state()
        mention = member_mention(current_member) if current_member else query.from_user.full_name
        await query.edit_message_text(
            text=f"✅ Дейли проводит: {mention}",
            parse_mode="HTML",
        )
        await query.answer("Принято")
        return

    if action == "no":
        pick["declined"].add(pick["current"])
        candidates = [m for m in members if m["user_id"] not in pick["declined"]]
        if not candidates:
            del daily_picks[key]
            save_state()
            await query.edit_message_text(
                text="🤷 Все отказались. Договоритесь сами, кто проводит дейли."
            )
            await query.answer()
            return
        new_chosen = random.choice(candidates)
        pick["current"] = new_chosen["user_id"]
        save_state()
        await query.edit_message_text(
            text=(
                f"🔄 {member_mention(current_member) if current_member else 'Предыдущий'} не сможет.\n"
                f"🎲 Замена: {member_mention(new_chosen)}\n\n"
                f"Сможешь? Нажми кнопку ниже."
            ),
            reply_markup=build_pick_keyboard(),
            parse_mode="HTML",
        )
        await query.answer("Выбрана замена")


async def picknow_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_daily_pick(context.application, update.effective_chat.id)


async def send_daily_pick_to_all(app):
    for chat_id in list(team_members.keys()):
        try:
            await send_daily_pick(app, chat_id)
        except Exception as e:
            log.warning("daily pick send failed for chat %s: %s", chat_id, e)


def schedule_daily_pick(app):
    scheduler.add_job(
        send_daily_pick_to_all,
        trigger=CronTrigger(day_of_week="mon-fri", hour=11, minute=59, timezone=TIMEZONE),
        args=[app],
        id="daily_pick",
        replace_existing=True,
    )
    log.info("Daily facilitator pick scheduled (mon-fri 11:59 %s)", TIMEZONE)


# ============================================================
# Настройки
# ============================================================
async def settings_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cfg = chat_configs.get(chat_id)
    if not cfg:
        await update.message.reply_text(
            "Этот чат ещё не рабочий. Сделай /setchat, чтобы зарегистрировать его."
        )
        return
    await update.message.reply_text(
        "⚙️ Настройки этого чата:\n"
        f"• chat_id: {chat_id}\n"
        f"• team_size: {cfg['team_size']}\n"
        f"• sprint_start: {cfg['sprint_start']}\n"
        f"• sprint_poll_time: {SPRINT_POLL_TIME}\n"
        f"• env_poll_time: {ENV_POLL_TIME}\n"
        f"• timezone: {TIMEZONE}"
    )


async def setteamsize_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /setteamsize N")
        return
    try:
        n = int(context.args[0])
        if n <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Нужно положительное число.")
        return
    chat_id = update.effective_chat.id
    if chat_id not in chat_configs:
        await update.message.reply_text("Сначала сделай /setchat в этом чате.")
        return
    chat_configs[chat_id]["team_size"] = n
    save_state()
    await update.message.reply_text(f"✅ team_size = {n} (для этого чата)")


async def setsprintstart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /setsprintstart YYYY-MM-DD")
        return
    try:
        datetime.strptime(context.args[0], "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("Неверный формат даты, нужно YYYY-MM-DD")
        return
    chat_id = update.effective_chat.id
    if chat_id not in chat_configs:
        await update.message.reply_text("Сначала сделай /setchat в этом чате.")
        return
    chat_configs[chat_id]["sprint_start"] = context.args[0]
    schedule_sprint_poll(context.application, chat_id)
    schedule_env_poll(context.application, chat_id)
    save_state()
    await update.message.reply_text(f"✅ sprint_start = {context.args[0]} (для этого чата)")


# ============================================================
# Скрам-мастер (один на чат, ведёт планирование в пн)
# ============================================================
async def registerscrum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    prev = scrum_masters.get(chat_id)
    scrum_masters[chat_id] = {
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
    }
    save_state()
    if prev and prev["user_id"] != user.id:
        await update.message.reply_text(
            f"✅ Скрам-мастер заменён: {prev['full_name']} → {user.full_name}."
        )
    else:
        await update.message.reply_text(
            f"✅ {user.full_name} зарегистрирован как скрам-мастер. "
            f"Будет тегаться в пятницу в конце спринта на следующее планирование."
        )


async def unregisterscrum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    sm = scrum_masters.get(chat_id)
    if not sm:
        await update.message.reply_text("В этом чате нет зарегистрированного скрам-мастера.")
        return
    if sm["user_id"] != user.id:
        await update.message.reply_text("Снять регистрацию может только сам скрам-мастер.")
        return
    del scrum_masters[chat_id]
    save_state()
    await update.message.reply_text("🗑 Скрам-мастер снят с регистрации.")


async def scrum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sm = scrum_masters.get(chat_id)
    if not sm:
        await update.message.reply_text(
            "Скрам-мастер не зарегистрирован. Пусть напишет /registerscrum, "
            "или админ назначит командой /setscrum в reply."
        )
        return
    suffix = f" (@{sm['username']})" if sm.get("username") else ""
    await update.message.reply_text(f"🧭 Скрам-мастер: {sm['full_name']}{suffix}")


async def setscrum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    target = reply_target_user(update)
    if not target:
        await update.message.reply_text(
            "Используй reply: ответь этой командой на сообщение того, кого хочешь назначить скрам-мастером."
        )
        return
    if target.is_bot:
        await update.message.reply_text("Бота скрам-мастером сделать нельзя.")
        return
    chat_id = update.effective_chat.id
    prev = scrum_masters.get(chat_id)
    scrum_masters[chat_id] = {
        "user_id": target.id,
        "username": target.username,
        "full_name": target.full_name,
    }
    save_state()
    if prev and prev["user_id"] != target.id:
        await update.message.reply_text(
            f"✅ Скрам-мастер заменён: {prev['full_name']} → {target.full_name}."
        )
    else:
        await update.message.reply_text(
            f"✅ {target.full_name} назначен скрам-мастером."
        )


async def unsetscrum_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    chat_id = update.effective_chat.id
    sm = scrum_masters.get(chat_id)
    if not sm:
        await update.message.reply_text("В этом чате нет зарегистрированного скрам-мастера.")
        return
    del scrum_masters[chat_id]
    save_state()
    await update.message.reply_text(f"🗑 {sm['full_name']} снят с роли скрам-мастера.")


# ============================================================
# Дни рождения
# ============================================================
async def setbirthday_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Формат: /setbirthday ДД.ММ\nПример: /setbirthday 15.07\n"
            "Чтобы поставить кому-то — ответь этой командой на его сообщение (только админы)."
        )
        return
    try:
        month, day = parse_birthday(context.args[0])
    except ValueError:
        await update.message.reply_text("Неверная дата. Нужно ДД.ММ, например 15.07.")
        return

    chat_id = update.effective_chat.id
    target = reply_target_user(update)

    if target:
        if not await is_chat_admin(update, context):
            await update.message.reply_text(
                "Назначать день рождения другим могут только админы чата."
            )
            return
        if target.is_bot:
            await update.message.reply_text("Боту день рождения не положен.")
            return
        user_id = target.id
        username = target.username
        full_name = target.full_name
    else:
        user = update.effective_user
        user_id = user.id
        username = user.username
        full_name = user.full_name

    birthdays.setdefault(chat_id, {})[user_id] = {
        "username": username,
        "full_name": full_name,
        "month": month,
        "day": day,
    }
    save_state()
    await update.message.reply_text(
        f"🎂 День рождения {full_name} сохранён: {day:02d}.{month:02d}."
    )


async def removebirthday_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    target = reply_target_user(update)

    if target:
        if not await is_chat_admin(update, context):
            await update.message.reply_text(
                "Удалять день рождения у других могут только админы чата."
            )
            return
        user_id = target.id
        full_name = target.full_name
    else:
        user_id = update.effective_user.id
        full_name = update.effective_user.full_name

    chat_bdays = birthdays.get(chat_id, {})
    if user_id not in chat_bdays:
        await update.message.reply_text(f"У {full_name} нет сохранённого дня рождения.")
        return
    del chat_bdays[user_id]
    save_state()
    await update.message.reply_text(f"🗑 День рождения {full_name} удалён.")


async def birthdays_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_bdays = birthdays.get(chat_id, {})
    if not chat_bdays:
        await update.message.reply_text(
            "Список дней рождения пуст. Добавить свой — /setbirthday ДД.ММ."
        )
        return
    items = sorted(chat_bdays.values(), key=lambda b: (b["month"], b["day"]))
    lines = ["🎂 Дни рождения:"]
    for b in items:
        suffix = f" (@{b['username']})" if b.get("username") else ""
        lines.append(f"• {b['day']:02d}.{b['month']:02d} — {b['full_name']}{suffix}")
    await update.message.reply_text("\n".join(lines))


def is_birthday_today(b: dict) -> bool:
    today = now_tz().date()
    if b["month"] == today.month and b["day"] == today.day:
        return True
    # 29.02 в невисокосный год — поздравляем 28.02
    if b["month"] == 2 and b["day"] == 29 and today.month == 2 and today.day == 28:
        from calendar import isleap
        if not isleap(today.year):
            return True
    return False


async def send_birthday_greetings_to_all(app):
    for chat_id, chat_bdays in list(birthdays.items()):
        for user_id, b in list(chat_bdays.items()):
            if not is_birthday_today(b):
                continue
            mention = member_mention({
                "user_id": user_id,
                "username": b.get("username"),
                "full_name": b["full_name"],
            })
            try:
                await app.bot.send_message(
                    chat_id=chat_id,
                    text=f"🎉 Сегодня день рождения у {mention}!\n🎂 С днём рождения! 🎈",
                    parse_mode="HTML",
                )
            except Exception as e:
                log.warning("birthday greeting failed in chat %s: %s", chat_id, e)


async def checkbirthdays_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_birthday_greetings_to_all(context.application)
    await update.message.reply_text(
        "Проверил. Если у кого-то сегодня день рождения — поздравил."
    )


def schedule_birthday_check(app):
    scheduler.add_job(
        send_birthday_greetings_to_all,
        trigger=CronTrigger(hour=9, minute=0, timezone=TIMEZONE),
        args=[app],
        id="birthday_check",
        replace_existing=True,
    )
    log.info("Daily birthday check scheduled at 09:00 %s", TIMEZONE)


# ============================================================
# Голосование по тестовой среде (тестировщики, 0–5, среднее)
# ============================================================
async def registertester_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    chat_testers = testers.setdefault(chat_id, [])
    for t in chat_testers:
        if t["user_id"] == user.id:
            t["username"] = user.username
            t["full_name"] = user.full_name
            save_state()
            await update.message.reply_text("Ты уже в списке тестировщиков (профиль обновлён).")
            return
    chat_testers.append({
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
    })
    save_state()
    await update.message.reply_text(
        f"✅ {user.full_name} добавлен в список тестировщиков. "
        f"Сейчас в списке: {len(chat_testers)}."
    )


async def addtester_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    target = reply_target_user(update)
    if not target:
        await update.message.reply_text(
            "Используй reply: ответь этой командой на сообщение того, кого хочешь добавить."
        )
        return
    if target.is_bot:
        await update.message.reply_text("Бота добавить в тестировщики нельзя.")
        return
    chat_id = update.effective_chat.id
    chat_testers = testers.setdefault(chat_id, [])
    for t in chat_testers:
        if t["user_id"] == target.id:
            t["username"] = target.username
            t["full_name"] = target.full_name
            save_state()
            await update.message.reply_text(
                f"{target.full_name} уже в списке тестировщиков (профиль обновлён)."
            )
            return
    chat_testers.append({
        "user_id": target.id,
        "username": target.username,
        "full_name": target.full_name,
    })
    save_state()
    await update.message.reply_text(
        f"✅ {target.full_name} добавлен в список тестировщиков. Сейчас в списке: {len(chat_testers)}."
    )


async def removetester_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_chat_admin(update, context):
        await update.message.reply_text("Эту команду могут использовать только админы чата.")
        return
    target = reply_target_user(update)
    if not target:
        await update.message.reply_text(
            "Используй reply: ответь этой командой на сообщение того, кого хочешь удалить."
        )
        return
    chat_id = update.effective_chat.id
    chat_testers = testers.get(chat_id, [])
    new_list = [t for t in chat_testers if t["user_id"] != target.id]
    if len(new_list) == len(chat_testers):
        await update.message.reply_text(f"{target.full_name} не было в списке тестировщиков.")
        return
    testers[chat_id] = new_list
    save_state()
    await update.message.reply_text(
        f"🗑 {target.full_name} удалён из списка тестировщиков."
    )


async def unregistertester_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user
    chat_testers = testers.get(chat_id, [])
    new_list = [t for t in chat_testers if t["user_id"] != user.id]
    if len(new_list) == len(chat_testers):
        await update.message.reply_text("Тебя и не было в списке тестировщиков.")
        return
    testers[chat_id] = new_list
    save_state()
    await update.message.reply_text("🗑 Удалил тебя из списка тестировщиков.")


async def tester_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_testers = testers.get(chat_id, [])
    if not chat_testers:
        await update.message.reply_text(
            "Тестировщики не зарегистрированы. Пусть напишут /registertester."
        )
        return
    lines = ["🧪 Тестировщики:"]
    for t in chat_testers:
        suffix = f" (@{t['username']})" if t.get("username") else ""
        lines.append(f"• {t['full_name']}{suffix}")
    await update.message.reply_text("\n".join(lines))


def build_env_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(i), callback_data=f"envvote:{i}") for i in range(0, 6)]
    ])


def env_poll_text(chat_id: int, votes: dict) -> str:
    chat_testers = testers.get(chat_id, [])
    mentions = " ".join(member_mention(t) for t in chat_testers)
    return (
        "🧪 Оценка тестовой среды за спринт\n"
        f"{mentions}, поставьте оценку от 0 до 5.\n\n"
        f"Проголосовало: {len(votes)}/{len(chat_testers)}"
    )


async def send_env_poll(app, chat_id: int):
    chat_testers = testers.get(chat_id, [])
    if not chat_testers:
        log.info("Skipping env poll for chat %s — no testers registered", chat_id)
        return
    msg = await app.bot.send_message(
        chat_id=chat_id,
        text=env_poll_text(chat_id, {}),
        reply_markup=build_env_keyboard(),
        parse_mode="HTML",
    )
    env_polls[(chat_id, msg.message_id)] = {"votes": {}, "closed": False}
    save_state()


async def env_vote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split(":")
    if len(parts) != 2 or parts[0] != "envvote":
        await query.answer()
        return
    try:
        score = int(parts[1])
    except ValueError:
        await query.answer()
        return
    chat_id = query.message.chat_id
    key = (chat_id, query.message.message_id)
    poll = env_polls.get(key)
    if not poll:
        await query.answer("Это голосование больше не активно.", show_alert=True)
        return
    if poll["closed"]:
        await query.answer("Голосование уже закрыто.", show_alert=True)
        return
    chat_testers = testers.get(chat_id, [])
    tester = next((t for t in chat_testers if t["user_id"] == query.from_user.id), None)
    if not tester:
        await query.answer("Голосуют только зарегистрированные тестировщики.", show_alert=True)
        return

    is_new = tester["user_id"] not in poll["votes"]
    poll["votes"][tester["user_id"]] = (tester["full_name"], score)
    save_state()
    await query.answer("Оценка принята")

    try:
        await query.edit_message_text(
            text=env_poll_text(chat_id, poll["votes"]),
            reply_markup=build_env_keyboard(),
            parse_mode="HTML",
        )
    except Exception as e:
        log.warning("env poll edit failed: %s", e)

    if is_new and len(poll["votes"]) >= len(chat_testers):
        await close_env_poll(context.application, key)


async def close_env_poll(app, key):
    poll = env_polls.get(key)
    if not poll or poll["closed"]:
        return
    poll["closed"] = True
    save_state()
    chat_id, message_id = key
    votes = poll["votes"]
    chat_testers = testers.get(chat_id, [])
    if not votes:
        text = "🧪 Оценка тестовой среды — голосование закрыто. Никто не оценил."
    else:
        scores = [s for _, s in votes.values()]
        avg = sum(scores) / len(scores)
        lines = [f"• {name}: {score}/5" for name, score in votes.values()]
        text = (
            "🧪 Оценка тестовой среды — итог\n"
            f"Проголосовало: {len(votes)}/{len(chat_testers)}\n"
            f"Средняя: {avg:.2f}/5\n\n" + "\n".join(lines)
        )
    try:
        await app.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
    except Exception as e:
        log.warning("close_env_poll edit failed: %s", e)
        await app.bot.send_message(chat_id=chat_id, text=text)


async def startenvpoll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_env_poll(context.application, update.effective_chat.id)


async def closeenvpoll_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    open_keys = [k for k, p in env_polls.items() if k[0] == chat_id and not p["closed"]]
    if not open_keys:
        await update.message.reply_text("В этом чате нет активных голосований по тестовой среде.")
        return
    # Берём самое позднее по message_id
    await close_env_poll(context.application, max(open_keys, key=lambda k: k[1]))


# ============================================================
# Расписание авто-голосования
# ============================================================
def is_sprint_last_friday(chat_id: int) -> bool:
    """Сегодня — пятница второй недели текущего спринта данного чата."""
    cfg = chat_configs.get(chat_id)
    if not cfg:
        return False
    sprint_start = datetime.strptime(cfg["sprint_start"], "%Y-%m-%d").date()
    today = now_tz().date()
    delta = (today - sprint_start).days
    if delta < 0:
        return False
    day_in_cycle = delta % 14
    return day_in_cycle in range(7, 14) and today.weekday() == DAYS_MAP["fri"]


def next_second_week_friday(time_str: str, chat_id: int) -> datetime:
    """Ближайшая пятница второй недели спринта (для конкретного чата) в указанное время МСК."""
    cfg = chat_configs[chat_id]
    h, m = parse_time(time_str)
    sprint_start = datetime.strptime(cfg["sprint_start"], "%Y-%m-%d")
    second_week_start = sprint_start + timedelta(days=7)
    days_until_friday = (DAYS_MAP["fri"] - second_week_start.weekday()) % 7
    run = second_week_start + timedelta(days=days_until_friday)
    run = run.replace(hour=h, minute=m, tzinfo=ZoneInfo(TIMEZONE))
    while run < now_tz():
        run += timedelta(days=14)
    return run


def schedule_sprint_poll(app, chat_id: int):
    if chat_id not in chat_configs:
        return
    first_run = next_second_week_friday(SPRINT_POLL_TIME, chat_id)
    job_id = f"sprint_poll__{chat_id}"
    scheduler.add_job(
        send_sprint_poll,
        trigger=IntervalTrigger(weeks=2, start_date=first_run),
        args=[app, chat_id],
        id=job_id,
        replace_existing=True,
    )
    log.info("Sprint poll scheduled for chat %s, first run at %s", chat_id, first_run)


def schedule_env_poll(app, chat_id: int):
    if chat_id not in chat_configs:
        return
    first_run = next_second_week_friday(ENV_POLL_TIME, chat_id)
    job_id = f"env_poll__{chat_id}"
    scheduler.add_job(
        send_env_poll,
        trigger=IntervalTrigger(weeks=2, start_date=first_run),
        args=[app, chat_id],
        id=job_id,
        replace_existing=True,
    )
    log.info("Env poll scheduled for chat %s, first run at %s", chat_id, first_run)


# ============================================================
# Отправка
# ============================================================
async def send_message(app, chat_id, text):
    await app.bot.send_message(chat_id=chat_id, text=text)


# ============================================================
# Entry
# ============================================================
async def post_init(app):
    load_state()
    scheduler.start()
    for chat_id in list(chat_configs.keys()):
        schedule_sprint_poll(app, chat_id)
        schedule_env_poll(app, chat_id)
    schedule_daily_pick(app)
    schedule_birthday_check(app)
    restore_user_reminders(app)
    log.info("Scheduler started")


def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setchat", setchat))
    app.add_handler(CommandHandler("unsetchat", unsetchat))

    app.add_handler(CommandHandler("daily", daily_cmd))
    app.add_handler(CommandHandler("weekly", weekly_cmd))
    app.add_handler(CommandHandler("biweekly", biweekly_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("remove", remove_cmd))

    app.add_handler(CommandHandler("startpoll", startpoll_cmd))
    app.add_handler(CommandHandler("closepoll", closepoll_cmd))

    app.add_handler(CommandHandler("joindaily", joindaily_cmd))
    app.add_handler(CommandHandler("leavedaily", leavedaily_cmd))
    app.add_handler(CommandHandler("dailymembers", dailymembers_cmd))
    app.add_handler(CommandHandler("picknow", picknow_cmd))
    app.add_handler(CommandHandler("addfacilitator", addfacilitator_cmd))
    app.add_handler(CommandHandler("removefacilitator", removefacilitator_cmd))

    app.add_handler(CommandHandler("registertester", registertester_cmd))
    app.add_handler(CommandHandler("unregistertester", unregistertester_cmd))
    app.add_handler(CommandHandler("tester", tester_cmd))
    app.add_handler(CommandHandler("testers", tester_cmd))
    app.add_handler(CommandHandler("startenvpoll", startenvpoll_cmd))
    app.add_handler(CommandHandler("closeenvpoll", closeenvpoll_cmd))
    app.add_handler(CommandHandler("addtester", addtester_cmd))
    app.add_handler(CommandHandler("removetester", removetester_cmd))

    app.add_handler(CommandHandler("registerscrum", registerscrum_cmd))
    app.add_handler(CommandHandler("unregisterscrum", unregisterscrum_cmd))
    app.add_handler(CommandHandler("scrum", scrum_cmd))
    app.add_handler(CommandHandler("scrummaster", scrum_cmd))
    app.add_handler(CommandHandler("setscrum", setscrum_cmd))
    app.add_handler(CommandHandler("unsetscrum", unsetscrum_cmd))

    app.add_handler(CommandHandler("setbirthday", setbirthday_cmd))
    app.add_handler(CommandHandler("removebirthday", removebirthday_cmd))
    app.add_handler(CommandHandler("birthdays", birthdays_cmd))
    app.add_handler(CommandHandler("checkbirthdays", checkbirthdays_cmd))

    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("setteamsize", setteamsize_cmd))
    app.add_handler(CommandHandler("setsprintstart", setsprintstart_cmd))

    app.add_handler(CallbackQueryHandler(vote_callback, pattern=r"^vote:"))
    app.add_handler(CallbackQueryHandler(daily_pick_callback, pattern=r"^dpick:"))
    app.add_handler(CallbackQueryHandler(env_vote_callback, pattern=r"^envvote:"))

    print("Бот запущен...")
    app.run_polling()


if __name__ == "__main__":
    main()
