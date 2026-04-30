import html
import logging
import random
import os
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

# Сколько человек в команде — нужно для голосования за спринт
TEAM_SIZE = 6

# Дата начала первого спринта (ГГГГ-ММ-ДД).
# От неё отсчитываются все двухнедельные события (ретро, голосование).
SPRINT_START_DATE = "2026-04-29"

# Время, в которое раз в 2 недели присылается голосование за спринт
# (всегда в пятницу второй недели спринта)
SPRINT_POLL_TIME = "12:00"

# Время голосования по тестовой среде (тот же день, через минуту)
ENV_POLL_TIME = "12:01"

# ID рабочего чата для авто-голосования за спринт.
# Можно оставить None и установить через /setchat в нужном чате.
TARGET_CHAT_ID = None

# ============================================================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)
scheduler = AsyncIOScheduler(timezone=TIMEZONE)

DAYS_MAP = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}

config = {
    "team_size": TEAM_SIZE,
    "sprint_start": SPRINT_START_DATE,
    "chat_id": TARGET_CHAT_ID,
}

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


# ============================================================
# /start
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 Привет! Я скрам-помощник команды.\n\n"
        "Что я умею:\n"
        "1) Напоминать о встречах (дейли, ретро и др.)\n"
        "2) Раз в 2 недели присылать голосование за спринт (0–10) и считать среднее\n\n"
        "📌 Сначала зайди в рабочий чат и выполни /setchat — тогда я начну "
        "присылать туда голосование за спринт автоматически.\n\n"
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
        "⚙️ Настройки:\n"
        "/settings — показать текущие настройки\n"
        "/setteamsize N — изменить размер команды\n"
        "/setsprintstart YYYY-MM-DD — изменить дату начала спринта\n\n"
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
    config["chat_id"] = update.effective_chat.id
    schedule_sprint_poll(context.application)
    schedule_env_poll(context.application)
    await update.message.reply_text(
        f"✅ Этот чат назначен рабочим (ID: {config['chat_id']}).\n"
        f"Голосование за спринт и тестовую среду будет приходить раз в 2 недели."
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

    first_run = next_biweekly_run(day, hour, minute)
    suffix = f"{day}_{hour:02d}{minute:02d}_{abs(hash(text)) % 100000}"
    job_id = make_job_id("biweekly", chat_id, suffix)
    scheduler.add_job(
        send_message,
        trigger=IntervalTrigger(weeks=2, start_date=first_run),
        args=[context.application, chat_id, f"⏰ {text}"],
        id=job_id,
        replace_existing=True,
    )
    await update.message.reply_text(
        f"✅ Раз в 2 недели ({day} {hour:02d}:{minute:02d}) добавлено.\n"
        f"Первый запуск: {first_run.strftime('%Y-%m-%d %H:%M')}\nID: {job_id}"
    )


def next_biweekly_run(day: str, hour: int, minute: int) -> datetime:
    """Ближайшая дата нужного дня недели начиная от sprint_start, не раньше now."""
    sprint_start = datetime.strptime(config["sprint_start"], "%Y-%m-%d")
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


def poll_text(poll_id: int) -> str:
    p = polls[poll_id]
    return (
        "📊 Оценка команды за спринт\n"
        "Поставь оценку от 0 до 10.\n\n"
        f"Проголосовало: {len(p['votes'])}/{config['team_size']}"
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
    await query.answer("Голос принят")

    try:
        await query.edit_message_text(
            text=poll_text(poll_id),
            reply_markup=build_poll_keyboard(poll_id),
        )
    except Exception as e:
        log.warning("edit_message_text failed: %s", e)

    if is_new and len(poll["votes"]) >= config["team_size"]:
        await close_poll(context.application, poll_id)


async def close_poll(app, poll_id: int):
    poll = polls.get(poll_id)
    if not poll or poll["closed"]:
        return
    poll["closed"] = True
    votes = poll["votes"]
    if not votes:
        text = "📊 Голосование закрыто. Никто не проголосовал."
    else:
        scores = [v[1] for v in votes.values()]
        avg = sum(scores) / len(scores)
        lines = [f"• {name}: {score}" for name, score in votes.values()]
        text = (
            "📊 Оценка команды за спринт — итог\n"
            f"Проголосовало: {len(votes)}/{config['team_size']}\n"
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
        await update.message.reply_text("Ты уже в списке фасилитаторов дейли (профиль обновлён).")
        return
    members.append({
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
    })
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
            await update.message.reply_text(
                f"{target.full_name} уже в списке фасилитаторов (профиль обновлён)."
            )
            return
    members.append({
        "user_id": target.id,
        "username": target.username,
        "full_name": target.full_name,
    })
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
    if is_sprint_last_friday():
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
            await query.edit_message_text(
                text="🤷 Все отказались. Договоритесь сами, кто проводит дейли."
            )
            await query.answer()
            return
        new_chosen = random.choice(candidates)
        pick["current"] = new_chosen["user_id"]
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
    await update.message.reply_text(
        "⚙️ Настройки:\n"
        f"• team_size: {config['team_size']}\n"
        f"• sprint_start: {config['sprint_start']}\n"
        f"• chat_id: {config['chat_id']}\n"
        f"• sprint_poll_time: {SPRINT_POLL_TIME}\n"
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
    config["team_size"] = n
    await update.message.reply_text(f"✅ team_size = {n}")


async def setsprintstart_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /setsprintstart YYYY-MM-DD")
        return
    try:
        datetime.strptime(context.args[0], "%Y-%m-%d")
    except ValueError:
        await update.message.reply_text("Неверный формат даты, нужно YYYY-MM-DD")
        return
    config["sprint_start"] = context.args[0]
    schedule_sprint_poll(context.application)
    schedule_env_poll(context.application)
    await update.message.reply_text(f"✅ sprint_start = {context.args[0]}")


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
    await update.message.reply_text(f"🗑 {sm['full_name']} снят с роли скрам-мастера.")


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
            await update.message.reply_text("Ты уже в списке тестировщиков (профиль обновлён).")
            return
    chat_testers.append({
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
    })
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
            await update.message.reply_text(
                f"{target.full_name} уже в списке тестировщиков (профиль обновлён)."
            )
            return
    chat_testers.append({
        "user_id": target.id,
        "username": target.username,
        "full_name": target.full_name,
    })
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
def is_sprint_last_friday() -> bool:
    """Сегодня — пятница второй недели текущего спринта (день 7..13, weekday=fri)."""
    sprint_start = datetime.strptime(config["sprint_start"], "%Y-%m-%d").date()
    today = now_tz().date()
    delta = (today - sprint_start).days
    if delta < 0:
        return False
    day_in_cycle = delta % 14
    return day_in_cycle in range(7, 14) and today.weekday() == DAYS_MAP["fri"]


def next_second_week_friday(time_str: str) -> datetime:
    """Ближайшая пятница второй недели спринта в указанное время МСК."""
    h, m = parse_time(time_str)
    sprint_start = datetime.strptime(config["sprint_start"], "%Y-%m-%d")
    second_week_start = sprint_start + timedelta(days=7)
    days_until_friday = (DAYS_MAP["fri"] - second_week_start.weekday()) % 7
    run = second_week_start + timedelta(days=days_until_friday)
    run = run.replace(hour=h, minute=m, tzinfo=ZoneInfo(TIMEZONE))
    while run < now_tz():
        run += timedelta(days=14)
    return run


def schedule_sprint_poll(app):
    chat_id = config["chat_id"]
    if not chat_id:
        return
    first_run = next_second_week_friday(SPRINT_POLL_TIME)
    job_id = f"sprint_poll__{chat_id}"
    scheduler.add_job(
        send_sprint_poll,
        trigger=IntervalTrigger(weeks=2, start_date=first_run),
        args=[app, chat_id],
        id=job_id,
        replace_existing=True,
    )
    log.info("Sprint poll scheduled, first run at %s", first_run)


def schedule_env_poll(app):
    chat_id = config["chat_id"]
    if not chat_id:
        return
    first_run = next_second_week_friday(ENV_POLL_TIME)
    job_id = f"env_poll__{chat_id}"
    scheduler.add_job(
        send_env_poll,
        trigger=IntervalTrigger(weeks=2, start_date=first_run),
        args=[app, chat_id],
        id=job_id,
        replace_existing=True,
    )
    log.info("Env poll scheduled, first run at %s", first_run)


# ============================================================
# Отправка
# ============================================================
async def send_message(app, chat_id, text):
    await app.bot.send_message(chat_id=chat_id, text=text)


# ============================================================
# Entry
# ============================================================
async def post_init(app):
    scheduler.start()
    if config["chat_id"]:
        schedule_sprint_poll(app)
        schedule_env_poll(app)
    schedule_daily_pick(app)
    log.info("Scheduler started")


def main():
    app = ApplicationBuilder().token(TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setchat", setchat))

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
