import html
from datetime import datetime
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes


# {chat_id: [{"id", "user_id", "username", "full_name", "start", "end"}, ...]}
vacations = {}

_deps = {}


def configure(
    *,
    save_state,
    is_chat_admin,
    reply_target_user,
    now_tz,
    member_mention,
    team_members,
    testers,
    scrum_masters,
    birthdays,
):
    _deps.update({
        "save_state": save_state,
        "is_chat_admin": is_chat_admin,
        "reply_target_user": reply_target_user,
        "now_tz": now_tz,
        "member_mention": member_mention,
        "team_members": team_members,
        "testers": testers,
        "scrum_masters": scrum_masters,
        "birthdays": birthdays,
    })


def serialize_vacations() -> dict:
    return {str(cid): vlist for cid, vlist in vacations.items()}


def load_vacations(data: dict):
    vacations.clear()
    for cid, vlist in data.items():
        vacations[int(cid)] = list(vlist)


def parse_iso_date(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date()


def user_record(user) -> dict:
    return {
        "user_id": user.id,
        "username": user.username,
        "full_name": user.full_name,
    }


def normalize_username(username: str):
    username = username.strip().lstrip("@").lower()
    return username or None


def known_users(chat_id: int) -> list:
    result = {}

    def add(user: dict):
        if not user or not user.get("user_id"):
            return
        if user["user_id"] in result:
            if not result[user["user_id"]].get("username") and user.get("username"):
                result[user["user_id"]]["username"] = user.get("username")
            return
        result[user["user_id"]] = {
            "user_id": user["user_id"],
            "username": user.get("username"),
            "full_name": user["full_name"],
        }

    for user in _deps["team_members"].get(chat_id, []):
        add(user)
    for user in _deps["testers"].get(chat_id, []):
        add(user)
    add(_deps["scrum_masters"].get(chat_id))
    for user_id, b in _deps["birthdays"].get(chat_id, {}).items():
        add({
            "user_id": user_id,
            "username": b.get("username"),
            "full_name": b["full_name"],
        })
    for vacation in vacations.get(chat_id, []):
        add(vacation)
    return list(result.values())


def find_known_user_by_username(chat_id: int, username: str):
    username = normalize_username(username)
    if not username:
        return None
    for user in known_users(chat_id):
        if normalize_username(user.get("username") or "") == username:
            return user
    return None


async def resolve_vacation_target_by_token(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    token: str,
):
    chat_id = update.effective_chat.id
    current_user = update.effective_user
    username = normalize_username(token)
    if not username:
        return None, "Не понял пользователя. Используй @username или reply."
    if normalize_username(current_user.username or "") == username:
        return user_record(current_user), None
    if not await _deps["is_chat_admin"](update, context):
        return None, "Управлять отпусками других могут только админы или владелец чата."
    target = find_known_user_by_username(chat_id, username)
    if not target:
        return None, (
            "Я не знаю такого пользователя. Надёжнее ответить командой reply "
            "на сообщение нужного человека."
        )
    return target, None


async def resolve_vacation_reply_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = _deps["reply_target_user"](update)
    if not target:
        return None, None
    if target.is_bot:
        return None, "Боту отпуск не нужен."
    if target.id != update.effective_user.id and not await _deps["is_chat_admin"](update, context):
        return None, "Управлять отпусками других могут только админы или владелец чата."
    return user_record(target), None


def vacation_range(vacation: dict) -> str:
    return f"{vacation['start']} - {vacation['end']}"


def vacation_range_from_dates(start, end) -> str:
    return f"{start.isoformat()} - {end.isoformat()}"


def vacation_is_active_or_future(vacation: dict) -> bool:
    return parse_iso_date(vacation["end"]) >= _deps["now_tz"]().date()


def vacation_includes_date(vacation: dict, day) -> bool:
    return parse_iso_date(vacation["start"]) <= day <= parse_iso_date(vacation["end"])


def vacation_overlaps(vacation: dict, start, end) -> bool:
    vacation_start = parse_iso_date(vacation["start"])
    vacation_end = parse_iso_date(vacation["end"])
    return start <= vacation_end and vacation_start <= end


def active_or_future_vacations(chat_id: int, user_id=None) -> list:
    items = [
        vacation
        for vacation in vacations.get(chat_id, [])
        if vacation_is_active_or_future(vacation)
        and (user_id is None or vacation["user_id"] == user_id)
    ]
    return sorted(items, key=lambda v: (v["start"], v["end"], v["full_name"]))


def today_vacations(chat_id: int) -> list:
    today = _deps["now_tz"]().date()
    items = [
        vacation
        for vacation in vacations.get(chat_id, [])
        if vacation_includes_date(vacation, today)
    ]
    return sorted(items, key=lambda v: (v["start"], v["end"], v["full_name"]))


def vacation_today_for_user(chat_id: int, user_id: int):
    for vacation in today_vacations(chat_id):
        if vacation["user_id"] == user_id:
            return vacation
    return None


def today_vacation_user_ids(chat_id: int) -> set:
    return {vacation["user_id"] for vacation in today_vacations(chat_id)}


def today_vacations_text(chat_id: int) -> str:
    current_vacations = today_vacations(chat_id)
    if not current_vacations:
        return ""
    lines = ["", "", "🌴 Сегодня в отпуске:"]
    for vacation in current_vacations:
        name = html.escape(vacation["full_name"])
        lines.append(f"• {name}: {vacation_range(vacation)}")
    return "\n".join(lines)


def daily_scrum_master_text(
    chat_id: int,
    prefix: str = None,
    planning: bool = False,
    skip_without_scrum: bool = False,
):
    lines = []
    if planning:
        lines.append("🧭 Следующий дейли - планирование нового спринта.")
    if prefix:
        lines.append(prefix)

    sm = _deps["scrum_masters"].get(chat_id)
    if not sm:
        if skip_without_scrum:
            return None
        lines.append("Скрам-мастер не зарегистрирован.")
        lines.append("Договоритесь сами, кто ведёт дейли.")
    elif vacation_today_for_user(chat_id, sm["user_id"]):
        name = html.escape(sm["full_name"])
        lines.append(f"🌴 Скрам-мастер сегодня в отпуске: {name}.")
        lines.append("Договоритесь сами, кто ведёт дейли.")
    elif planning:
        lines.append(f"Ведёт скрам-мастер: {_deps['member_mention'](sm)}")
    else:
        lines.append(f"🧭 Дейли ведёт скрам-мастер: {_deps['member_mention'](sm)}")

    return "\n".join(lines) + today_vacations_text(chat_id)


def build_vacation_delete_keyboard(chat_id: int, user_id: int, user_vacations: list) -> InlineKeyboardMarkup:
    rows = []
    for vacation in user_vacations:
        rows.append([
            InlineKeyboardButton(
                f"🌴 {vacation_range(vacation)}",
                callback_data=f"vacdel:{chat_id}:{user_id}:{vacation['id']}",
            )
        ])
    return InlineKeyboardMarkup(rows)


def make_vacation_id(chat_id: int) -> str:
    existing_ids = {vacation["id"] for vacation in vacations.get(chat_id, [])}
    for _ in range(10):
        vacation_id = uuid4().hex[:10]
        if vacation_id not in existing_ids:
            return vacation_id
    return uuid4().hex


async def setvacation_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    target, error = await resolve_vacation_reply_target(update, context)
    if error:
        await update.message.reply_text(error)
        return

    if target:
        if len(args) != 2:
            await update.message.reply_text("Формат: /setvacation YYYY-MM-DD YYYY-MM-DD")
            return
        date_args = args
    elif len(args) == 2:
        target = user_record(update.effective_user)
        date_args = args
    elif len(args) == 3:
        target, error = await resolve_vacation_target_by_token(update, context, args[0])
        if error:
            await update.message.reply_text(error)
            return
        date_args = args[1:]
    else:
        await update.message.reply_text(
            "Формат: /setvacation [@username] YYYY-MM-DD YYYY-MM-DD\n"
            "Чтобы поставить отпуск другому - ответь этой командой на его сообщение."
        )
        return

    try:
        start = parse_iso_date(date_args[0])
        end = parse_iso_date(date_args[1])
    except ValueError:
        await update.message.reply_text("Неверный формат даты, нужно YYYY-MM-DD.")
        return

    if end < start:
        await update.message.reply_text("Дата окончания не может быть раньше даты начала.")
        return
    if end < _deps["now_tz"]().date():
        await update.message.reply_text("Этот отпуск уже закончился, сохранять его не буду.")
        return

    for vacation in vacations.get(chat_id, []):
        if vacation["user_id"] == target["user_id"] and vacation_overlaps(vacation, start, end):
            await update.message.reply_text(
                "🌴 У этого человека уже есть отпуск на эти даты:\n"
                f"{vacation_range(vacation)}"
            )
            return

    vacation = {
        "id": make_vacation_id(chat_id),
        "user_id": target["user_id"],
        "username": target.get("username"),
        "full_name": target["full_name"],
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    vacations.setdefault(chat_id, []).append(vacation)
    _deps["save_state"]()

    await update.message.reply_text(
        "✅ 🌴 Отпуск сохранён:\n"
        f"{target['full_name']}\n"
        f"{vacation_range_from_dates(start, end)}"
    )


async def unsetvacation_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    target, error = await resolve_vacation_reply_target(update, context)
    if error:
        await update.message.reply_text(error)
        return

    if target:
        if args:
            await update.message.reply_text("Формат: /unsetvacation")
            return
    elif len(args) == 0:
        target = user_record(update.effective_user)
    elif len(args) == 1:
        target, error = await resolve_vacation_target_by_token(update, context, args[0])
        if error:
            await update.message.reply_text(error)
            return
    else:
        await update.message.reply_text("Формат: /unsetvacation [@username]")
        return

    user_vacations = active_or_future_vacations(chat_id, target["user_id"])
    if not user_vacations:
        await update.message.reply_text("🌴 Отпусков нет, сотрудник работяга.")
        return

    lines = [f"🌴 Отпуска {target['full_name']}:"]
    for index, vacation in enumerate(user_vacations, start=1):
        lines.append(f"{index}. {vacation_range(vacation)}")
    lines.append("")
    lines.append("Что удалить?")

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=build_vacation_delete_keyboard(chat_id, target["user_id"], user_vacations),
    )


async def vacations_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args

    target, error = await resolve_vacation_reply_target(update, context)
    if error:
        await update.message.reply_text(error)
        return

    if target:
        if args:
            await update.message.reply_text("Формат: /vacations")
            return
    elif len(args) == 1:
        target, error = await resolve_vacation_target_by_token(update, context, args[0])
        if error:
            await update.message.reply_text(error)
            return
    elif len(args) > 1:
        await update.message.reply_text("Формат: /vacations [@username]")
        return

    if target:
        user_vacations = active_or_future_vacations(chat_id, target["user_id"])
        if not user_vacations:
            await update.message.reply_text("🌴 Отпусков нет, сотрудник работяга.")
            return
        lines = [f"🌴 Отпуска {target['full_name']}:"]
        for vacation in user_vacations:
            lines.append(f"• {vacation_range(vacation)}")
    else:
        chat_vacations = active_or_future_vacations(chat_id)
        if not chat_vacations:
            await update.message.reply_text("🌴 Отпусков нет.")
            return
        lines = ["🌴 Активные и будущие отпуска:"]
        for vacation in chat_vacations:
            lines.append(f"• {vacation['full_name']}: {vacation_range(vacation)}")

    await update.message.reply_text("\n".join(lines))


async def vacation_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split(":")
    if len(parts) != 4 or parts[0] != "vacdel":
        await query.answer()
        return

    try:
        chat_id = int(parts[1])
        user_id = int(parts[2])
    except ValueError:
        await query.answer()
        return
    vacation_id = parts[3]

    if query.message.chat_id != chat_id:
        await query.answer("Это удаление не из этого чата.", show_alert=True)
        return

    if query.from_user.id != user_id and not await _deps["is_chat_admin"](update, context):
        await query.answer(
            "Удалять чужие отпуска могут только админы или владелец чата.",
            show_alert=True,
        )
        return

    chat_vacations = vacations.get(chat_id, [])
    vacation = next(
        (
            item for item in chat_vacations
            if item["id"] == vacation_id and item["user_id"] == user_id
        ),
        None,
    )
    if not vacation:
        await query.answer("Отпуск уже удалён.", show_alert=True)
        return

    vacations[chat_id] = [item for item in chat_vacations if item["id"] != vacation_id]
    _deps["save_state"]()
    await query.edit_message_text(
        "🗑 🌴 Отпуск удалён:\n"
        f"{vacation['full_name']}\n"
        f"{vacation_range(vacation)}"
    )
    await query.answer("Удалено")
