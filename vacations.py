import calendar
import html
import os
import tempfile
from datetime import date, datetime
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes


# {chat_id: [{"id", "user_id", "username", "full_name", "start", "end"}, ...]}
vacations = {}

_deps = {}

PDF_COLOR_PALETTE = [
    "#8EC5FF",
    "#8FE3D9",
    "#FF9AA2",
    "#C8E986",
    "#FFF59D",
    "#C4B5FD",
    "#FDBA74",
    "#A7F3D0",
    "#F9A8D4",
    "#93C5FD",
    "#FDE68A",
    "#D8B4FE",
]

PDF_REGULAR_FONT = "Helvetica"
PDF_BOLD_FONT = "Helvetica-Bold"
PDF_FONT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "fonts")


class MissingPdfFontError(RuntimeError):
    pass


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
    # Зависимости приходят из bot.py, чтобы не тянуть весь bot.py обратно в этот модуль.
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
    # Собираем всех известных боту людей: это нужно для команд с @username.
    # Telegram не даёт получить произвольного пользователя по username без контекста.
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
    # /setvacation @username ... для себя доступен всем, для другого человека только админам.
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
    # Reply - самый надёжный способ выбрать человека: Telegram уже дал нам его user_id.
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
    # Пересечение inclusive-интервалов: отпуск 01-05 конфликтует с 05-10.
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
    # Общий fallback для дейли: скрам-мастер, конфликт отпуска или "договоритесь сами".
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


def vacation_intersects_year(vacation: dict, year: int) -> bool:
    # Нужны и отпуска, которые начались в прошлом году или заканчиваются в следующем.
    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    return parse_iso_date(vacation["start"]) <= year_end and year_start <= parse_iso_date(vacation["end"])


def vacations_for_year(chat_id: int, year: int) -> list:
    items = [
        vacation
        for vacation in vacations.get(chat_id, [])
        if vacation_intersects_year(vacation, year)
    ]
    return sorted(items, key=lambda v: (v["start"], v["end"], v["full_name"]))


def _safe_pdf_filename(chat_id: int, year: int) -> str:
    return f"vacations-{year}-{abs(chat_id)}.pdf"


def _register_pdf_fonts(pdfmetrics, TTFont):
    # ReportLab built-in Helvetica не умеет кириллицу. Сначала ищем шрифт в репе,
    # потом системный TTF-шрифт. Если не нашли - PDF не генерим.
    regular_paths = [
        os.path.join(PDF_FONT_DIR, "DejaVuSans.ttf"),
        os.path.join(PDF_FONT_DIR, "NotoSans-Regular.ttf"),
        os.path.join(PDF_FONT_DIR, "Arial.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/usr/local/share/fonts/DejaVuSans.ttf",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
    ]
    bold_paths = [
        os.path.join(PDF_FONT_DIR, "DejaVuSans-Bold.ttf"),
        os.path.join(PDF_FONT_DIR, "NotoSans-Bold.ttf"),
        os.path.join(PDF_FONT_DIR, "Arial Bold.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
        "/usr/local/share/fonts/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    ]

    regular_font = PDF_REGULAR_FONT
    bold_font = PDF_BOLD_FONT
    for path in regular_paths:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("GOS-Regular", path))
                regular_font = "GOS-Regular"
                break
            except Exception:
                continue
    for path in bold_paths:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("GOS-Bold", path))
                bold_font = "GOS-Bold"
                break
            except Exception:
                continue
    if regular_font == PDF_REGULAR_FONT:
        raise MissingPdfFontError(
            "Не найден TTF-шрифт с кириллицей для PDF. "
            "Проверь assets/fonts/DejaVuSans.ttf или установи fonts-dejavu-core."
        )
    if bold_font == PDF_BOLD_FONT:
        bold_font = regular_font
    return regular_font, bold_font


def _hex_color(hex_color: str):
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i:i + 2], 16) / 255 for i in (0, 2, 4))


def _text_color_for_bg(hex_color: str):
    # Подбираем тёмный или белый текст под цвет плашки.
    r, g, b = _hex_color(hex_color)
    brightness = (r * 299 + g * 587 + b * 114) / 1000
    return "#111827" if brightness > 0.58 else "#FFFFFF"


def _short_pdf_name(full_name: str) -> str:
    parts = full_name.split()
    if not parts:
        return full_name
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[1][0]}."


def _fit_pdf_text(canvas, text: str, max_width: float, font_name: str, font_size: float) -> str:
    if canvas.stringWidth(text, font_name, font_size) <= max_width:
        return text
    suffix = "..."
    while text and canvas.stringWidth(text + suffix, font_name, font_size) > max_width:
        text = text[:-1]
    return text + suffix if text else suffix


def _month_weeks(year: int, month: int) -> list:
    # Всегда рисуем 6 строк, чтобы все месяцы в PDF имели одинаковую сетку.
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdatescalendar(year, month)
    while len(weeks) < 6:
        last_day = weeks[-1][-1]
        weeks.append([date.fromordinal(last_day.toordinal() + i) for i in range(1, 8)])
    return weeks[:6]


def _vacations_by_date(year_vacations: list) -> dict:
    # Разворачиваем диапазоны отпусков в словарь "день -> люди в отпуске".
    result = {}
    for vacation in year_vacations:
        start = parse_iso_date(vacation["start"])
        end = parse_iso_date(vacation["end"])
        current = start
        while current <= end:
            result.setdefault(current, []).append(vacation)
            current = current.fromordinal(current.toordinal() + 1)
    for day_vacations in result.values():
        day_vacations.sort(key=lambda v: (v["full_name"], v["start"], v["end"]))
    return result


def _vacation_color_map(year_vacations: list) -> dict:
    # Цвет закрепляется за человеком стабильно в рамках всего PDF за год.
    users = {}
    for vacation in year_vacations:
        users[vacation["user_id"]] = vacation["full_name"]
    ordered_user_ids = [
        user_id for user_id, _ in sorted(users.items(), key=lambda item: (item[1], item[0]))
    ]
    return {
        user_id: PDF_COLOR_PALETTE[index % len(PDF_COLOR_PALETTE)]
        for index, user_id in enumerate(ordered_user_ids)
    }


def _draw_badge(canvas, x, y, width, height, color, text, font_name, font_size):
    from reportlab.lib.colors import HexColor

    canvas.setFillColor(HexColor(color))
    canvas.roundRect(x, y, width, height, 3, stroke=0, fill=1)
    canvas.setFillColor(HexColor(_text_color_for_bg(color)))
    canvas.setFont(font_name, font_size)
    canvas.drawString(
        x + 4,
        y + (height - font_size) / 2,
        _fit_pdf_text(canvas, text, width - 8, font_name, font_size),
    )


def _draw_compact_vacation_list(canvas, items, color_map, x, y, width, height, font_name):
    from reportlab.lib.colors import HexColor

    if not items:
        return
    # Если людей много, плашки будут нечитаемыми. Рисуем компактный список с цветными точками.
    top = y + height - 20
    bottom = y + 5
    available_h = max(10, top - bottom)
    columns = 1 if len(items) <= 8 else 2
    rows = (len(items) + columns - 1) // columns
    line_h = min(8.0, max(5.2, available_h / rows))
    font_size = max(4.8, line_h - 1.0)
    column_w = (width - 12) / columns

    for index, vacation in enumerate(items):
        col = index // rows
        row = index % rows
        tx = x + 6 + col * column_w
        ty = top - row * line_h - font_size
        color = color_map[vacation["user_id"]]
        canvas.setFillColor(HexColor(color))
        canvas.circle(tx + 2, ty + font_size / 2, 2, stroke=0, fill=1)
        canvas.setFillColor(HexColor("#111827"))
        canvas.setFont(font_name, font_size)
        name = _short_pdf_name(vacation["full_name"])
        max_text_w = column_w - 11
        if canvas.stringWidth(name, font_name, font_size) > max_text_w:
            # В компактном режиме важнее показать понятное имя, чем фамильный инициал.
            name = vacation["full_name"].split()[0] if vacation["full_name"].split() else name
        canvas.drawString(
            tx + 7,
            ty,
            _fit_pdf_text(canvas, name, max_text_w, font_name, font_size),
        )


def _draw_day_vacations(canvas, items, color_map, x, y, width, height, regular_font, bold_font):
    # До 4 человек показываем плашками, 5+ - компактным списком без скрытых "+N".
    if len(items) <= 4:
        badge_h = 12
        gap = 3
        start_y = y + height - 23
        for index, vacation in enumerate(items):
            by = start_y - index * (badge_h + gap) - badge_h
            if by < y + 4:
                break
            _draw_badge(
                canvas,
                x + 6,
                by,
                width - 12,
                badge_h,
                color_map[vacation["user_id"]],
                _short_pdf_name(vacation["full_name"]),
                bold_font,
                6.8,
            )
        return
    _draw_compact_vacation_list(canvas, items, color_map, x, y, width, height, regular_font)


def _draw_month_page(canvas, year, month, year_vacations, regular_font, bold_font):
    from reportlab.lib.colors import HexColor
    from reportlab.lib.pagesizes import A4, landscape

    month_names = [
        "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
        "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
    ]
    day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    page_w, page_h = landscape(A4)
    margin = 28
    title_y = page_h - 34
    grid_top = page_h - 72
    grid_bottom = 30
    day_header_h = 20
    grid_w = page_w - margin * 2
    grid_h = grid_top - grid_bottom
    cell_w = grid_w / 7
    cell_h = (grid_h - day_header_h) / 6
    vacations_by_day = _vacations_by_date(year_vacations)
    color_map = _vacation_color_map(year_vacations)

    canvas.setFillColor(HexColor("#111827"))
    canvas.setFont(bold_font, 18)
    canvas.drawString(margin, title_y, f"Отпуска команды - {year}")
    canvas.setFont(regular_font, 11)
    canvas.setFillColor(HexColor("#4B5563"))
    canvas.drawString(margin, title_y - 18, f"{month_names[month - 1]} {year}")

    header_y = grid_top - day_header_h
    canvas.setFillColor(HexColor("#F9FAFB"))
    canvas.rect(margin, header_y, grid_w, day_header_h, stroke=0, fill=1)
    canvas.setStrokeColor(HexColor("#E5E7EB"))
    canvas.setLineWidth(0.5)

    for index, day_name in enumerate(day_names):
        x = margin + index * cell_w
        canvas.setFillColor(HexColor("#374151"))
        canvas.setFont(bold_font, 8.5)
        canvas.drawCentredString(x + cell_w / 2, header_y + 6, day_name)
        canvas.line(x, grid_bottom, x, grid_top)
    canvas.line(margin + grid_w, grid_bottom, margin + grid_w, grid_top)
    canvas.line(margin, grid_top, margin + grid_w, grid_top)
    canvas.line(margin, header_y, margin + grid_w, header_y)

    weeks = _month_weeks(year, month)
    for row, week in enumerate(weeks):
        y = header_y - (row + 1) * cell_h
        canvas.line(margin, y, margin + grid_w, y)
        for col, day in enumerate(week):
            x = margin + col * cell_w
            if day.month != month:
                canvas.setFillColor(HexColor("#FBFBFC"))
                canvas.rect(x, y, cell_w, cell_h, stroke=0, fill=1)
            elif day.weekday() >= 5:
                canvas.setFillColor(HexColor("#FCFCFD"))
                canvas.rect(x, y, cell_w, cell_h, stroke=0, fill=1)

            canvas.setFillColor(HexColor("#9CA3AF" if day.month != month else "#111827"))
            canvas.setFont(bold_font, 8.5)
            canvas.drawString(x + 5, y + cell_h - 12, str(day.day))
            # На соседних днях из прошлого/следующего месяца отпуска не показываем.
            _draw_day_vacations(
                canvas,
                vacations_by_day.get(day, []) if day.month == month else [],
                color_map,
                x,
                y,
                cell_w,
                cell_h,
                regular_font,
                bold_font,
            )

    canvas.setStrokeColor(HexColor("#E5E7EB"))
    canvas.setLineWidth(0.5)
    # Фоны выходных/соседних месяцев рисуются поверх сетки, поэтому линии кладём финальным слоем.
    for col in range(8):
        x = margin + col * cell_w
        canvas.line(x, grid_bottom, x, grid_top)
    canvas.line(margin, grid_top, margin + grid_w, grid_top)
    canvas.line(margin, header_y, margin + grid_w, header_y)
    for row in range(7):
        y = header_y - row * cell_h
        canvas.line(margin, y, margin + grid_w, y)


def generate_vacation_year_pdf(chat_id: int, year: int, output_path: str):
    # Импорт reportlab ленивый: бот стартует даже до установки PDF-зависимостей.
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.pdfgen import canvas

    year_vacations = vacations_for_year(chat_id, year)
    regular_font, bold_font = _register_pdf_fonts(pdfmetrics, TTFont)
    pdf = canvas.Canvas(output_path, pagesize=landscape(A4))
    pdf.setTitle(f"Отпуска команды - {year}")
    for month in range(1, 13):
        _draw_month_page(pdf, year, month, year_vacations, regular_font, bold_font)
        pdf.showPage()
    pdf.save()


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


async def vacationpdf_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    if len(args) > 1:
        await update.message.reply_text("Формат: /vacationpdf [YYYY]")
        return
    if args:
        try:
            year = int(args[0])
        except ValueError:
            await update.message.reply_text("Год должен быть числом, например: /vacationpdf 2026")
            return
        if not (2000 <= year <= 2100):
            await update.message.reply_text("Год должен быть в диапазоне 2000-2100.")
            return
    else:
        year = _deps["now_tz"]().year

    if not vacations_for_year(chat_id, year):
        await update.message.reply_text(f"🌴 Отпусков за {year} нет.")
        return

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            filename = _safe_pdf_filename(chat_id, year)
            output_path = os.path.join(tmp_dir, filename)
            generate_vacation_year_pdf(chat_id, year, output_path)
            with open(output_path, "rb") as pdf_file:
                await update.message.reply_document(
                    document=pdf_file,
                    filename=filename,
                    caption=f"🌴 Отпуска команды за {year}",
                )
    except ModuleNotFoundError:
        await update.message.reply_text(
            "Для генерации PDF нужен reportlab. Установи зависимости: pip install -r requirements.txt"
        )
    except MissingPdfFontError:
        await update.message.reply_text(
            "Не нашёл TTF-шрифт с кириллицей для PDF.\n"
            "Проверь assets/fonts/DejaVuSans.ttf или установи системный пакет со шрифтами DejaVu."
        )


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
