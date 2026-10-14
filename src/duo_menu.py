"""
Telegram-меню «🎯 Стратегии» бота двух стратегий (логика — src/duo.py):
тексты, кнопки, обработка нажатий, статистика и строки для 4-часового отчёта.
Отдельно от telegram_notify.py, чтобы не раздувать его ещё больше.
"""
from __future__ import annotations
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import settings
from src import duo, loss_stats, runtime_state, storage

N_PRESETS = (0, 1, 2, 3, 4, 5, 6, 8)


def _enabled_assets() -> list[str]:
    return [a for a in settings.ASSETS if runtime_state.is_asset_enabled(a)]


def _live_coins_text() -> str:
    if runtime_state.get("dry_run"):
        return "нет — бот в DRY RUN (🔴 Включить LIVE в главном меню)"
    coins = [a.upper() for a in _enabled_assets() if runtime_state.is_asset_live(a)]
    return ", ".join(coins) if coins else "нет — ни одна монета не 🔴 LIVE (🪙 Активы)"


def _st_short(label: str, st: dict) -> str:
    n = st.get("trades", 0)
    if not n:
        return f"{label}: пока нет закрытых сделок"
    exp = st["expected_losses"]
    k = st["losses"] / exp if exp > 0 else 0.0
    return (f"{label}: {loss_stats.plural_trades(n)}, проигрышей {st['losses']} "
            f"(ожидалось ≈{exp:.1f}, k={k:.2f}), PnL {st['pnl_usdc']:+.2f}")


def _strategy_lines(since_ts: int = 0) -> list[str]:
    lines = [_st_short("👻 Виртуальные", storage.get_outcome_stats(live=False, since_ts=since_ts, strategy=duo.VIRT))]
    for strat in duo.STRATS:
        real = storage.get_outcome_stats(live=True, since_ts=since_ts, strategy=strat)
        test = storage.get_outcome_stats(live=False, since_ts=since_ts, strategy=strat)
        if not real["trades"] and not test["trades"]:
            lines.append(f"{duo.SHORT[strat]}: пока нет закрытых сделок")
            continue
        if real["trades"]:
            lines.append(_st_short(f"{duo.SHORT[strat]} реально", real))
        if test["trades"]:
            lines.append(_st_short(f"{duo.SHORT[strat]} тест", test))
    return lines


# -------------------------------------------------------- 🎯 Стратегии ----

def menu_text(now: float | None = None) -> str:
    now = time.time() if now is None else now
    assets = _enabled_assets()
    pause = int(runtime_state.get("strat_b_pause_min"))
    lines = [
        "🎯 Стратегии",
        "",
        "Каждый сигнал бот записывает как виртуальную сделку 👻 (логика сломанного бота"
        + (": фильтр расстояния выкл" if not runtime_state.get("distance_filter_enabled") else
           ", но 📏 фильтр расстояния сейчас ВКЛ") + "). По их результатам стратегии решают, "
        "когда входить по-настоящему.",
        "",
        f"1️⃣ После проигрыша — {duo.mode_text(duo.A)}",
        "После проигрыша монеты — N входов по ней (N своё у монеты, 🔢). Проигрыш внутри серии "
        "начинает её заново.",
        f"Сейчас: {duo.a_state_text(assets)}",
        "",
        f"2️⃣ Пауза после проигрыша — {duo.mode_text(duo.B)}",
        (f"Вход по монете, только если за последние {pause} мин у неё не было проигрышей — "
         "каждая монета сама по себе." if duo.scope() == "coin" else
         f"Вход, только если за последние {pause} мин не было проигрышей ни по одной монете "
         "(общая пауза на все монеты)."),
        f"Сейчас: {duo.b_state_text(now)}",
        "",
        "💵 реально — реальный ордер, если бот в LIVE и монета 🔴 LIVE в 🪙 Активах; "
        f"сейчас реальными можно: {_live_coins_text()}. 🧪 тест — входы стратегии пишутся "
        "виртуально, со своей статистикой.",
        "Ставка — общая: 💰 Размер позиции (своя у монеты — ⚙️ в 🪙 Активах).",
        "",
        "Итоги:",
    ]
    lines += _strategy_lines()
    cmp_line = duo.compare_pause_text()
    if cmp_line:
        lines += ["", cmp_line + " (как если бы 2️⃣ входила по всем виртуальным; на истории 18.09–12.10 "
                  "у каждой монеты k=1.05, общая k=0.85)"]
    early = duo.early_stats_text()
    if early:
        lines += ["", early]
    return "\n".join(lines)


def menu_markup() -> InlineKeyboardMarkup:
    rows = []
    for strat in duo.STRATS:
        on = duo.strategy_enabled(strat)
        live = duo.strategy_live(strat)
        rows.append([
            InlineKeyboardButton(f"{duo.SHORT[strat]} {'✅ вкл' if on else '⏸ выкл'}", callback_data=f"duo:on:{strat}"),
            InlineKeyboardButton(f"{duo.SHORT[strat]} {'💵 реально' if live else '🧪 тест'}",
                                 callback_data=f"duo:live:{strat}"),
        ])
        if strat == duo.A:
            rows.append([InlineKeyboardButton("🔢 1️⃣ Входов после проигрыша по монетам", callback_data="duo:n")])
    cur = int(runtime_state.get("strat_b_pause_min"))
    rows.append([
        InlineKeyboardButton(("✅ " if cur == m else "") + f"⏳ {m} мин", callback_data=f"duo:pause:{m}")
        for m in runtime_state.STRAT_B_PAUSE_PRESETS
    ])
    sc = duo.scope()
    rows.append([
        InlineKeyboardButton(("✅ " if sc == "coin" else "") + "🪙 Пауза у каждой монеты", callback_data="duo:scope:coin"),
        InlineKeyboardButton(("✅ " if sc == "all" else "") + "🌐 Общая", callback_data="duo:scope:all"),
    ])
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="duo:menu")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def n_menu_text() -> str:
    lines = [
        "🔢 Стратегия 1️⃣: сколько входов делать после проигрыша монеты.",
        "Проигрыш внутри серии начинает её заново, так что серия кончается после N входов "
        "подряд без проигрыша. 0 — по монете стратегия не входит.",
        "",
    ]
    for a in settings.ASSETS:
        n = runtime_state.strat_a_count(a)
        rem = duo.a_remaining(a)
        state = f"идёт серия, осталось {rem}" if rem else ("не входит" if n == 0 else "ждёт проигрыша")
        off = "" if runtime_state.is_asset_enabled(a) else " (монета выключена)"
        lines.append(f"{a.upper()}: N = {n} — {state}{off}")
    return "\n".join(lines)


def n_menu_markup() -> InlineKeyboardMarkup:
    rows, row = [], []
    for a in settings.ASSETS:
        row.append(InlineKeyboardButton(f"{a.upper()}: {runtime_state.strat_a_count(a)} ✏️",
                                        callback_data=f"duo:nc:{a}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="duo:menu")])
    return InlineKeyboardMarkup(rows)


def n_pick_text(asset: str) -> str:
    return (f"🔢 {asset.upper()}: сейчас N = {runtime_state.strat_a_count(asset)}. Сколько входов делать "
            f"после проигрыша {asset.upper()}?")


def n_pick_markup(asset: str) -> InlineKeyboardMarkup:
    cur = runtime_state.strat_a_count(asset)
    nums = [InlineKeyboardButton(("✅ " if n == cur else "") + str(n), callback_data=f"duo:ns:{asset}:{n}")
            for n in N_PRESETS]
    rows = [nums[:4], nums[4:]]
    rows.append([InlineKeyboardButton("✏️ Своё число", callback_data=f"duo:ni:{asset}")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="duo:n")])
    return InlineKeyboardMarkup(rows)


def handle(data: str):
    """Нажатие кнопки «duo:…». Возвращает (текст, кнопки, ожидание_ввода)
    или None, если кнопка не отсюда."""
    if not data.startswith("duo:"):
        return None
    parts = data.split(":")
    cmd = parts[1] if len(parts) > 1 else ""
    if cmd == "menu":
        return menu_text(), menu_markup(), None
    if cmd == "on" and len(parts) == 3 and parts[2] in duo.STRATS:
        strat = parts[2]
        key = "strat_a_enabled" if strat == duo.A else "strat_b_enabled"
        runtime_state.set(key, not runtime_state.get(key))
        if strat == duo.A:
            duo.reset_a()   # с чистого листа: ждём следующего проигрыша
        return menu_text(), menu_markup(), None
    if cmd == "live" and len(parts) == 3 and parts[2] in duo.STRATS:
        strat = parts[2]
        key = "strat_a_live" if strat == duo.A else "strat_b_live"
        runtime_state.set(key, not runtime_state.get(key))
        head = ""
        if runtime_state.get(key):
            head = (f"✅ {duo.LABELS[strat]}: реальные входы по монетам: {_live_coins_text()}. "
                    "Ставка — как в 💰 Размер позиции.\n\n")
        return head + menu_text(), menu_markup(), None
    if cmd == "pause" and len(parts) == 3:
        try:
            minutes = int(parts[2])
        except ValueError:
            return menu_text(), menu_markup(), None
        if minutes in runtime_state.STRAT_B_PAUSE_PRESETS:
            runtime_state.set("strat_b_pause_min", minutes)
        return menu_text(), menu_markup(), None
    if cmd == "scope" and len(parts) == 3 and parts[2] in ("coin", "all"):
        runtime_state.set("strat_b_scope", parts[2])
        return menu_text(), menu_markup(), None
    if cmd == "n":
        return n_menu_text(), n_menu_markup(), None
    if cmd in ("nc", "ns", "ni") and len(parts) >= 3 and parts[2] in settings.ASSETS:
        asset = parts[2]
        if cmd == "nc":
            return n_pick_text(asset), n_pick_markup(asset), None
        if cmd == "ni":
            return (f"✏️ Напиши следующим сообщением, сколько входов делать после проигрыша {asset.upper()} "
                    f"(целое от 0 до {runtime_state.STRAT_A_MAX_N}, 0 — не входить).",
                    InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"duo:nc:{asset}")]]),
                    f"duo_n:{asset}")
        try:
            n = int(parts[3])
        except (IndexError, ValueError):
            return n_menu_text(), n_menu_markup(), None
        set_n(asset, n)
        return "✅ " + n_menu_text(), n_menu_markup(), None
    return menu_text(), menu_markup(), None


def set_n(asset: str, n: int) -> int:
    """N стратегии 1️⃣ у монеты; идущая серия не длиннее нового N."""
    n = runtime_state.set_strat_a_count(asset, n)
    return n


def apply_text_input(pending: str, raw: str):
    """Текст после «✏️ Своё число». Возвращает (ответ, кнопки) или None, если
    ввод не наш. Ошибку ввода — (текст ошибки, None)."""
    if not pending.startswith("duo_n:"):
        return None
    asset = pending.split(":", 1)[1]
    if asset not in settings.ASSETS:
        return "Монета не найдена.", None
    try:
        n = int(float(raw.strip().replace(",", ".")))
    except ValueError:
        return f"Нужно целое число от 0 до {runtime_state.STRAT_A_MAX_N}, например 3.", None
    if n < 0:
        return f"Нужно целое число от 0 до {runtime_state.STRAT_A_MAX_N}, например 3.", None
    set_n(asset, n)
    return "✅ " + n_menu_text(), n_menu_markup()


# ---------------------------------------------------------- 📊 и отчёты ----

def stats_text() -> str:
    """📊 Статистика в боте двух стратегий (Markdown)."""
    today_start = int(time.time() // 86400) * 86400
    lines = ["📊 *Статистика по стратегиям*", "", "Сегодня (с 00:00 UTC):"]
    lines += ["  " + x for x in _strategy_lines(today_start)]
    lines += ["", "Всего:"]
    for label, live, strat in (("👻 Виртуальные", False, duo.VIRT),
                               ("1️⃣ реально", True, duo.A), ("1️⃣ тест", False, duo.A),
                               ("2️⃣ реально", True, duo.B), ("2️⃣ тест", False, duo.B)):
        st = storage.get_outcome_stats(live=live, strategy=strat)
        if not st["trades"] and strat != duo.VIRT:
            continue
        lines.extend(loss_stats.summary_lines(label, st))
        if st["trades"]:
            lines.append(f"  PnL {st['pnl_usdc']:+.2f} USDC" + ("" if live else " (виртуально)"))
    lines += ["", "*По монетам* (виртуальные — так стратегия видит рынок):"]
    for a in settings.ASSETS:
        st = storage.get_outcome_stats(live=False, asset=a, strategy=duo.VIRT)
        if st["trades"]:
            lines.append(f"  {_st_short(a.upper(), st)}")
    real = storage.get_pnl_summary(0, live_only=True)
    lines += ["", f"Реальные деньги всего: {loss_stats.plural_trades(real['trades'])}, PnL {real['pnl_usdc']:+.2f} USDC",
              "Все сделки одним файлом: /export (колонка strategy: virt, A, B)"]
    return "\n".join(lines)


def report_lines(since_ts: int) -> list[str]:
    """Строки для подписи 4-часового отчёта: итоги стратегий за период и
    состояние сейчас."""
    summ = storage.get_strategy_summary(since_ts)
    out = ["🎯 Стратегии за период:"]
    order = [(duo.VIRT, True), (duo.A, False), (duo.A, True), (duo.B, False), (duo.B, True)]
    for strat, dry in order:
        b = summ.get((strat, dry))
        if not b:
            continue
        name = duo.SHORT[strat] + ("" if strat == duo.VIRT else (" тест" if dry else " реально"))
        k = b["losses"] / b["expected_losses"] if b["expected_losses"] > 0 else 0.0
        out.append(f"  {name}: {loss_stats.plural_trades(b['trades'])}, проигрышей {b['losses']} "
                   f"(k={k:.2f}), PnL {b['pnl_usdc']:+.2f}")
    if len(out) == 1:
        out.append("  закрытых сделок нет")
    out.append(duo.summary_line())
    cmp_line = duo.compare_pause_text(since_ts)
    if cmp_line:
        out.append(cmp_line)
    early = duo.early_stats_text()
    if early:
        out.append(early)
    return out
