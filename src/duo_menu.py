"""
Telegram-меню «🎯 Стратегии» бота двух стратегий (логика — src/duo.py):
тексты, кнопки, обработка нажатий, статистика и строки для 4-часового отчёта.
Отдельно от telegram_notify.py, чтобы не раздувать его ещё больше.
"""
from __future__ import annotations
import math
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


def _combined_lines(stats: dict) -> list[str]:
    """Режим отыгрыша: итог 1️⃣ и 2️⃣ вместе — перекрывает ли 1️⃣ минусы 2️⃣.
    stats: {(strat, live): get_outcome_stats(...)}."""
    if duo.a_mode() != "recover":
        return []
    out = []
    for live, name in ((True, "реально"), (False, "тест")):
        a, b = stats.get((duo.A, live)), stats.get((duo.B, live))
        if a and b and a["trades"] and b["trades"]:
            out.append(f"1️⃣+2️⃣ {name} вместе: {loss_stats.plural_trades(a['trades'] + b['trades'])}, "
                       f"PnL {a['pnl_usdc'] + b['pnl_usdc']:+.2f}")
    return out


def _strategy_lines(since_ts: int = 0) -> list[str]:
    lines = [_st_short("👻 Виртуальные", storage.get_outcome_stats(live=False, since_ts=since_ts, strategy=duo.VIRT))]
    stats = {}
    for strat in duo.STRATS:
        real = storage.get_outcome_stats(live=True, since_ts=since_ts, strategy=strat)
        test = storage.get_outcome_stats(live=False, since_ts=since_ts, strategy=strat)
        stats[(strat, True)], stats[(strat, False)] = real, test
        if not real["trades"] and not test["trades"]:
            lines.append(f"{duo.SHORT[strat]}: пока нет закрытых сделок")
            continue
        if real["trades"]:
            lines.append(_st_short(f"{duo.SHORT[strat]} реально", real))
        if test["trades"]:
            lines.append(_st_short(f"{duo.SHORT[strat]} тест", test))
    return lines + _combined_lines(stats)


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
        f"{duo.label(duo.A)} — {duo.mode_text(duo.A)}",
        ("2️⃣ проиграла по монете — 1️⃣ один раз входит по этой монете на следующем сигнале на сумму, "
         "выигрыш с которой (за вычетом комиссии) возвращает этот минус. Не больше банка монеты; "
         "проиграл отыгрыш — повтора нет. Вход реальный, только если и минус 2️⃣ реальный, и 1️⃣ "
         "в «💵 реально»; иначе — тестовый (видно, как сработал бы). Пример: минус $5 по цене 0.92 — "
         "ставка ≈$61."
         if duo.a_mode() == "recover" else
         "После проигрыша монеты — N входов по ней (N своё у монеты, 🔢). Проигрыш внутри серии "
         "начинает её заново."),
        f"Сейчас: {duo.a_state_text(assets)}",
        "",
        f"2️⃣ Пауза после проигрыша — {duo.mode_text(duo.B)}",
        (f"Вход по монете, только если за последние {pause} мин у неё не было проигрышей — "
         "каждая монета сама по себе." if duo.scope() == "coin" else
         f"Вход, только если за последние {pause} мин не было проигрышей ни по одной монете "
         "(общая пауза на все монеты)."),
        f"Сейчас: {duo.b_state_text(now)}",
        "",
        f"🧹 Фильтры входа 1️⃣ и 2️⃣: {duo.filters_text()}. Решают один раз на рынок, по первому сигналу; "
        "виртуальные сделки пишутся всегда.",
        "",
        "💵 реально — реальный ордер, если бот в LIVE и монета 🔴 LIVE в 🪙 Активах; "
        f"сейчас реальными можно: {_live_coins_text()}. 🧪 тест — входы стратегии пишутся "
        "виртуально, со своей статистикой.",
        "Ставка 2️⃣" + (" и 1️⃣" if duo.a_mode() == "series" else "") + " — 💰 Размер позиции (своя у монеты — "
        "⚙️ в 🪙 Активах): фикс. или % банка монеты"
        + ("; ставку отыгрыша 1️⃣ бот считает сам" if duo.a_mode() == "recover" else "")
        + ". Банк у каждой монеты свой — 🏦, ставка не больше него. Виртуальные — по $1.",
        "",
        "Итоги:",
    ]
    lines += _strategy_lines()
    cmp_line = duo.compare_pause_text()
    if cmp_line:
        lines += ["", cmp_line + " (как если бы 2️⃣ входила по всем виртуальным; на архиве 18.09–12.10 "
                  "у каждой монеты k=0.97, общая 0.82; 13–15.10 — 0.98 и 0.84)"]
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
            recover = duo.a_mode() == "recover"
            rows.append([InlineKeyboardButton(
                "1️⃣ режим: 🩹 отыгрыш минуса 2️⃣ → сменить на «N после проигрыша»" if recover
                else "1️⃣ режим: 🔁 N после проигрыша → сменить на «отыгрыш»", callback_data="duo:amode")])
            if not recover:
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
    rows.append([InlineKeyboardButton("🧹 Фильтры входа", callback_data="duo:flt"),
                 InlineKeyboardButton("🏦 Банки монет", callback_data="duo:banks")])
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="duo:menu")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def filter_split_lines(since_ts: int = 0) -> list[str]:
    """Виртуальные сделки по признакам фильтров — чтобы по своим данным
    видеть, какие входы хуже."""
    return [_st_short(name, st) for name, st in duo.feature_split(since_ts) if st["trades"]]


def filters_text() -> str:
    min_left, max_spread = duo.entry_filters()
    lines = [
        "🧹 Фильтры входа стратегий 1️⃣ и 2️⃣",
        "",
        f"Сейчас: {duo.filters_text()}.",
        "Решение — один раз на рынок, в момент первого сигнала. Отсеянный рынок стратегии пропускают, "
        "а виртуальная сделка пишется всё равно — по ней пауза и статистика ниже.",
        "",
        "⏱ Не позже чем за N минут до конца. Поздние входы проигрывали чаще, чем закладывает цена: "
        "k=1.22 на архиве 18.09–12.10 и 1.80 за 13–15.10.",
        "↔️ Спред — ask фаворита + ask другой стороны − 1. На архиве входы при спреде шире 1¢ были хуже "
        "(k=1.21), за 13–15.10 — нет (0.97), поэтому по умолчанию выкл: копим данные.",
        "",
        "По виртуальным сделкам (все сигналы, k — проигрышей к заложенному в цены):",
    ]
    split = filter_split_lines()
    lines += ["  " + x for x in split] if split else ["  пока нет закрытых виртуальных сделок с признаками"]
    return "\n".join(lines)


def filters_markup() -> InlineKeyboardMarkup:
    min_left, max_spread = duo.entry_filters()
    t_row = [InlineKeyboardButton(("✅ " if abs(v - min_left) < 1e-9 else "") + (f"⏱ {v:g} мин" if v else "⏱ выкл"),
                                  callback_data=f"duo:fm:{v:g}") for v in runtime_state.ENTRY_MIN_LEFT_PRESETS]
    s_row = [InlineKeyboardButton(("✅ " if abs(v - max_spread) < 1e-9 else "") + (f"↔️ ≤ {v * 100:g}¢" if v else "↔️ выкл"),
                                  callback_data=f"duo:fs:{v:g}") for v in runtime_state.ENTRY_SPREAD_PRESETS]
    return InlineKeyboardMarkup([t_row[:3], t_row[3:], s_row,
                                 [InlineKeyboardButton("🔄 Обновить", callback_data="duo:flt")],
                                 [InlineKeyboardButton("◀️ Назад", callback_data="duo:menu")]])


def banks_text() -> str:
    lines = [
        "🏦 Банк у каждой монеты свой: старт + итог закрытых сделок стратегий 1️⃣ и 2️⃣ этой монеты "
        "(реальные и тестовые — отдельно).",
        "От него считается ставка «% банка» (💰 / ⚙️ монеты). Никакая ставка, и отыгрыш 1️⃣ тоже, не больше "
        "банка: кончился — входов по монете нет, пока не поднимешь старт.",
        "",
    ]
    for a in settings.ASSETS:
        off = "" if runtime_state.is_asset_enabled(a) else " (монета выключена)"
        lines.append(f"{a.upper()}: старт ${runtime_state.coin_bank_start(a):.2f} · сейчас реальный "
                     f"${duo.coin_bank(a, False):.2f}, тестовый ${duo.coin_bank(a, True):.2f}{off}")
    return "\n".join(lines)


def banks_markup() -> InlineKeyboardMarkup:
    rows, row = [], []
    for a in settings.ASSETS:
        row.append(InlineKeyboardButton(f"{a.upper()}: ${runtime_state.coin_bank_start(a):g} ✏️",
                                        callback_data=f"duo:bk:{a}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="duo:menu")])
    return InlineKeyboardMarkup(rows)


def bank_pick_markup(asset: str) -> InlineKeyboardMarkup:
    cur = runtime_state.coin_bank_start(asset)
    nums = [InlineKeyboardButton(("✅ " if abs(v - cur) < 1e-9 else "") + f"${v}", callback_data=f"duo:bs:{asset}:{v}")
            for v in runtime_state.COIN_BANK_PRESETS]
    return InlineKeyboardMarkup([nums,
                                 [InlineKeyboardButton("✏️ Своя сумма", callback_data=f"duo:bi:{asset}")],
                                 [InlineKeyboardButton("◀️ Назад", callback_data="duo:banks")]])


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
    if cmd == "amode":
        runtime_state.set("strat_a_mode", "series" if duo.a_mode() == "recover" else "recover")
        duo.reset_a()       # серии и несыгранные минусы — с чистого листа
        return menu_text(), menu_markup(), None
    if cmd == "banks":
        return banks_text(), banks_markup(), None
    if cmd == "flt":
        return filters_text(), filters_markup(), None
    if cmd in ("fm", "fs") and len(parts) == 3:
        try:
            value = float(parts[2])
        except ValueError:
            return filters_text(), filters_markup(), None
        key, presets = (("entry_min_minutes_left", runtime_state.ENTRY_MIN_LEFT_PRESETS) if cmd == "fm"
                        else ("entry_max_spread", runtime_state.ENTRY_SPREAD_PRESETS))
        if any(abs(value - v) < 1e-9 for v in presets):
            runtime_state.set(key, value)
        return filters_text(), filters_markup(), None
    if cmd in ("bk", "bs", "bi") and len(parts) >= 3 and parts[2] in settings.ASSETS:
        asset = parts[2]
        if cmd == "bk":
            return (f"🏦 {asset.upper()}: стартовый банк ${runtime_state.coin_bank_start(asset):.2f}. Сколько поставить?",
                    bank_pick_markup(asset), None)
        if cmd == "bi":
            return (f"✏️ Напиши следующим сообщением стартовый банк {asset.upper()} в USDC, например 120",
                    InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"duo:bk:{asset}")]]),
                    f"duo_bank:{asset}")
        try:
            value = float(parts[3])
        except (IndexError, ValueError):
            return banks_text(), banks_markup(), None
        if not math.isfinite(value) or value < 0:
            return banks_text(), banks_markup(), None
        runtime_state.set_coin_bank_start(asset, value)
        duo.invalidate_banks()
        return "✅ " + banks_text(), banks_markup(), None
    if cmd == "live" and len(parts) == 3 and parts[2] in duo.STRATS:
        strat = parts[2]
        key = "strat_a_live" if strat == duo.A else "strat_b_live"
        runtime_state.set(key, not runtime_state.get(key))
        head = ""
        if runtime_state.get(key):
            head = f"✅ {duo.label(strat)}: реальные входы по монетам: {_live_coins_text()}."
            if strat == duo.A and duo.a_mode() == "recover":
                head += (" ⚠️ Ставка отыгрыша в 11–20 раз больше минуса 2️⃣ (минус $5 → ≈$54–102, не больше "
                         "банка монеты 🏦). Реальными деньгами отыгрываются только реальные минусы 2️⃣.")
            head += "\n\n"
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
    """Текст после «✏️ Своё число» / «✏️ Своя сумма». Возвращает (ответ,
    кнопки) или None, если ввод не наш. Ошибку ввода — (текст ошибки, None)."""
    if pending.startswith("duo_bank:"):
        asset = pending.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return "Монета не найдена.", None
        try:
            value = float(raw.strip().replace(",", ".").replace("$", ""))
        except ValueError:
            return "Нужна сумма в USDC, например 120.", None
        if not math.isfinite(value) or value < 0 or value > 1_000_000:
            return "Нужна сумма в USDC, например 120.", None
        runtime_state.set_coin_bank_start(asset, value)
        duo.invalidate_banks()
        return "✅ " + banks_text(), banks_markup()
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
    stats = {}
    for label, live, strat in (("👻 Виртуальные", False, duo.VIRT),
                               ("1️⃣ реально", True, duo.A), ("1️⃣ тест", False, duo.A),
                               ("2️⃣ реально", True, duo.B), ("2️⃣ тест", False, duo.B)):
        st = storage.get_outcome_stats(live=live, strategy=strat)
        stats[(strat, live)] = st
        if not st["trades"] and strat != duo.VIRT:
            continue
        lines.extend(loss_stats.summary_lines(label, st))
        if st["trades"]:
            lines.append(f"  PnL {st['pnl_usdc']:+.2f} USDC" + ("" if live else " (виртуально)"))
    lines += _combined_lines(stats)
    lines += ["", "*По монетам* (виртуальные — так стратегия видит рынок):"]
    for a in settings.ASSETS:
        st = storage.get_outcome_stats(live=False, asset=a, strategy=duo.VIRT)
        if st["trades"]:
            lines.append(f"  {_st_short(a.upper(), st)}")
    split = filter_split_lines()
    if split:
        lines += ["", f"*Фильтры входа* ({duo.filters_text()}) — виртуальные по признакам:"]
        lines += ["  " + x for x in split]
    real = storage.get_pnl_summary(0, live_only=True)
    lines += ["", f"Реальные деньги всего: {loss_stats.plural_trades(real['trades'])}, PnL {real['pnl_usdc']:+.2f} USDC",
              "Все сделки одним файлом: /export (колонки strategy, filtered и признаки входа: "
              "minutes_left, spread, distance_pct…)"]
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
    out += ["  " + x for x in _combined_lines({(s, not dry): {"trades": b["trades"], "pnl_usdc": b["pnl_usdc"]}
                                               for (s, dry), b in summ.items()})]
    out.append(duo.summary_line())
    cmp_line = duo.compare_pause_text(since_ts)
    if cmp_line:
        out.append(cmp_line)
    split = filter_split_lines(since_ts)
    if split:
        out.append(f"🧹 Фильтры ({duo.filters_text()}), виртуальные за период:")
        out += ["  " + x for x in split]
    early = duo.early_stats_text()
    if early:
        out.append(early)
    return out
