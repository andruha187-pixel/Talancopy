"""
Телеграм-бот: кнопочное меню управления + пуш-уведомления о сделках.

Меню:
  ▶️/⏸ Старт-стоп | 💰 Размер позиции | 🛑 Стоп-лосс | 📊 Статистика
  ⚙️ Настройки (safety score) | 🧪/🔴 режим DRY RUN / LIVE (с подтверждением)

Все изменения пишутся в runtime_state (который сам сохраняет их в SQLite),
так что настройки переживают рестарт процесса.
"""
from __future__ import annotations
import time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

from config import settings
from src import storage, runtime_state, loss_stats, diagnostics

_app: Application | None = None
_state_ref: dict = {}  # заполняется из main.py: последний сигнал/статус для меню
_pending_input: str | None = None  # "size" | "stoploss" | None — ждём ли текстовый ввод числа

SIZE_PRESETS = [5, 10, 20, 50, 100]
STOPLOSS_PRESETS = [20, 50, 100, 200]
POSITION_SL_PRESETS = [20, 30, 50, 70]
SCORE_PRESETS = [75, 85, 88, 92]
# Мин. расстояние от страйка, % от цены (0 = выкл). Для BTC ~84k: 0.05% ≈ $42, 0.07% ≈ $59, 0.10% ≈ $84.
DISTANCE_PRESETS = [0.0, 0.05, 0.07, 0.10]
# Окно входа (минут до конца рынка). 1–4.5 — рекомендованное по отчётам 25.09–08.10.
WINDOW_PRESETS = [(1.0, 3.5), (1.0, 4.0), (1.0, 4.5)]
MAX_POS_PRESETS = [2, 4, 6, 8]   # бот двух стратегий: по умолчанию 8 (4 монеты × 2 стратегии)


def set_state_ref(state: dict) -> None:
    global _state_ref
    _state_ref = state


# ---------------------------------------------------------------- меню ----

def _fmt_pct(pct: float) -> str:
    return f"{pct:g}%"


def _size_summary() -> str:
    """Общая ставка — для монет без своей (⚙️ в 🪙 Активах)."""
    mode, usdc, pct = runtime_state.sizing(None)
    if mode == "percent":
        size_now = runtime_state.compute_trade_size()
        return f"{_fmt_pct(pct)} банка (сейчас {size_now:.2f} USDC)"
    return f"{usdc:.2f} USDC (фикс.)"


def _sizing_text(asset: str | None = None) -> str:
    """'10% банка ≈ 37.40 USDC' или '20.00 USDC фикс.' — ставка монеты (своя
    или общая); asset=None — общая."""
    mode, usdc, pct = runtime_state.sizing(asset)
    if mode == "percent":
        now = runtime_state.compute_trade_size(asset, max_bank_age=10)
        return f"{_fmt_pct(pct)} банка ≈ {now:.2f} USDC"
    return f"{usdc:.2f} USDC фикс."


def _sizing_short(asset: str | None = None) -> str:
    mode, usdc, pct = runtime_state.sizing(asset)
    return f"{_fmt_pct(pct)} банка" if mode == "percent" else f"{usdc:g} USDC"


def _range_text(asset: str | None = None) -> str:
    lo, hi = runtime_state.entry_range(asset)
    return f"{lo:.2f}–{hi:.2f}"


def _coin_mode_badge(asset: str) -> str:
    """Режим монеты: 🔴 LIVE, 🔴 LIVE ⏳ (помечена LIVE, но весь бот в DRY RUN —
    реальных сделок пока нет) или 🧪 DRY."""
    if runtime_state.is_asset_live(asset):
        return "🔴 LIVE ⏳" if runtime_state.get("dry_run") else "🔴 LIVE"
    return "🧪 DRY"


def _coin_settings_short(asset: str) -> str:
    """'вход 0.92–0.95 (свой) · ставка 10% банка ≈ 37.40 USDC'"""
    rng = f"вход {_range_text(asset)}" + (" (свой)" if runtime_state.has_own_range(asset) else "")
    size = f"ставка {_sizing_text(asset)}" + (" (своя)" if runtime_state.has_own_sizing(asset) else "")
    return f"{rng} · {size}"


def _overrides_summary() -> str:
    """'SOL: вход 0.92–0.95; BTC: ставка 10% банка ≈ 37.40 USDC' — только монеты
    со своими настройками; пустая строка, если таких нет."""
    parts = []
    for a in runtime_state.assets_with_own_settings():
        bits = []
        if runtime_state.has_own_range(a):
            bits.append(f"вход {_range_text(a)}")
        if runtime_state.has_own_sizing(a):
            bits.append(f"ставка {_sizing_text(a)}")
        parts.append(f"{a.upper()}: {', '.join(bits)}")
    return "; ".join(parts)


def _own_list(kind: str) -> str:
    """'BTC (10% банка), SOL (5 USDC)' — монеты со своей ставкой ('size') или
    своим диапазоном ('range')."""
    if kind == "size":
        return ", ".join(f"{a.upper()} ({_sizing_short(a)})" for a in settings.ASSETS
                         if runtime_state.has_own_sizing(a))
    return ", ".join(f"{a.upper()} ({_range_text(a)})" for a in settings.ASSETS
                     if runtime_state.has_own_range(a))


def _live_pending_note() -> str:
    """Монеты помечены 🔴 LIVE, но весь бот в DRY RUN: реальных сделок нет,
    пока не нажата «🔴 Включить LIVE» в главном меню."""
    if not runtime_state.get("dry_run"):
        return ""
    marked = [a.upper() for a in settings.ASSETS
              if runtime_state.is_asset_live(a) and runtime_state.is_asset_enabled(a)]
    if not marked:
        return ""
    verb = "помечен" if len(marked) == 1 else "помечены"
    return (f"⏳ {', '.join(marked)} {verb} 🔴 LIVE, но бот в DRY RUN: реальные сделки начнутся "
            "после «🔴 Включить LIVE» внизу главного меню.")


def _distance_summary() -> str:
    pct = runtime_state.get("min_distance_pct") or 0.0
    if pct <= 0:
        return "выкл"
    return f"{pct:.2f}% (≈${pct / 100 * 84000:.0f} для BTC)"


def _settings_text() -> str:
    scaling_on = runtime_state.get("size_scaling_enabled")
    scaling_line = (
        f"Размер ставки масштабируется от порога: на пограничном score — "
        f"{settings.SIZE_SCALING_MIN_FRACTION*100:.0f}% от размера позиции, "
        f"на score {settings.SIZE_SCALING_MAX_SCORE:.0f}+ — полный размер."
        if scaling_on else
        "Масштабирование выключено — любая прошедшая порог сделка идёт полным размером."
    )
    lo, hi = runtime_state.entry_window()
    return (
        f"⚙️ Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}\n"
        "Чем выше — тем реже и осторожнее входы.\n\n"
        f"⏱ Окно входа: за {lo:g}–{hi:g} мин до конца рынка.\n"
        "Шире окно — больше входов. 1–4.5 вместо 1–3.5 на истории 25.09–06.10 дало "
        "28 входов в день вместо 23 и +40% прибыли на ту же ставку; на новых данных "
        "06–08.10 добавочные входы были без проигрышей.\n\n"
        f"📦 Реальных позиций одновременно: до {runtime_state.get('max_open_positions')}\n"
        "Считаются только позиции в ещё идущих рынках. Монеты двигаются вместе, "
        "поэтому больше позиций — больше проигрышей разом в плохие 5 минут.\n\n"
        f"📏 Фильтр расстояния: {'вкл' if runtime_state.get('distance_filter_enabled') else 'выкл'}\n"
        "Выкл — логика «сломанного» бота 12–14.10: вход по фавориту без проверки расстояния от "
        "страйка (1.5 ATR и минимум в %). Вкл — как в основном боте.\n\n"
        f"📏 Мин. расстояние от страйка: {_distance_summary()}"
        + ("" if runtime_state.get("distance_filter_enabled") else " (не действует, пока фильтр выкл)") + "\n"
        "Не входить, если цена ближе к страйку, чем этот % от цены.\n\n"
        f"⚡ Быстрый вход: {'вкл' if runtime_state.get('fast_entry_enabled') else 'выкл'}\n"
        f"Кроме проверки раз в 3 с бот раз в {max(0.1, settings.FAST_LOOP_INTERVAL_SEC):g} с проверяет сигнал "
        "по цене Binance из потока и живому стакану — те же условия входа, без запросов. "
        "У трети рынков с сигналом вход держался не дольше одной 3-секундной проверки; "
        "по отчётам это примерно +10% входов.\n\n"
        f"📡 Цена сигнала: {'Chainlink' if runtime_state.get('price_source') == 'chainlink' else 'Binance'}\n"
        "Chainlink — цена, по которой Polymarket решает исход: и текущая цена, и страйк "
        "(цена на начало окна) берутся из неё, Binance остаётся для ATR и тренда. Нет свежей "
        "цены Chainlink или бот не застал начало окна — входа нет. Binance — как было до 12.10.\n\n"
        f"⏱ Подтверждение сигнала: {'вкл' if runtime_state.get('entry_confirm_enabled') else 'выкл'}\n"
        f"Вход, только если сигнал был и {max(0.0, settings.ENTRY_CONFIRM_SEC):g} с назад, в ту же сторону. "
        "Все 4 проигрыша 11.10 были на сигнале, который не продержался и 3 с. "
        "Архив 25.09–11.10: проигрышей 1 вместо 7, прибыль +13%, входов на 37% меньше.\n\n"
        + scaling_line
    )


_MODE_ICON = {"live": "🔴", "dry": "🧪", "off": "⏸"}


def _assets_modes_line() -> str:
    """'BTC 🔴, ETH 🧪, SOL 🧪' — включённые монеты с режимом: 🔴 реальные
    сделки, 🧪 виртуальные (DRY)."""
    return ", ".join(
        f"{a.upper()} {_MODE_ICON[runtime_state.asset_mode(a)]}"
        for a in settings.ASSETS if runtime_state.is_asset_enabled(a)
    )


def _fast_status_line() -> str:
    """Состояние быстрой проверки и потока цены Binance (src/fast_signal.py)."""
    try:
        from src import fast_signal  # здесь, а не наверху: fast_signal -> executor -> telegram_notify
        return fast_signal.status_line()
    except Exception:  # noqa: BLE001 — строка статуса не должна ломать меню
        return "⚡ Быстрый вход: " + ("вкл" if runtime_state.get("fast_entry_enabled") else "выкл")


def _price_status_line() -> str:
    """Источник цены сигнала и состояние потока Chainlink (src/chainlink_feed.py)."""
    try:
        from src import chainlink_feed
        return chainlink_feed.status_line()
    except Exception:  # noqa: BLE001 — строка статуса не должна ломать меню
        return "📡 Цена сигнала: " + ("Chainlink" if runtime_state.get("price_source") == "chainlink" else "Binance")


def _confirm_status_line() -> str:
    """Подтверждение сигнала (src/entry_confirm.py): вкл/выкл и сколько отсеяно."""
    try:
        from src import entry_confirm  # здесь: единообразно с быстрой проверкой
        return entry_confirm.status_line()
    except Exception:  # noqa: BLE001 — строка статуса не должна ломать меню
        return "⏱ Подтверждение сигнала: " + ("вкл" if runtime_state.get("entry_confirm_enabled") else "выкл")


def _main_menu_text() -> str:
    s = _state_ref  # dict: "asset:timeframe" -> instance state
    paused = runtime_state.get("paused")
    dry_run = runtime_state.get("dry_run")
    pos_sl_on = runtime_state.get("position_stop_loss_enabled")
    enabled_assets = runtime_state.get_enabled_assets()
    lines = [
        "🤖 *Polymarket Multi-Asset Bot*",
        "",
        f"Статус: {'⏸ на паузе' if paused else '▶️ активен'} | Режим: {'🧪 DRY RUN' if dry_run else '🔴 LIVE'}",
    ]
    pending = _live_pending_note()
    if pending:
        lines.append(pending)
    lines += [
        f"Активы: {_assets_modes_line() or '(нет включённых)'}",
        f"Размер позиции: {_size_summary()}",
        f"Стоп-лосс/день: {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC",
        f"Стоп-лосс позиции: {'вкл ' + str(round(runtime_state.get('position_stop_loss_pct'))) + '%' if pos_sl_on else 'выкл'}",
        f"Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}",
        f"Диапазон входа: {_range_text()}",
        f"Окно входа: за {runtime_state.entry_window()[0]:g}–{runtime_state.entry_window()[1]:g} мин до конца | "
        f"позиций до {runtime_state.get('max_open_positions')}",
        (f"Мин. расстояние от страйка: {_distance_summary()}" if runtime_state.get("distance_filter_enabled")
         else "📏 Фильтр расстояния: выкл — логика сломанного бота"),
    ]
    if _duo_on():
        from src import duo  # здесь: duo -> chainlink_feed, без циклов при старте
        lines.append(duo.summary_line())
    own = _overrides_summary()
    if own:
        lines.append(f"⚙️ Свои у монет: {own}")
    lines.append(_fast_status_line())
    lines.append(_confirm_status_line())
    lines.append(_price_status_line())
    if s:
        lines.append("")
        lines.append(f"Потоков активно: {len(s)}")
        # Сортируем по активу, потом по таймфрейму — стабильный порядок в UI
        for key in sorted(s.keys()):
            inst = s[key]
            lines.append(
                f"  {inst['asset'].upper()} {inst['timeframe']}: {inst.get('direction','—')} "
                f"score {inst.get('safety_score','—')}"
            )
    return "\n".join(lines)


def _duo_on() -> bool:
    """Бот двух стратегий (src/duo.py) включён."""
    return bool(runtime_state._state.get("strategies_mode", False))


def _main_menu_markup() -> InlineKeyboardMarkup:
    paused = runtime_state.get("paused")
    rows = [
        [InlineKeyboardButton("▶️ Старт" if paused else "⏸ Стоп", callback_data="pause_toggle")],
    ]
    if _duo_on():
        rows.append([InlineKeyboardButton("🎯 Стратегии", callback_data="duo:menu")])
    rows += [
        [InlineKeyboardButton("🪙 Активы", callback_data="menu:assets")],
        [
            InlineKeyboardButton("💰 Размер позиции", callback_data="menu:size"),
            InlineKeyboardButton("🛑 Стоп-лосс/день", callback_data="menu:sl"),
        ],
        [
            InlineKeyboardButton("📉 Стоп-лосс позиции", callback_data="menu:possl"),
            InlineKeyboardButton("📈 Диапазон входа", callback_data="menu:range"),
        ],
        [InlineKeyboardButton("🐋 Слежка за кошельком", callback_data="menu:wallet")],
        [InlineKeyboardButton("🔒 Хедж-бот", callback_data="menu:hedge")],
        [
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
            InlineKeyboardButton("⚙️ Настройки", callback_data="menu:settings"),
        ],
        [InlineKeyboardButton("🔎 Почему нет сделок", callback_data="why")],
        [InlineKeyboardButton(
            "🔴 Включить LIVE" if runtime_state.get("dry_run") else "🧪 Переключить в DRY RUN",
            callback_data="mode_toggle",
        )],
    ]
    return InlineKeyboardMarkup(rows)


def _assets_menu_markup() -> InlineKeyboardMarkup:
    """Строка на монету: [✅/⏸ МОНЕТА] — включить/выключить поток,
    [🔴 LIVE / 🧪 DRY] — реальные или виртуальные сделки по этой монете,
    [⚙️] — свой диапазон входа и своя ставка монеты."""
    enabled = runtime_state.get_enabled_assets()
    rows = []
    for asset in settings.ASSETS:
        on = asset in enabled
        own = runtime_state.has_own_range(asset) or runtime_state.has_own_sizing(asset)
        rows.append([
            InlineKeyboardButton(f"{'✅' if on else '⏸'} {asset.upper()}", callback_data=f"asset_toggle:{asset}"),
            InlineKeyboardButton(_coin_mode_badge(asset), callback_data=f"asset_mode:{asset}"),
            InlineKeyboardButton("⚙️ свои" if own else "⚙️", callback_data=f"acfg:{asset}"),
        ])
    notify_on = runtime_state.get("notify_dry_assets")
    rows.append([InlineKeyboardButton(
        "🔔 Уведомления DRY-монет: вкл" if notify_on else "🔕 Уведомления DRY-монет: выкл",
        callback_data="dry_notify_toggle",
    )])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _assets_menu_text() -> str:
    enabled = runtime_state.get_enabled_assets()
    general = "🧪 DRY RUN — все сделки виртуальные" if runtime_state.get("dry_run") else "🔴 LIVE"
    lines = [f"🪙 Монеты: включено {len(enabled)} из {len(settings.ASSETS)}. Общий режим бота: {general}."]
    pending = _live_pending_note()
    if pending:
        lines.append(pending)
    coin_lines = [f"{a.upper()} {_coin_mode_badge(a)}: {_coin_settings_short(a)}"
                  for a in settings.ASSETS if a in enabled]
    if coin_lines:
        lines += [""] + coin_lines
    mode_line = (
        "• посередине — 🔴 LIVE: стратегиям 🎯 можно входить по монете реальными деньгами (если "
        "стратегия в «💵 реально» и бот в LIVE, иначе ⏳); 🧪 DRY — по монете только виртуальные "
        "и тестовые входы;"
        if _duo_on() else
        "• посередине — режим: 🔴 LIVE — реальные сделки (только когда сам бот в LIVE, "
        "иначе ⏳), 🧪 DRY — виртуальные: стратегия проверяется без денег;"
    )
    lines += [
        "",
        "Кнопки в строке монеты:",
        "• слева — включить/выключить монету;",
        mode_line,
        "• ⚙️ — свой диапазон входа и своя ставка монеты (% банка или фикс. сумма). "
        "«⚙️ свои» — у монеты уже есть свои значения; без них она берёт общие из главного меню.",
        "",
        "Новые монеты всегда начинают с DRY. Каждый тик по каждой включённой монете "
        "попадает в 4-часовой отчёт — по нему видно, на какой монете стратегия работает.",
    ]
    return "\n".join(lines)


# ----------------------------------------------- свои настройки монеты ----

def _asset_cfg_text(asset: str) -> str:
    a = asset.upper()
    own_r = runtime_state.has_own_range(asset)
    own_s = runtime_state.has_own_sizing(asset)
    mode = _coin_mode_badge(asset)
    if not runtime_state.is_asset_enabled(asset):
        mode += " (монета выключена ⏸)"
    lines = [f"⚙️ {a} — настройки монеты", "", f"Режим: {mode}"]
    if runtime_state.is_asset_live(asset) and runtime_state.get("dry_run"):
        lines.append("⏳ Бот в DRY RUN — реальные сделки по монете начнутся после «🔴 Включить LIVE» "
                     "в главном меню.")
    lines += [
        f"📈 Диапазон входа: {_range_text(asset)} — {'свой' if own_r else 'общий'}",
        f"💰 Ставка: {_sizing_text(asset)} — {'своя' if own_s else 'общая'}",
        "",
        "Общий/общая — значение из главного меню, меняется вместе с ним.",
        "Свой/своя — только для этой монеты, общие настройки на него не влияют.",
    ]
    return "\n".join(lines)


def _asset_cfg_markup(asset: str) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton("📈 Диапазон входа", callback_data=f"arng:{asset}"),
        InlineKeyboardButton("💰 Ставка", callback_data=f"asz:{asset}"),
    ]]
    if runtime_state.has_own_range(asset) or runtime_state.has_own_sizing(asset):
        rows.append([InlineKeyboardButton("↩️ Всё как в общих", callback_data=f"arst:{asset}")])
    rows.append([InlineKeyboardButton("◀️ К монетам", callback_data="menu:assets")])
    return InlineKeyboardMarkup(rows)


def _asset_range_text(asset: str) -> str:
    a = asset.upper()
    own = runtime_state.has_own_range(asset)
    lines = [f"📈 {a} — диапазон входа: {_range_text(asset)} ({'свой' if own else 'общий'})"]
    if own:
        lines.append(f"Общий: {_range_text(None)}.")
    else:
        lines.append("Сейчас монета берёт общий. Любая кнопка ниже создаст ей свой диапазон, "
                     "начиная с общего.")
    lines += [
        "",
        f"Бот входит по {a}, только если ask нужной стороны внутри диапазона. Максимум — это и "
        f"потолок цены ордера; дороже {runtime_state.max_entry_cap():.2f} бот не покупает.",
    ]
    return "\n".join(lines)


def _asset_range_markup(asset: str) -> InlineKeyboardMarkup:
    lo, hi = runtime_state.entry_range(asset)
    cap = runtime_state.max_entry_cap()
    rows = [[InlineKeyboardButton("— Минимум —", callback_data="noop")]]
    rows.append([
        InlineKeyboardButton(f"{'✅ ' if abs(v - lo) < 0.001 else ''}{v:.2f}", callback_data=f"amin:{asset}:{v}")
        for v in MIN_ENTRY_PRESETS
    ])
    rows.append([
        InlineKeyboardButton("−0.01", callback_data=f"amind:{asset}:-0.01"),
        InlineKeyboardButton("+0.01", callback_data=f"amind:{asset}:0.01"),
        InlineKeyboardButton("✏️ Свой", callback_data=f"aminc:{asset}"),
    ])
    rows.append([InlineKeyboardButton("— Максимум —", callback_data="noop")])
    rows.append([
        InlineKeyboardButton(f"{'✅ ' if abs(v - hi) < 0.001 else ''}{v:.2f}", callback_data=f"amax:{asset}:{v}")
        for v in MAX_ENTRY_PRESETS if v <= cap + 1e-9
    ])
    rows.append([
        InlineKeyboardButton("−0.01", callback_data=f"amaxd:{asset}:-0.01"),
        InlineKeyboardButton("+0.01", callback_data=f"amaxd:{asset}:0.01"),
        InlineKeyboardButton("✏️ Свой", callback_data=f"amaxc:{asset}"),
    ])
    if runtime_state.has_own_range(asset):
        rows.append([InlineKeyboardButton(f"↩️ Как в общих ({_range_text(None)})", callback_data=f"arngr:{asset}")])
    rows.append([InlineKeyboardButton(f"◀️ Назад к {asset.upper()}", callback_data=f"acfg:{asset}")])
    return InlineKeyboardMarkup(rows)


def _asset_size_text(asset: str) -> str:
    a = asset.upper()
    own = runtime_state.has_own_sizing(asset)
    mode, _usdc, _pct = runtime_state.sizing(asset)
    lines = [f"💰 {a} — ставка: {_sizing_text(asset)} ({'своя' if own else 'общая'})"]
    if own:
        lines.append(f"Общая: {_sizing_text(None)}.")
    else:
        lines.append("Сейчас монета берёт общую. Любая кнопка ниже создаст ей свою ставку, "
                     "начиная с общей.")
    if mode == "percent":
        bank = runtime_state.current_bankroll()
        lines += [
            "",
            f"Банк один на все монеты: {bank:.2f} USDC (стартовый "
            f"{runtime_state.get('starting_bankroll_usdc'):.2f} + реализованный PnL реальных сделок). "
            "Стартовый банк меняется в 💰 Размер позиции главного меню.",
        ]
    if runtime_state.is_asset_live(asset) and runtime_state.get("dry_run"):
        lines += ["", "Бот сейчас в DRY RUN: пока ставка влияет только на виртуальный PnL. "
                      "После «🔴 Включить LIVE» по монете пойдут реальные ордера этого размера."]
    elif runtime_state.trade_is_dry(asset):
        lines += ["", "Монета в DRY: ставка влияет только на виртуальный PnL."]
    return "\n".join(lines)


def _asset_size_markup(asset: str) -> InlineKeyboardMarkup:
    mode, usdc, pct = runtime_state.sizing(asset)
    rows = [[InlineKeyboardButton(
        "🔀 Перейти на фикс. сумму" if mode == "percent" else "🔀 Перейти на % от банка",
        callback_data=f"aszm:{asset}",
    )]]
    if mode == "percent":
        rows.append([
            InlineKeyboardButton(f"{'✅ ' if abs(v - pct) < 0.01 else ''}{v}%", callback_data=f"aszp:{asset}:{v}")
            for v in BANKROLL_PCT_PRESETS
        ])
        rows.append([
            InlineKeyboardButton("−1%", callback_data=f"aszpd:{asset}:-1"),
            InlineKeyboardButton("+1%", callback_data=f"aszpd:{asset}:1"),
            InlineKeyboardButton("✏️ Свой %", callback_data=f"aszpc:{asset}"),
        ])
    else:
        buttons = [
            InlineKeyboardButton(f"{'✅ ' if abs(v - usdc) < 0.01 else ''}{v}", callback_data=f"aszu:{asset}:{v}")
            for v in SIZE_PRESETS
        ]
        rows += [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
        rows.append([InlineKeyboardButton("✏️ Своя сумма", callback_data=f"aszuc:{asset}")])
    if runtime_state.has_own_sizing(asset):
        rows.append([InlineKeyboardButton(f"↩️ Как в общих ({_sizing_short(None)})", callback_data=f"aszr:{asset}")])
    rows.append([InlineKeyboardButton(f"◀️ Назад к {asset.upper()}", callback_data=f"acfg:{asset}")])
    return InlineKeyboardMarkup(rows)


def _confirm_live_text() -> str:
    """Подтверждение общего LIVE: по каким монетам и с какой ставкой пойдут
    реальные ордера — чтобы не включить LIVE со сброшенной ставкой."""
    live = [a for a in settings.ASSETS if runtime_state.is_asset_live(a) and runtime_state.is_asset_enabled(a)]
    lines = ["⚠️ Включить LIVE-режим? Бот начнёт выставлять реальные ордера на Polymarket.", ""]
    if live:
        lines.append("Реальные сделки пойдут по:")
        lines += [f"  {a.upper()}: {_coin_settings_short(a)}" for a in live]
        dry = [a.upper() for a in settings.ASSETS if runtime_state.is_asset_enabled(a) and a not in live]
        if dry:
            lines.append(f"Остальные ({', '.join(dry)}) останутся в DRY — виртуально.")
        lines += ["", "Проверь ставку: поменять её можно в ⚙️ монеты (🪙 Активы) или в 💰 Размер позиции."]
    else:
        lines.append("Ни одна включённая монета не помечена 🔴 LIVE в 🪙 Активах — реальных сделок "
                     "не будет, пока не пометишь.")
    return "\n".join(lines)


BANKROLL_PCT_PRESETS = [3, 5, 7, 10]
COPYTRADE_SIZE_PRESETS = [2, 5, 10, 20]


HEDGE_STAKE_PRESETS = [2, 5, 10, 20]
HEDGE_TRIGGER_PRESETS = [0.85, 0.88, 0.90, 0.93]


def _hedge_menu_markup() -> InlineKeyboardMarkup:
    enabled = runtime_state.get("hedge_bot_enabled")
    entry = runtime_state.get("hedge_entry_price")
    trigger = runtime_state.get("hedge_trigger_price")
    stake = runtime_state.get("hedge_stake_usdc")

    rows = [
        [InlineKeyboardButton(
            "🔴 Выключить хедж-бота" if enabled else "🟢 Включить хедж-бота",
            callback_data="hedge_toggle",
        )],
        [InlineKeyboardButton("— Порог хеджа (сейчас {:.2f}) —".format(trigger), callback_data="noop")],
    ]
    row = []
    for val in HEDGE_TRIGGER_PRESETS:
        mark = "✅ " if abs(val - trigger) < 0.001 else ""
        row.append(InlineKeyboardButton(f"{mark}{val:.2f}", callback_data=f"hedgetrigger_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("— Размер ставки —", callback_data="noop")])
    row = []
    for val in HEDGE_STAKE_PRESETS:
        mark = "✅ " if abs(val - stake) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"hedgestake_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Свой размер ставки", callback_data="hedgestake_custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _wallet_menu_markup() -> InlineKeyboardMarkup:
    notify_on = runtime_state.get("wallet_notify_enabled")
    copy_on = runtime_state.get("wallet_copytrade_enabled")
    size = runtime_state.get("copytrade_size_usdc")

    rows = [
        [InlineKeyboardButton(
            "🔔 Уведомления: выкл" if not notify_on else "🔕 Уведомления: вкл",
            callback_data="wallet_notify_toggle",
        )],
        [InlineKeyboardButton(
            "🟢 Включить копитрейдинг" if not copy_on else "🔴 Выключить копитрейдинг",
            callback_data="wallet_copytrade_toggle",
        )],
        [InlineKeyboardButton("— Размер копи-сделки —", callback_data="noop")],
    ]
    row = []
    for val in COPYTRADE_SIZE_PRESETS:
        mark = "✅ " if abs(val - size) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"copysize_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Свой размер копи-сделки", callback_data="copysize_custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _size_menu_markup() -> InlineKeyboardMarkup:
    mode = runtime_state.get("sizing_mode")
    # Подпись — куда переключит кнопка (раньше «Режим: % от банка» читалось
    # как текущий режим, хотя он был фиксированным).
    rows = [[InlineKeyboardButton(
        "🔀 Перейти на % от банка" if mode == "fixed" else "🔀 Перейти на фикс. сумму",
        callback_data="sizing_mode_toggle",
    )]]

    if mode == "percent":
        row = []
        for val in BANKROLL_PCT_PRESETS:
            mark = "✅ " if abs(val - runtime_state.get("bankroll_pct")) < 0.01 else ""
            row.append(InlineKeyboardButton(f"{mark}{val}%", callback_data=f"bankrollpct_set:{val}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([
            InlineKeyboardButton("−1%", callback_data="bankrollpct_delta:-1"),
            InlineKeyboardButton("+1%", callback_data="bankrollpct_delta:+1"),
        ])
        rows.append([InlineKeyboardButton("✏️ Свой стартовый банк", callback_data="startbank_custom")])
    else:
        current = runtime_state.get("trade_size_usdc")
        row = []
        for val in SIZE_PRESETS:
            mark = "✅ " if abs(val - current) < 0.01 else ""
            row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"size_set:{val}"))
            if len(row) == 3:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([InlineKeyboardButton("✏️ Свой размер", callback_data="size_custom")])

    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _stoploss_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("daily_loss_limit_usdc")
    row = []
    rows = []
    for val in STOPLOSS_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"sl_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−10", callback_data="sl_delta:-10"),
        InlineKeyboardButton("+10", callback_data="sl_delta:+10"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой лимит", callback_data="sl_custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _position_sl_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("position_stop_loss_pct")
    enabled = runtime_state.get("position_stop_loss_enabled")
    row = []
    rows = []
    for val in POSITION_SL_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}%", callback_data=f"possl_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−5", callback_data="possl_delta:-5"),
        InlineKeyboardButton("+5", callback_data="possl_delta:+5"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой процент", callback_data="possl_custom")])
    rows.append([InlineKeyboardButton(
        "🔴 Выключить" if enabled else "🟢 Включить", callback_data="possl_toggle",
    )])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


MIN_ENTRY_PRESETS = [0.80, 0.85, 0.87, 0.90]
MAX_ENTRY_PRESETS = [0.93, 0.95, 0.97]


def _range_menu_markup() -> InlineKeyboardMarkup:
    cur_min, cur_max = runtime_state.entry_range(None)
    rows = [[InlineKeyboardButton("— Минимум —", callback_data="noop")]]
    row = []
    for val in MIN_ENTRY_PRESETS:
        mark = "✅ " if abs(val - cur_min) < 0.001 else ""
        row.append(InlineKeyboardButton(f"{mark}{val:.2f}", callback_data=f"minentry_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−0.01", callback_data="minentry_delta:-0.01"),
        InlineKeyboardButton("+0.01", callback_data="minentry_delta:+0.01"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой минимум", callback_data="minentry_custom")])

    rows.append([InlineKeyboardButton("— Максимум —", callback_data="noop")])
    row = []
    for val in MAX_ENTRY_PRESETS:
        mark = "✅ " if abs(val - cur_max) < 0.001 else ""
        row.append(InlineKeyboardButton(f"{mark}{val:.2f}", callback_data=f"maxentry_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−0.01", callback_data="maxentry_delta:-0.01"),
        InlineKeyboardButton("+0.01", callback_data="maxentry_delta:+0.01"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой максимум", callback_data="maxentry_custom")])

    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _settings_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("safety_score_threshold")
    row = []
    rows = []
    for val in SCORE_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"score_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−5", callback_data="score_delta:-5"),
        InlineKeyboardButton("+5", callback_data="score_delta:+5"),
    ])
    rows.append([
        InlineKeyboardButton("−1", callback_data="score_delta:-1"),
        InlineKeyboardButton("+1", callback_data="score_delta:+1"),
    ])
    rows.append([InlineKeyboardButton("— ⏱ Окно входа, мин до конца —", callback_data="noop")])
    wlo, whi = runtime_state.entry_window()
    rows.append([
        InlineKeyboardButton(("✅ " if abs(a - wlo) < 1e-6 and abs(b - whi) < 1e-6 else "") + f"{a:g}–{b:g}"
                             + (" ⭐" if (a, b) == runtime_state.RECOMMENDED_WINDOW else ""),
                             callback_data=f"win_set:{a}:{b}")
        for a, b in WINDOW_PRESETS
    ])
    rows.append([InlineKeyboardButton("— 📦 Реальных позиций одновременно —", callback_data="noop")])
    cur_pos = runtime_state.get("max_open_positions")
    rows.append([
        InlineKeyboardButton(("✅ " if n == cur_pos else "") + str(n), callback_data=f"maxpos_set:{n}")
        for n in MAX_POS_PRESETS
    ])
    rows.append([InlineKeyboardButton(
        "📏 Фильтр расстояния: выкл (сломанный бот) — включить" if not runtime_state.get("distance_filter_enabled")
        else "📏 Фильтр расстояния: вкл — выключить (как сломанный бот)",
        callback_data="distf_toggle",
    )])
    rows.append([InlineKeyboardButton("— 📏 Мин. расстояние от страйка —", callback_data="noop")])
    cur_dist = runtime_state.get("min_distance_pct") or 0.0
    drow = []
    for val in DISTANCE_PRESETS:
        mark = "✅ " if abs(val - cur_dist) < 1e-6 else ""
        label = "выкл" if val == 0 else f"{val:.2f}%"
        drow.append(InlineKeyboardButton(f"{mark}{label}", callback_data=f"dist_set:{val}"))
    rows.append(drow)
    rows.append([InlineKeyboardButton("✏️ Своё расстояние, %", callback_data="dist_custom")])
    rows.append([InlineKeyboardButton(
        "⚡ Быстрый вход: вкл — выключить" if runtime_state.get("fast_entry_enabled")
        else "⚡ Быстрый вход: выкл — включить",
        callback_data="fast_toggle",
    )])
    rows.append([InlineKeyboardButton(
        "📡 Цена сигнала: Chainlink — сменить на Binance" if runtime_state.get("price_source") == "chainlink"
        else "📡 Цена сигнала: Binance — сменить на Chainlink",
        callback_data="price_src_toggle",
    )])
    rows.append([InlineKeyboardButton(
        "⏱ Подтверждение сигнала: вкл — выключить" if runtime_state.get("entry_confirm_enabled")
        else "⏱ Подтверждение сигнала: выкл — включить",
        callback_data="confirm_toggle",
    )])
    rows.append([InlineKeyboardButton("⭐ Рекомендованные настройки", callback_data="preset_apply")])
    scaling_on = runtime_state.get("size_scaling_enabled")
    rows.append([InlineKeyboardButton(
        "📉 Масштабировать размер по score" if not scaling_on else "💯 Входить полным размером",
        callback_data="scaling_toggle",
    )])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _price_from_input(value: float) -> float | None:
    """Цена входа из текста: 0.92 или в центах — 92 -> 0.92. Больше 100 — ошибка."""
    if value > 100:
        return None
    return value / 100 if value > 1 else value


def _range_menu_text() -> str:
    text = (
        f"📈 Диапазон входа: {_range_text()}\n\n"
        "Бот входит, только если ask на нужной стороне попадает в этот диапазон "
        "(и score выше порога). Шире диапазон — больше сигналов, но ниже средняя "
        "цена входа (более рискованные, менее 'подтверждённые' рынком ситуации). "
        f"Максимум — это и потолок цены ордера; дороже {runtime_state.max_entry_cap():.2f} бот не покупает."
    )
    own = _own_list("range")
    text += ("\n\nЭто общий диапазон — для монет без своего."
             + (f" Свой у: {own}." if own else "")
             + " Свой диапазон монеты — 🪙 Активы → ⚙️.")
    return text


def _size_menu_text() -> str:
    mode = runtime_state.get("sizing_mode")
    if mode == "percent":
        bank = runtime_state.current_bankroll()
        size_now = runtime_state.compute_trade_size()
        text = (
            f"💰 Режим: % от банка\n"
            f"Стартовый банк: {runtime_state.get('starting_bankroll_usdc'):.2f} USDC\n"
            f"Текущий банк (старт + реализованный PnL): {bank:.2f} USDC\n"
            f"Доля на сделку: {_fmt_pct(runtime_state.get('bankroll_pct'))} → сейчас это {size_now:.2f} USDC\n\n"
            "Размер сам растёт на прибыли и сжимается на просадке."
        )
    else:
        text = f"💰 Режим: фиксированная сумма — {runtime_state.get('trade_size_usdc'):.2f} USDC на сделку."
    own = _own_list("size")
    text += ("\n\nЭто общая ставка — для монет без своей."
             + (f" Своя у: {own}." if own else "")
             + " Своя ставка монеты — 🪙 Активы → ⚙️. Банк для % один на все монеты.")
    return text


def _confirm_live_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, включить LIVE", callback_data="mode_confirm_live")],
        [InlineKeyboardButton("❌ Отмена", callback_data="menu:main")],
    ])


# ------------------------------------------------------------- команды ----

def _owner_chat_id() -> int | None:
    try:
        return int(str(settings.TELEGRAM_CHAT_ID).strip())
    except (TypeError, ValueError):
        return None


def _is_owner(update: Update) -> bool:
    """Управлять ботом может только чат из TELEGRAM_CHAT_ID (тот же, куда
    приходят уведомления). Раньше проверки не было: любой, кто найдёт имя
    бота, мог открыть /menu и нажать кнопки — вплоть до LIVE и размера
    ставки. Если TELEGRAM_CHAT_ID не задан, ограничение не включаем, чтобы
    не запереть владельца."""
    owner = _owner_chat_id()
    if owner is None:
        return True
    chat = getattr(update, "effective_chat", None)
    return chat is not None and getattr(chat, "id", None) == owner


async def _cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/export — ВСЕ сделки из базы одним CSV (в 4-часовых отчётах только
    сделки своего окна, и по ним легко недосчитаться проигрышей)."""
    if not _is_owner(update):
        return
    import csv
    import os
    rows = storage.get_trades_since(0)
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)
    path = os.path.join(settings.REPORTS_DIR, time.strftime("all_trades_%Y%m%d-%H%M.csv", time.gmtime()))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(storage.TRADES_COLUMNS)
        w.writerows(rows)
    live = storage.get_outcome_stats(live=True)
    caption = f"📥 Все сделки: {len(rows)}\n" + loss_stats.short_line("LIVE", live)
    if _duo_on():
        try:
            from src import duo_menu
            caption += "\n" + "\n".join(duo_menu.report_lines(0)[:6])
        except Exception:  # noqa: BLE001
            pass
    await send_document(path, caption[:1000])


async def _cmd_start_or_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(
        _main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown",
    )


async def _cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(
        _main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown",
    )


async def _cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_stats_text(), parse_mode="Markdown")


async def _cmd_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    s = _state_ref
    if not s:
        await update.message.reply_text("Пока нет данных ни по одному потоку — подожди первого тика бота.")
        return
    parts = []
    for key in sorted(s.keys()):
        inst = s[key]
        parts.append(
            f"*{inst['asset'].upper()} {inst['timeframe']}* — `{inst.get('market_slug', '—')}`\n"
            f"UP: `{inst.get('up_token_id', '—')}`\n"
            f"DOWN: `{inst.get('down_token_id', '—')}`"
        )
    await update.message.reply_text(
        "\n\n".join(parts) + "\n\nДолгий тап на строку с ID — скопировать.",
        parse_mode="Markdown",
    )


def _stats_text() -> str:
    if _duo_on():
        from src import duo_menu
        return duo_menu.stats_text()
    today_start = int(time.time() // 86400) * 86400

    t_live = storage.get_pnl_summary(today_start, live_only=True)
    a_live = storage.get_pnl_summary(0, live_only=True)
    t_dry = storage.get_pnl_summary(today_start, dry_only=True)
    a_dry = storage.get_pnl_summary(0, dry_only=True)

    lines = [
        "📊 *Статистика*",
        "",
        f"LIVE сегодня: {loss_stats.plural_trades(t_live['trades'])}, PnL {t_live['pnl_usdc']:+.2f} USDC, побед {t_live['wins']}",
        f"LIVE всего: {loss_stats.plural_trades(a_live['trades'])}, PnL {a_live['pnl_usdc']:+.2f} USDC, побед {a_live['wins']}",
    ]
    if a_dry["trades"]:
        # Виртуальный PnL — отдельно, чтобы не путать с реальными деньгами
        lines.append(f"DRY сегодня: {loss_stats.plural_trades(t_dry['trades'])}, PnL {t_dry['pnl_usdc']:+.2f} (виртуально), побед {t_dry['wins']}")
        lines.append(f"DRY всего: {loss_stats.plural_trades(a_dry['trades'])}, PnL {a_dry['pnl_usdc']:+.2f} (виртуально), побед {a_dry['wins']}")

    # Нормальные ли проигрыши: факт против того, что заложено в цены входа
    # (см. src/loss_stats.py). Отдельно LIVE и DRY RUN — их нельзя смешивать.
    try:
        live_st = storage.get_outcome_stats(live=True)
        dry_st = storage.get_outcome_stats(live=False)
        lines.append("")
        lines.append("*Проигрыши: факт против цены входа*")
        lines.extend(loss_stats.summary_lines("LIVE", live_st))
        if dry_st["trades"]:
            lines.extend(loss_stats.summary_lines("DRY RUN", dry_st))
        lines.append("Все сделки одним файлом: /export")
    except Exception:  # noqa: BLE001 — статистика не должна ломать меню
        pass

    # По каждой монете отдельно для LIVE и DRY: так видно, на какой монете
    # стратегия даёт меньше проигрышей, чем заложено в цены, а на какой нет.
    try:
        by = storage.get_pnl_by_asset_mode(0)
        if by:
            lines.append("")
            lines.append("*По монетам:*")
            for asset, dry in sorted(by.keys(), key=lambda k: (k[1], k[0])):
                st = storage.get_outcome_stats(live=not dry, asset=asset)
                label = f"{asset.upper()} {'DRY' if dry else 'LIVE'}"
                if st["trades"]:
                    lines.append(f"  {loss_stats.short_line(label, st)}, PnL {st['pnl_usdc']:+.2f}")
    except Exception:  # noqa: BLE001
        pass

    return "\n".join(lines)


async def _on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ловит обычный текст, когда мы ждём число после '✏️ Свой размер'/'✏️ Свой лимит'.
    Вне этого режима ничего не делает — не мешает обычной переписке."""
    global _pending_input
    if _pending_input is None or not _is_owner(update):
        return

    raw = (update.message.text or "").strip().replace(",", ".")
    if _pending_input.startswith(("duo_n:", "duo_bank:")):
        from src import duo_menu
        res = duo_menu.apply_text_input(_pending_input, raw)
        if res is not None:
            text, markup = res
            if markup is None:
                await update.message.reply_text(text)   # ждём ввод дальше
                return
            _pending_input = None
            await update.message.reply_text(text, reply_markup=markup)
            return
    try:
        value = float(raw)
        if value < 0 or (value == 0 and _pending_input != "min_distance"):
            raise ValueError
    except ValueError:
        await update.message.reply_text("Не похоже на положительное число, попробуй ещё раз (например: 15.5)")
        return

    if _pending_input in ("min_entry", "max_entry") or _pending_input.startswith(("a:min:", "a:max:")):
        price = _price_from_input(value)
        if price is None:
            await update.message.reply_text(
                "Цена входа — число до 1, например 0.92 (или в центах: 92). Попробуй ещё раз.")
            return
        value = price

    if _pending_input.startswith("a:"):
        # Свои настройки монеты: "a:<поле>:<монета>"
        _prefix, field, asset = _pending_input.split(":", 2)
        _pending_input = None
        if asset not in settings.ASSETS:
            return
        if field == "min":
            runtime_state.set_asset_range(asset, lo=value)
        elif field == "max":
            runtime_state.set_asset_range(asset, hi=value)
        elif field == "pct":
            runtime_state.set_asset_sizing(asset, mode="percent", bankroll_pct=value)
        elif field == "usdc":
            runtime_state.set_asset_sizing(asset, mode="fixed", trade_size_usdc=value)
        if field in ("min", "max"):
            await update.message.reply_text("✅ " + _asset_range_text(asset), reply_markup=_asset_range_markup(asset))
        else:
            await update.message.reply_text("✅ " + _asset_size_text(asset), reply_markup=_asset_size_markup(asset))
        return

    if _pending_input == "size":
        runtime_state.set("trade_size_usdc", value)
        _pending_input = None
        await update.message.reply_text(f"✅ Размер позиции: {value:.2f} USDC", reply_markup=_size_menu_markup())
    elif _pending_input == "stoploss":
        runtime_state.set("daily_loss_limit_usdc", value)
        _pending_input = None
        await update.message.reply_text(f"✅ Стоп-лосс/день: {value:.2f} USDC", reply_markup=_stoploss_menu_markup())
    elif _pending_input == "position_sl":
        value = min(99.0, value)  # 100%+ бессмысленно — это уже полная потеря
        runtime_state.set("position_stop_loss_pct", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Стоп-лосс позиции: {value:.1f}%", reply_markup=_position_sl_menu_markup(),
        )
    elif _pending_input == "min_entry":
        lo, _hi = runtime_state.set_global_range(lo=value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Минимум диапазона входа: {lo:.2f}", reply_markup=_range_menu_markup(),
        )
    elif _pending_input == "max_entry":
        _lo, hi = runtime_state.set_global_range(hi=value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Максимум диапазона входа: {hi:.2f}", reply_markup=_range_menu_markup(),
        )
    elif _pending_input == "starting_bankroll":
        runtime_state.set("starting_bankroll_usdc", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Стартовый банк: {value:.2f} USDC", reply_markup=_size_menu_markup(),
        )
    elif _pending_input == "copytrade_size":
        runtime_state.set("copytrade_size_usdc", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Размер копи-сделки: {value:.2f} USDC", reply_markup=_wallet_menu_markup(),
        )
    elif _pending_input == "min_distance":
        value = min(value, 5.0)
        runtime_state.set("min_distance_pct", value)
        _pending_input = None
        await update.message.reply_text("✅ " + _settings_text(), reply_markup=_settings_menu_markup())
    elif _pending_input == "hedge_stake":
        runtime_state.set("hedge_stake_usdc", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Размер ставки хеджа: {value:.2f} USDC", reply_markup=_hedge_menu_markup(),
        )


# ------------------------------------------------------------- кнопки -----

async def _safe_edit(query, text: str, markup=None) -> None:
    """edit_message_text, который не падает на «message is not modified»
    (повторное нажатие той же кнопки)."""
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except Exception as exc:  # noqa: BLE001
        if "not modified" not in str(exc).lower():
            raise


# Кнопки ⚙️ монеты: префикс:монета[:значение]. Короткие префиксы — у Telegram
# лимит 64 байта на callback_data.
_ASSET_CFG_PREFIXES = {
    "acfg", "arst",                                   # экран монеты, сброс всего
    "arng", "amin", "amind", "aminc",                 # диапазон: экран, минимум
    "amax", "amaxd", "amaxc", "arngr",                # максимум, сброс диапазона
    "asz", "aszm", "aszp", "aszpd", "aszpc",          # ставка: экран, режим, %
    "aszu", "aszuc", "aszr",                          # фикс. сумма, сброс ставки
}


async def _handle_asset_cfg(query, data: str) -> bool:
    """Свои настройки монеты (диапазон входа, ставка). True — кнопка отсюда."""
    global _pending_input
    prefix, _sep, rest = data.partition(":")
    if prefix not in _ASSET_CFG_PREFIXES:
        return False
    asset, _sep, arg = rest.partition(":")
    if asset not in settings.ASSETS:
        return True
    a = asset.upper()
    try:
        num = float(arg) if arg else None
    except ValueError:
        return True

    def cancel(back: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=back)]])

    if prefix == "acfg":
        await _safe_edit(query, _asset_cfg_text(asset), _asset_cfg_markup(asset))

    elif prefix == "arst":
        runtime_state.reset_asset(asset)
        await _safe_edit(query, f"↩️ {a}: диапазон входа и ставка снова общие.\n\n" + _asset_cfg_text(asset),
                         _asset_cfg_markup(asset))

    elif prefix == "arng":
        await _safe_edit(query, _asset_range_text(asset), _asset_range_markup(asset))

    elif prefix in ("amin", "amind", "amax", "amaxd"):
        if num is None:
            return True
        lo, hi = runtime_state.entry_range(asset)
        if prefix == "amin":
            runtime_state.set_asset_range(asset, lo=num)
        elif prefix == "amind":
            runtime_state.set_asset_range(asset, lo=lo + num)
        elif prefix == "amax":
            runtime_state.set_asset_range(asset, hi=num)
        else:
            runtime_state.set_asset_range(asset, hi=hi + num)
        await _safe_edit(query, "✅ " + _asset_range_text(asset), _asset_range_markup(asset))

    elif prefix in ("aminc", "amaxc"):
        is_min = prefix == "aminc"
        _pending_input = f"a:{'min' if is_min else 'max'}:{asset}"
        await _safe_edit(
            query,
            f"✏️ Напиши {'минимальную' if is_min else 'максимальную'} цену входа для {a} следующим "
            f"сообщением, например: {'0.90' if is_min else '0.95'} (можно в центах: {'90' if is_min else '95'})",
            cancel(f"arng:{asset}"),
        )

    elif prefix == "arngr":
        runtime_state.reset_asset_range(asset)
        await _safe_edit(query, "↩️ " + _asset_range_text(asset), _asset_range_markup(asset))

    elif prefix == "asz":
        await _safe_edit(query, _asset_size_text(asset), _asset_size_markup(asset))

    elif prefix == "aszm":
        mode, _usdc, _pct = runtime_state.sizing(asset)
        runtime_state.set_asset_sizing(asset, mode="fixed" if mode == "percent" else "percent")
        await _safe_edit(query, "✅ " + _asset_size_text(asset), _asset_size_markup(asset))

    elif prefix in ("aszp", "aszpd"):
        if num is None:
            return True
        _mode, _usdc, pct = runtime_state.sizing(asset)
        runtime_state.set_asset_sizing(asset, mode="percent", bankroll_pct=num if prefix == "aszp" else pct + num)
        await _safe_edit(query, "✅ " + _asset_size_text(asset), _asset_size_markup(asset))

    elif prefix == "aszu":
        if num is None:
            return True
        runtime_state.set_asset_sizing(asset, mode="fixed", trade_size_usdc=num)
        await _safe_edit(query, "✅ " + _asset_size_text(asset), _asset_size_markup(asset))

    elif prefix in ("aszpc", "aszuc"):
        is_pct = prefix == "aszpc"
        _pending_input = f"a:{'pct' if is_pct else 'usdc'}:{asset}"
        await _safe_edit(
            query,
            (f"✏️ Напиши процент банка на одну сделку {a} следующим сообщением, например: 2.5"
             if is_pct else
             f"✏️ Напиши сумму одной сделки {a} в USDC следующим сообщением, например: 15"),
            cancel(f"asz:{asset}"),
        )

    elif prefix == "aszr":
        runtime_state.reset_asset_sizing(asset)
        await _safe_edit(query, "↩️ " + _asset_size_text(asset), _asset_size_markup(asset))

    return True


async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Повторное нажатие той же кнопки даёт у Telegram ошибку «message is not
    modified» — это не ошибка бота, глушим её здесь для всех кнопок сразу."""
    try:
        await _on_callback_inner(update, context)
    except Exception as exc:  # noqa: BLE001
        if "not modified" not in str(exc).lower():
            raise


async def _on_callback_inner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _pending_input
    query = update.callback_query
    if not _is_owner(update):
        await query.answer()
        return
    data = query.data
    await query.answer()
    # Любая кнопка отменяет ожидание текстового ввода (например, «❌ Отмена»
    # после «✏️ Свой …»): раньше ожидание оставалось, и следующее число в чате
    # молча меняло настройку. Кнопки «✏️ …» ниже выставляют его заново.
    _pending_input = None

    if await _handle_asset_cfg(query, data):
        return

    if data == "duo:hist":
        # 🎯 → 📚: серии UP/DOWN по свечам Binance — считается в фоне, итог придёт сообщением
        from src import duo_menu, history_study
        if history_study.start():
            head = (f"📚 Запустил: беру свечи Binance за {settings.SERIES_HISTORY_DAYS} дн. (5m и 15m, "
                    f"{', '.join(a.upper() for a in settings.ASSETS)}) и считаю серии и удвоения. "
                    "Минут 5 — пришлю итог и файл. Бот в это время работает как обычно.")
        else:
            head = history_study.status_text() or "📚 Расчёт уже идёт."
        await _safe_edit(query, head + "\n\n" + duo_menu.menu_text(), duo_menu.menu_markup())
        return

    if data == "duo:spdx":
        # ⚡ Скорость рынка → 📤: файл со всеми рывками из базы
        from src import duo_menu, reporting, speed_lab
        try:
            n = await reporting.send_speed_all()
            head = f"📤 Отправил файл: рывков {n}." if n else "📤 Рывков в базе пока нет — нечего выгружать."
        except Exception as exc:  # noqa: BLE001
            head = f"⚠️ Файл не ушёл: {exc}"[:300]
        await _safe_edit(query, head + "\n\n" + speed_lab.menu_text(), duo_menu.speed_markup())
        return

    if data.startswith("duo:"):
        from src import duo_menu
        res = duo_menu.handle(data)
        if res is not None:
            text, markup, pending = res
            _pending_input = pending
            await _safe_edit(query, text, markup)
            return

    if data == "distf_toggle":
        runtime_state.set("distance_filter_enabled", not runtime_state.get("distance_filter_enabled"))
        await _safe_edit(query, _settings_text(), _settings_menu_markup())
        return

    if data == "menu:main":
        await query.edit_message_text(_main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data == "menu:size":
        await _safe_edit(query, _size_menu_text(), _size_menu_markup())

    elif data == "sizing_mode_toggle":
        new_mode = "percent" if runtime_state.get("sizing_mode") == "fixed" else "fixed"
        runtime_state.set("sizing_mode", new_mode)
        await query.edit_message_text(
            f"✅ Режим размера ставки: {'% от банка' if new_mode=='percent' else 'фиксированная сумма'}",
            reply_markup=_size_menu_markup(),
        )

    elif data.startswith("bankrollpct_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("bankroll_pct", val)
        await query.edit_message_text(f"✅ Доля от банка: {val:.0f}%", reply_markup=_size_menu_markup())

    elif data.startswith("bankrollpct_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = min(50.0, max(1.0, runtime_state.get("bankroll_pct") + delta))
        runtime_state.set("bankroll_pct", new_val)
        await query.edit_message_text(f"✅ Доля от банка: {new_val:.0f}%", reply_markup=_size_menu_markup())

    elif data == "startbank_custom":
        _pending_input = "starting_bankroll"
        await query.edit_message_text(
            "✏️ Напиши стартовый банк в USDC следующим сообщением, например: 60",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:size")]]),
        )

    elif data == "menu:hedge":
        enabled = runtime_state.get("hedge_bot_enabled")
        await query.edit_message_text(
            f"🔒 Хедж-бот: {'🟢 включён' if enabled else '🔴 выключен'}\n"
            f"Вход при цене {runtime_state.get('hedge_entry_price'):.2f}, хедж противоположной стороны при "
            f"{runtime_state.get('hedge_trigger_price'):.2f}\n"
            f"Размер ставки: {runtime_state.get('hedge_stake_usdc'):.2f} USDC\n\n"
            "Если цена не доходит до порога хеджа — остаётся односторонняя позиция "
            "(по нашим данным такие случаи почти всегда проигрывают). "
            "PnL в отчётах — без учёта комиссии тейкера.",
            reply_markup=_hedge_menu_markup(),
        )

    elif data == "hedge_toggle":
        new_val = not runtime_state.get("hedge_bot_enabled")
        runtime_state.set("hedge_bot_enabled", new_val)
        await query.edit_message_text(
            f"{'🟢 Хедж-бот включён' if new_val else '🔴 Хедж-бот выключен'}",
            reply_markup=_hedge_menu_markup(),
        )

    elif data.startswith("hedgetrigger_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("hedge_trigger_price", val)
        await query.edit_message_text(f"✅ Порог хеджа: {val:.2f}", reply_markup=_hedge_menu_markup())

    elif data.startswith("hedgestake_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("hedge_stake_usdc", val)
        await query.edit_message_text(f"✅ Размер ставки хеджа: {val:.2f} USDC", reply_markup=_hedge_menu_markup())

    elif data == "hedgestake_custom":
        _pending_input = "hedge_stake"
        await query.edit_message_text(
            "✏️ Напиши размер ставки хеджа в USDC следующим сообщением, например: 7.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:hedge")]]),
        )

    elif data == "menu:wallet":
        addr = settings.WALLET_TRACK_ADDRESS
        notify_on = runtime_state.get("wallet_notify_enabled")
        copy_on = runtime_state.get("wallet_copytrade_enabled")
        await query.edit_message_text(
            f"🐋 Слежу за кошельком:\n`{addr}`\n\n"
            f"Уведомления: {'🔔 включены' if notify_on else '🔕 выключены'}\n"
            f"Копитрейдинг: {'🟢 включён' if copy_on else '🔴 выключен'} "
            f"(размер: {runtime_state.get('copytrade_size_usdc'):.2f} USDC на сделку)\n\n"
            "Копитрейдинг использует те же лимиты риска, что и основная стратегия "
            "(дневной стоп-лосс, общий потолок открытых позиций).",
            reply_markup=_wallet_menu_markup(),
            parse_mode="Markdown",
        )

    elif data == "wallet_notify_toggle":
        new_val = not runtime_state.get("wallet_notify_enabled")
        runtime_state.set("wallet_notify_enabled", new_val)
        await query.edit_message_text(
            f"{'🔔 Уведомления включены' if new_val else '🔕 Уведомления выключены'}",
            reply_markup=_wallet_menu_markup(),
        )

    elif data == "wallet_copytrade_toggle":
        new_val = not runtime_state.get("wallet_copytrade_enabled")
        runtime_state.set("wallet_copytrade_enabled", new_val)
        msg = (
            "🟢 Копитрейдинг включён — бот будет пытаться повторять его входы реальными "
            "(или dry-run) сделками." if new_val else
            "🔴 Копитрейдинг выключен — только уведомления, без автоматических сделок."
        )
        await query.edit_message_text(msg, reply_markup=_wallet_menu_markup())

    elif data.startswith("copysize_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("copytrade_size_usdc", val)
        await query.edit_message_text(f"✅ Размер копи-сделки: {val:.2f} USDC", reply_markup=_wallet_menu_markup())

    elif data == "copysize_custom":
        _pending_input = "copytrade_size"
        await query.edit_message_text(
            "✏️ Напиши размер копи-сделки в USDC следующим сообщением, например: 7.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:wallet")]]),
        )

    elif data == "menu:assets":
        await query.edit_message_text(_assets_menu_text(), reply_markup=_assets_menu_markup())

    elif data.startswith("asset_toggle:"):
        asset = data.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return
        now_enabled = runtime_state.toggle_asset(asset)
        mode = "реальные сделки" if not runtime_state.trade_is_dry(asset) else "виртуальные сделки (DRY)"
        await query.edit_message_text(
            f"{'✅' if now_enabled else '⏸'} {asset.upper()} теперь "
            f"{'включён — ' + mode if now_enabled else 'выключен'}.\n\n" + _assets_menu_text(),
            reply_markup=_assets_menu_markup(),
        )

    elif data.startswith("asset_mode:"):
        asset = data.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return
        if runtime_state.is_asset_live(asset):
            # LIVE -> DRY — безопасное направление, без подтверждения
            runtime_state.set_asset_live(asset, False)
            await query.edit_message_text(
                f"🧪 {asset.upper()} переведён в DRY: дальше по нему только виртуальные сделки.\n\n"
                + _assets_menu_text(),
                reply_markup=_assets_menu_markup(),
            )
        else:
            await query.edit_message_text(
                f"⚠️ Включить реальные сделки по {asset.upper()}?\n"
                "Когда бот в LIVE, по этой монете пойдут настоящие ордера. Сначала стоит "
                "убедиться в 📊 Статистике, что в DRY по ней проигрышей заметно меньше, "
                "чем заложено в цены.\n\n"
                f"Настройки {asset.upper()}: {_coin_settings_short(asset)}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"✅ Да, LIVE для {asset.upper()}", callback_data=f"asset_live_confirm:{asset}")],
                    [InlineKeyboardButton("❌ Оставить DRY", callback_data="menu:assets")],
                ]),
            )

    elif data.startswith("asset_live_confirm:"):
        asset = data.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return
        runtime_state.set_asset_live(asset, True)
        note = ("" if not runtime_state.get("dry_run")
                else "\nСам бот сейчас в DRY RUN — реальные сделки пойдут, когда включишь LIVE в главном меню.")
        await query.edit_message_text(
            f"🔴 {asset.upper()} помечен для реальных сделок.{note}\n\n" + _assets_menu_text(),
            reply_markup=_assets_menu_markup(),
        )

    elif data == "dry_notify_toggle":
        new_val = not runtime_state.get("notify_dry_assets")
        runtime_state.set("notify_dry_assets", new_val)
        await query.edit_message_text(
            ("🔔 Уведомления о виртуальных сделках DRY-монет включены.\n\n" if new_val
             else "🔕 Уведомления о виртуальных сделках DRY-монет выключены — они видны в 📊 Статистике и отчётах.\n\n")
            + _assets_menu_text(),
            reply_markup=_assets_menu_markup(),
        )

    elif data == "menu:sl":
        await query.edit_message_text(
            f"🛑 Дневной стоп-лосс: {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC.\n"
            "При достижении убытка на эту сумму за день бот перестаёт открывать новые позиции до полуночи.",
            reply_markup=_stoploss_menu_markup(),
        )

    elif data == "menu:possl":
        enabled = runtime_state.get("position_stop_loss_enabled")
        await query.edit_message_text(
            f"📉 Стоп-лосс ОТДЕЛЬНОЙ позиции: {'🟢 включён' if enabled else '🔴 выключен'}, "
            f"порог {runtime_state.get('position_stop_loss_pct'):.0f}%.\n\n"
            "Если стоимость открытой позиции (по текущей цене в стакане) падает на этот "
            "процент от суммы входа ещё ДО резолюции рынка — бот продаёт её досрочно, "
            "не дожидаясь исхода. Это отдельно от дневного лимита в USDC.\n\n"
            "Что показала история (18.09–06.10, ~2700 рынков BTC 5m и все 213 реальных сделок): "
            "прибыль стоп не увеличивает. Проигрышная позиция падает с 0.9 до 0.4 за секунды, "
            "поэтому продаёт он обычно уже около −50…−80%, а примерно 4 из 10 позиций, просевших "
            "на 50%, потом всё равно выигрывают — их стоп продаёт в минус. На реальных сделках "
            "25.09–06.10 стоп 50% дал бы +$66 вместо +$114. Уменьшить боль от проигрышей "
            "честнее размером ставки (💰 Размер позиции).",
            reply_markup=_position_sl_menu_markup(),
        )

    elif data == "menu:range":
        await _safe_edit(query, _range_menu_text(), _range_menu_markup())

    elif data == "noop":
        pass

    elif data.startswith("minentry_set:"):
        lo, _hi = runtime_state.set_global_range(lo=float(data.split(":", 1)[1]))
        await _safe_edit(query, f"✅ Минимум диапазона входа: {lo:.2f}", _range_menu_markup())

    elif data.startswith("minentry_delta:"):
        delta = float(data.split(":", 1)[1])
        lo, _hi = runtime_state.set_global_range(lo=runtime_state.entry_range(None)[0] + delta)
        await _safe_edit(query, f"✅ Минимум диапазона входа: {lo:.2f}", _range_menu_markup())

    elif data == "minentry_custom":
        _pending_input = "min_entry"
        await query.edit_message_text(
            "✏️ Напиши минимальную цену входа следующим сообщением, например: 0.82",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:range")]]),
        )

    elif data.startswith("maxentry_set:"):
        _lo, hi = runtime_state.set_global_range(hi=float(data.split(":", 1)[1]))
        await _safe_edit(query, f"✅ Максимум диапазона входа: {hi:.2f}", _range_menu_markup())

    elif data.startswith("maxentry_delta:"):
        delta = float(data.split(":", 1)[1])
        _lo, hi = runtime_state.set_global_range(hi=runtime_state.entry_range(None)[1] + delta)
        await _safe_edit(query, f"✅ Максимум диапазона входа: {hi:.2f}", _range_menu_markup())

    elif data == "maxentry_custom":
        _pending_input = "max_entry"
        await query.edit_message_text(
            "✏️ Напиши максимальную цену входа следующим сообщением, например: 0.96",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:range")]]),
        )

    elif data == "menu:settings":
        await query.edit_message_text(_settings_text(), reply_markup=_settings_menu_markup())

    elif data.startswith("win_set:"):
        try:
            _p, a, b = data.split(":")
            runtime_state.set_entry_window(float(a), float(b))
        except ValueError:
            return
        await query.edit_message_text("✅ " + _settings_text(), reply_markup=_settings_menu_markup())

    elif data.startswith("maxpos_set:"):
        try:
            n = int(data.split(":", 1)[1])
        except ValueError:
            return
        runtime_state.set("max_open_positions", max(1, min(n, 10)))
        await query.edit_message_text("✅ " + _settings_text(), reply_markup=_settings_menu_markup())

    elif data == "why":
        await query.edit_message_text(
            diagnostics.why_text(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="why_refresh")],
                [InlineKeyboardButton("◀️ Назад", callback_data="menu:main")],
            ]),
        )

    elif data == "why_refresh":
        await query.edit_message_text(
            diagnostics.why_text(),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить", callback_data="why")],
                [InlineKeyboardButton("◀️ Назад", callback_data="menu:main")],
            ]),
        )

    elif data.startswith("dist_set:"):
        runtime_state.set("min_distance_pct", float(data.split(":", 1)[1]))
        await query.edit_message_text("✅ " + _settings_text(), reply_markup=_settings_menu_markup())

    elif data == "dist_custom":
        _pending_input = "min_distance"
        await query.edit_message_text(
            "✏️ Напиши минимальное расстояние от страйка в % от цены, например: 0.07\n"
            "(для BTC ~84k: 0.05 ≈ $42, 0.07 ≈ $59, 0.10 ≈ $84). 0 — выключить фильтр.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:settings")]]),
        )

    elif data == "preset_apply":
        runtime_state.apply_recommended()
        own = _overrides_summary()
        await query.edit_message_text(
            "⭐ Применены рекомендованные настройки: порог 88, диапазон "
            f"{_range_text()}, окно входа {runtime_state.entry_window()[0]:g}–"
            f"{runtime_state.entry_window()[1]:g} мин, мин. расстояние {_distance_summary()}, "
            "быстрый вход вкл, подтверждение сигнала вкл, цена сигнала — Chainlink.\n"
            + (f"Свои настройки монет не тронуты: {own}.\n" if own else "")
            + "\n" + _settings_text(),
            reply_markup=_settings_menu_markup(),
        )

    elif data == "stats":
        await query.edit_message_text(_stats_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data == "pause_toggle":
        runtime_state.set("paused", not runtime_state.get("paused"))
        await query.edit_message_text(_main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data.startswith("size_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("trade_size_usdc", val)
        await query.edit_message_text(f"✅ Размер позиции: {val:.0f} USDC", reply_markup=_size_menu_markup())

    elif data == "size_custom":
        _pending_input = "size"
        await query.edit_message_text(
            "✏️ Напиши число (USDC) следующим сообщением, например: 15.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:size")]]),
        )

    elif data.startswith("sl_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("daily_loss_limit_usdc", val)
        await query.edit_message_text(f"✅ Стоп-лосс: {val:.0f} USDC/день", reply_markup=_stoploss_menu_markup())

    elif data.startswith("sl_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = max(5.0, runtime_state.get("daily_loss_limit_usdc") + delta)
        runtime_state.set("daily_loss_limit_usdc", new_val)
        await query.edit_message_text(f"✅ Стоп-лосс: {new_val:.0f} USDC/день", reply_markup=_stoploss_menu_markup())

    elif data == "sl_custom":
        _pending_input = "stoploss"
        await query.edit_message_text(
            "✏️ Напиши число (USDC/день) следующим сообщением, например: 75",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:sl")]]),
        )

    elif data.startswith("possl_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("position_stop_loss_pct", val)
        await query.edit_message_text(f"✅ Стоп-лосс позиции: {val:.0f}%", reply_markup=_position_sl_menu_markup())

    elif data.startswith("possl_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = min(99.0, max(1.0, runtime_state.get("position_stop_loss_pct") + delta))
        runtime_state.set("position_stop_loss_pct", new_val)
        await query.edit_message_text(
            f"✅ Стоп-лосс позиции: {new_val:.0f}%", reply_markup=_position_sl_menu_markup(),
        )

    elif data == "possl_custom":
        _pending_input = "position_sl"
        await query.edit_message_text(
            "✏️ Напиши процент просадки следующим сообщением, например: 40",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:possl")]]),
        )

    elif data == "possl_toggle":
        new_val = not runtime_state.get("position_stop_loss_enabled")
        runtime_state.set("position_stop_loss_enabled", new_val)
        msg = (f"🟢 Стоп-лосс позиции включён, порог {runtime_state.get('position_stop_loss_pct'):.0f}%."
               if new_val else "🔴 Стоп-лосс позиции выключен — позиции держим до резолюции рынка в любом случае.")
        await query.edit_message_text(msg, reply_markup=_position_sl_menu_markup())

    elif data.startswith("score_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = min(100.0, max(0.0, runtime_state.get("safety_score_threshold") + delta))
        runtime_state.set("safety_score_threshold", new_val)
        await query.edit_message_text(
            f"⚙️ Safety score порог: {new_val:.0f}", reply_markup=_settings_menu_markup(),
        )

    elif data.startswith("score_set:"):
        new_val = float(data.split(":", 1)[1])
        runtime_state.set("safety_score_threshold", new_val)
        await query.edit_message_text(
            f"⚙️ Safety score порог: {new_val:.0f}", reply_markup=_settings_menu_markup(),
        )

    elif data == "fast_toggle":
        new_val = not runtime_state.get("fast_entry_enabled")
        runtime_state.set("fast_entry_enabled", new_val)
        msg = ("⚡ Быстрый вход включён — сигнал проверяется и между 3-секундными тиками."
               if new_val else
               "⚡ Быстрый вход выключен — сигнал проверяется только раз в 3 с, как раньше.")
        await query.edit_message_text(msg + "\n\n" + _settings_text(), reply_markup=_settings_menu_markup())

    elif data == "price_src_toggle":
        new_val = "binance" if runtime_state.get("price_source") == "chainlink" else "chainlink"
        runtime_state.set("price_source", new_val)
        msg = ("📡 Цена сигнала — Chainlink: цена и страйк как у Polymarket при расчёте исхода."
               if new_val == "chainlink" else
               "📡 Цена сигнала — Binance, как было до 12.10.")
        await query.edit_message_text(msg + "\n\n" + _settings_text(), reply_markup=_settings_menu_markup())

    elif data == "confirm_toggle":
        new_val = not runtime_state.get("entry_confirm_enabled")
        runtime_state.set("entry_confirm_enabled", new_val)
        sec = f"{max(0.0, settings.ENTRY_CONFIRM_SEC):g}"
        msg = (f"⏱ Подтверждение включено — вход, только если сигнал был и {sec} с назад."
               if new_val else
               "⏱ Подтверждение выключено — вход на первой проверке с сигналом, как раньше.")
        await query.edit_message_text(msg + "\n\n" + _settings_text(), reply_markup=_settings_menu_markup())

    elif data == "scaling_toggle":
        new_val = not runtime_state.get("size_scaling_enabled")
        runtime_state.set("size_scaling_enabled", new_val)
        msg = ("📉 Масштабирование включено — размер сделки зависит от score."
               if new_val else
               "💯 Масштабирование выключено — любая прошедшая порог сделка идёт полным размером.")
        await query.edit_message_text(msg, reply_markup=_settings_menu_markup())

    elif data == "mode_toggle":
        if runtime_state.get("dry_run"):
            if not settings.POLY_PRIVATE_KEY:
                await query.edit_message_text(
                    "❌ Нельзя включить LIVE: POLY_PRIVATE_KEY не задан в переменных окружения.\n"
                    "Добавь ключ и передеплой бота, потом попробуй снова.",
                    reply_markup=_main_menu_markup(),
                )
                return
            # DRY RUN -> LIVE — это реальные деньги, спрашиваем подтверждение
            # и показываем, по каким монетам и с какой ставкой пойдут ордера.
            await query.edit_message_text(_confirm_live_text(), reply_markup=_confirm_live_markup())
        else:
            runtime_state.set("dry_run", True)
            await query.edit_message_text(
                "🧪 Переключено в DRY RUN — реальные сделки остановлены.",
                reply_markup=_main_menu_markup(),
            )

    elif data == "mode_confirm_live":
        if not settings.POLY_PRIVATE_KEY:
            # Двойная защита: ключ мог пропасть между нажатием "Старт" и подтверждением
            # (например, кто-то параллельно поменял env и не передеплоил).
            await query.edit_message_text(
                "❌ Нельзя включить LIVE: POLY_PRIVATE_KEY не задан. Остаёмся в DRY RUN.",
                reply_markup=_main_menu_markup(),
            )
            return
        runtime_state.set("dry_run", False)
        await query.edit_message_text(
            "🔴 LIVE включён. Бот будет выставлять реальные ордера на реальные деньги.",
            reply_markup=_main_menu_markup(),
        )


def build_app() -> Application:
    global _app
    _app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    _app.add_handler(CommandHandler("start", _cmd_start_or_menu))
    _app.add_handler(CommandHandler("menu", _cmd_start_or_menu))
    _app.add_handler(CommandHandler("status", _cmd_status))
    _app.add_handler(CommandHandler("pnl", _cmd_pnl))
    _app.add_handler(CommandHandler("token", _cmd_token))
    _app.add_handler(CommandHandler("export", _cmd_export))
    _app.add_handler(CallbackQueryHandler(_on_callback))
    _app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _on_text))
    return _app


async def clear_legacy_keyboard() -> None:
    """
    Если этот бот-токен раньше использовался другой программой (например,
    copy-trading ботом со своей постоянной клавиатурой START/STOP/AMOUNT/...),
    Telegram продолжает показывать её внизу чата, пока что-то явно не пришлёт
    ReplyKeyboardRemove — наши собственные кнопки инлайновые и её не трогают.
    Вызывается один раз при старте.
    """
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    try:
        await _app.bot.send_message(
            chat_id=settings.TELEGRAM_CHAT_ID,
            text="🧹 Убираю старую клавиатуру, если она осталась от другого бота...",
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception:
        pass


async def notify(text: str) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text)


async def send_document(path: str, caption: str | None) -> None:
    """Отправляет файл (например, CSV-отчёт) в чат. caption может быть None,
    если это второй файл в паре и подпись уже была у первого."""
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    with open(path, "rb") as f:
        await _app.bot.send_document(chat_id=settings.TELEGRAM_CHAT_ID, document=f, caption=caption)
