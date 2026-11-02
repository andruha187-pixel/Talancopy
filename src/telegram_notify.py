"""
Телеграм: кнопочное меню ⏱ бота позднего входа и уведомления.

/menu:
  ⚙️ Настройки — вкл/выкл, окно входа (за сколько до конца), цена, до страйка
                 (% и ATR), сумма входа, дневной стоп; ✏️ — своё значение сообщением
  🪙 Монеты    — у каждой: ✅ вкл / ⏸ выкл, 🧪 тест / 🔴 реально и 💵 своя сумма входа
                 (общая, фиксированная $ или % от банка) и 🏦 свой банк монеты
  📊 Статистика · 📤 Файлы
  🔴 Включить LIVE (с подтверждением) / 🧪 Выключить LIVE

Реальная ставка уходит, только если монета включена и 🔴, а бот в 🔴 LIVE.
Изменения пишутся в runtime_state (SQLite) и переживают рестарт, если база
на постоянном томе. Управлять может только чат из TELEGRAM_CHAT_ID.
"""
from __future__ import annotations
import logging
import re
import time

from telegram import Update, InlineKeyboardButton as Btn, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters

from config import settings
from src import late_bot, runtime_state, storage

log = logging.getLogger("telegram")

_app: Application | None = None
TG_TEXT_LIMIT = 4096


def _fit(text: str) -> str:
    """Не длиннее лимита Telegram (иначе экран не откроется вовсе)."""
    return text if len(text) <= TG_TEXT_LIMIT else text[:TG_TEXT_LIMIT - 2].rstrip() + "\n…"


def _menu(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([list(r) for r in rows])


BACK = Btn("◀️ Назад", callback_data="m")


def _mark(ok: bool) -> str:
    return "✅ " if ok else ""


def _same(a: float, b: float) -> bool:
    return abs(a - b) < 1e-9


# ------------------------------------------------------------ главное меню ----

def coins_line() -> str:
    return " · ".join(f"{a.upper()} {'✅' if runtime_state.is_asset_enabled(a) else '⏸'}"
                      f"{'🔴' if runtime_state.is_asset_live(a) else '🧪'}" for a in settings.ASSETS)


def _day_line() -> str:
    day = int(time.time() // 86400) * 86400

    def part(st: dict) -> str:
        if not st["bets"]:
            return "ставок нет"
        s = late_bot.bets_text(st["bets"])
        if st["settled"]:
            s += f", выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
        return s
    test, real = late_bot.trade_stats(day, live=False), late_bot.trade_stats(day, live=True)
    line = f"Сегодня (UTC): тест — {part(test)}"
    if real["bets"] or late_bot.live_coins():
        line += f"; реальные — {part(real)}"
    return line


def stake_text() -> str:
    s = late_bot.stop()
    return (f"{late_bot.stakes_text()} (не меньше 5 акций)"
            + (f", дневной стоп реальных −${s:g}" if s > 0 else ", без дневного стопа"))


def main_text() -> str:
    lines = [
        "⏱ Поздний вход 5m — ставка на сторону, которая за ~3 минуты до конца уже далеко от страйка",
        "",
        "Режим: " + late_bot.mode_text(),
        f"Правило: {'✅' if late_bot.enabled() else '⏸ выключено —'} {late_bot.rule_short()}; {stake_text()}",
        "Монеты: " + coins_line(),
        _day_line(),
    ]
    why = late_bot.live_blocked() if late_bot.live_coins() and not runtime_state.get("dry_run") else None
    if why:
        lines.append(f"⚠️ Реальные ставки сейчас не уходят: {why}")
    if not settings.USE_LIVE_BOOK_STREAM:
        lines.append("⚠️ USE_LIVE_BOOK_STREAM=false — без живого стакана ставок не будет.")
    lines += late_bot.feed_warnings()
    return "\n".join(lines)


def main_markup() -> InlineKeyboardMarkup:
    live_btn = (Btn("🔴 Включить LIVE", callback_data="lv") if runtime_state.get("dry_run")
                else Btn("🧪 Выключить LIVE (в DRY RUN)", callback_data="lv"))
    return _menu(
        (Btn("⚙️ Настройки", callback_data="se"), Btn("🪙 Монеты", callback_data="co")),
        (Btn("📊 Статистика", callback_data="sta"), Btn("📤 Файлы", callback_data="fx")),
        (live_btn,),
        (Btn("🔄 Обновить", callback_data="m"),),
    )


# ----------------------------------------------------------------- монеты ----

def coins_text() -> str:
    lines = ["🪙 Монеты",
             "✅ — бот ставит по монете, ⏸ — нет (окна всё равно пишутся — чтобы потом выбрать монеты по архивам).",
             "🧪 — только тестовые ставки, 🔴 — ещё и реальные, когда бот в 🔴 LIVE.",
             "💵 — сумма входа монеты: общая (⚙️), своя фиксированная $ или % от банка.",
             "🏦 — у монеты свой банк: растёт и падает только от её реальных ставок (задаётся в 💵).",
             ""] + late_bot.coin_lines() + ["", "Режим: " + late_bot.mode_text()]
    warn = late_bot.banks_warning()
    if warn:
        lines.append(warn)
    return "\n".join(lines)


def coins_markup() -> InlineKeyboardMarkup:
    rows = []
    for a in settings.ASSETS:
        on, live = runtime_state.is_asset_enabled(a), runtime_state.is_asset_live(a)
        rows.append((Btn(f"{a.upper()} {'✅ вкл' if on else '⏸ выкл'}", callback_data=f"c:on:{a}"),
                     Btn(f"{'🔴 реально' if live else '🧪 тест'}", callback_data=f"c:lv:{a}"),
                     Btn(f"💵 {late_bot.stake_button(a)}", callback_data=f"cs:{a}")))
    rows.append((Btn("🔄 Обновить", callback_data="co"), BACK))
    return _menu(*rows)


# ------------------------------------------------- сумма входа монеты 💵 ----

def coin_stake_text(asset: str, head: str = "") -> str:
    lb = late_bot
    mode, _value = lb.stake_rule(asset)
    a = asset.upper()
    lines = ([head, ""] if head else []) + [
        f"💵 {a} — сумма входа",
        f"Сейчас: {lb.stake_label(asset)}" + (" (как в ⚙️ Настройках)" if mode == "общая" else "") + ".",
        "",
        f"• общая — как в ⚙️ Настройках (сейчас ${lb.stake():g});",
        "• $ — своя фиксированная сумма;",
        f"• % — доля банка в момент входа: своего банка {a} (🏦), а если его нет — общего (баланс кошелька "
        "Polymarket, читается раз в минуту; не читается — заданный вручную; неизвестен — общая сумма).",
        "",
        f"🏦 Свой банк {a} — отдельный счёт монеты: задаёшь сумму, дальше он растёт и падает только от "
        f"реальных ставок {a}. Ставка не больше свободных денег банка; кончились — реальные ставки по {a} "
        "не идут (тест идёт), пока не задашь банк заново.",
        "Меньше 5 акций биржа не берёт (при цене 0.95 это ~$4.8) — ставка будет не меньше. Тестовые ставки "
        "считаются с той же суммой.",
        "",
        lb.coin_bank_line(asset),
        lb.bank_line(),
    ]
    return "\n".join(lines)


def coin_stake_markup(asset: str) -> InlineKeyboardMarkup:
    lb = late_bot
    mode, value = lb.stake_rule(asset)
    own = runtime_state.coin_bank(asset)
    return _menu(
        (Btn(_mark(mode == "общая") + f"общая (${lb.stake():g})", callback_data=f"csg:{asset}"),),
        tuple(Btn(_mark(mode == "usd" and _same(value, v)) + f"${v:g}", callback_data=f"csu:{asset}:{v:g}")
              for v in lb.STAKE_PRESETS) + (Btn("✏️ $", callback_data=f"in:cu:{asset}"),),
        tuple(Btn(_mark(mode == "pct" and _same(value, v)) + f"{v:g}%", callback_data=f"csp:{asset}:{v:g}")
              for v in lb.PCT_PRESETS) + (Btn("✏️ %", callback_data=f"in:cp:{asset}"),),
        (Btn(f"🏦 ✏️ свой банк {asset.upper()}", callback_data=f"in:cb:{asset}"),)
        + ((Btn("🏦 ✖️ убрать свой банк", callback_data=f"cbx:{asset}"),) if own else ()),
        (Btn("🏦 ✏️ общий банк вручную", callback_data=f"in:bank:{asset}"),),
        (Btn("◀️ Монеты", callback_data="co"), BACK),
    )


COIN_PROMPTS = {
    "cu": "✏️ {A}: своя сумма входа в $ — например «15» или «7.5» (от 1 до 1000).",
    "cp": "✏️ {A}: сумма входа в % от банка — например «3» или «2.5» (от 0.1 до 100).",
    "cb": "✏️ {A}: свой банк монеты в $ — например «100». С этой минуты банк {A} = эта сумма + результат её "
          "реальных ставок. Задашь заново — счёт начнётся с новой суммы. «0» или «нет» — убрать свой банк "
          "(ставки в % снова от общего банка).",
    "bank": "✏️ Общий банк для ставок в % — в $, например «200». Берётся у монет без своего банка и только "
            "если баланс кошелька прочитать нельзя. 0 — не задан.",
}


def coin_prompt_text(kind: str, asset: str, head: str = "") -> str:
    lb = late_bot
    if kind in ("cu", "cp"):
        cur = lb.stake_label(asset)
    elif kind == "cb":
        cur = lb.coin_bank_short(asset)
    else:
        manual = float(runtime_state.get("late_bank_manual") or 0)
        cur = f"${manual:g}" if manual > 0 else "не задан"
    return ((head + "\n\n") if head else "") + COIN_PROMPTS[kind].format(A=asset.upper()) + f"\nСейчас: {cur}."


def apply_coin_stake(asset: str, mode: str | None, value: float | None = None) -> str:
    runtime_state.set_coin_stake(asset, mode, value)
    return f"✅ {asset.upper()}: сумма входа — {late_bot.stake_label(asset)}."


def apply_coin_bank(asset: str, value: float | None) -> str:
    a = asset.upper()
    if not value or value <= 0:
        runtime_state.set_coin_bank(asset, None)
        return f"✅ {a}: свой банк убран — ставки в % считаются от общего банка."
    runtime_state.set_coin_bank(asset, round(value, 2))
    return f"✅ {a}: свой банк ${value:g} — с этой минуты он растёт и падает только от реальных ставок {a}."


def apply_coin_input(kind: str, asset: str, text: str) -> str:
    if kind == "cu":
        return apply_coin_stake(asset, "usd", parse_number(text.replace("$", ""), *runtime_state.STAKE_LIMITS,
                                                           allow_off=False))
    if kind == "cp":
        return apply_coin_stake(asset, "pct", parse_number(text.replace("%", ""), *runtime_state.PCT_LIMITS,
                                                           allow_off=False))
    if kind == "cb":
        return apply_coin_bank(asset, parse_number(text.replace("$", ""), *runtime_state.BANK_LIMITS))
    if kind == "bank":
        v = parse_number(text.replace("$", ""), *runtime_state.BANK_LIMITS)
        runtime_state.set("late_bank_manual", round(v, 2))
        return (f"✅ Банк вручную: ${v:g} — для ставок в %, если баланс кошелька не читается." if v > 0
                else "✅ Банк вручную не задан.")
    raise ValueError("неизвестная настройка")


# --------------------------------------------------------------- настройки ----

def settings_text(head: str = "") -> str:
    lines = ([head, ""] if head else []) + [
        "⚙️ Настройки ⏱",
        f"Правило: {late_bot.rule_text()}.",
        f"Ставка: {stake_text()}.",
        "Стратегия: " + ("✅ включена" if late_bot.enabled() else "⏸ выключена — ставок нет, окна пишутся"),
        "",
        "• Окно входа — сколько осталось до конца 5-минутного окна. Бот проверяет раз в секунду и входит на "
        "первой секунде окна, где выполнены все условия; одна ставка на окно, держим до конца.",
        "• Цена — ask стороны, которая сейчас выигрывает (цена Chainlink выше страйка — UP, ниже — DOWN). "
        "Ордер — не дороже ask + 1¢.",
        "• До страйка — насколько цена Chainlink ушла от страйка (цены на начало окна): в % и в ATR "
        "(Wilder ATR(7) по 1-минутным свечам Binance).",
        "• ✏️ — своё значение сообщением (окно, цена, %, ATR, сумма, стоп).",
        "",
        "На данных 25.09–20.10: 3:00–3:10, 0.92–0.95, ≥0.10% и ≥2 ATR — 232 ставки, 98.3% выигрышей; соседние "
        "окна (2:50–3:10, 3:00–3:20) — 96.7–97.7%. Окупаемость при цене ~0.94 — от ~95% выигрышей.",
    ]
    return "\n".join(lines)


def _window_label(a: int, b: int) -> str:
    hi, lo = max(a, b), min(a, b)
    return late_bot.mmss(lo) if hi == lo else f"{late_bot.mmss(lo)}–{late_bot.mmss(hi)}"


def settings_markup() -> InlineKeyboardMarkup:
    lb = late_bot
    cur_w = (int(runtime_state.get("late_from_sec")), int(runtime_state.get("late_to_sec")))
    pl, ph = lb.price_range()
    dist, atr, stake, stop = lb.min_dist(), lb.min_atr(), lb.stake(), lb.stop()
    return _menu(
        (Btn("⏱ ✅ включена" if lb.enabled() else "⏱ ⏸ выключена", callback_data="set"),),
        tuple(Btn(_mark(cur_w == (a, b)) + _window_label(a, b), callback_data=f"sw:{a}:{b}")
              for a, b in lb.WINDOW_PRESETS) + (Btn("✏️ окно", callback_data="in:win"),),
        tuple(Btn(_mark(_same(pl, a) and _same(ph, b)) + f"{a:.2f}–{b:.2f}", callback_data=f"sp:{a:.2f}:{b:.2f}")
              for a, b in lb.PRICE_PRESETS) + (Btn("✏️ цена", callback_data="in:price"),),
        tuple(Btn(_mark(_same(dist, v)) + f"≥{v:g}%", callback_data=f"sd:{v:g}") for v in lb.DIST_PRESETS)
        + (Btn("✏️ %", callback_data="in:dist"),),
        tuple(Btn(_mark(_same(atr, v)) + (f"≥{v:g} ATR" if v else "без ATR"), callback_data=f"sa:{v:g}")
              for v in lb.ATR_PRESETS) + (Btn("✏️ ATR", callback_data="in:atr"),),
        tuple(Btn(_mark(_same(stake, v)) + f"${v:g}", callback_data=f"su:{v:g}") for v in lb.STAKE_PRESETS)
        + (Btn("✏️ $", callback_data="in:stake"),),
        tuple(Btn(_mark(_same(stop, v)) + (f"стоп ${v:g}" if v else "без стопа"), callback_data=f"ss:{v:g}")
              for v in lb.STOP_PRESETS) + (Btn("✏️ стоп", callback_data="in:stop"),),
        (Btn("🔄 Обновить", callback_data="se"), BACK),
    )


# ------------------------------------------------- ввод своего значения ✏️ ----

_await: dict = {"kind": None}          # какое значение ждём сообщением (владелец один)

PROMPTS = {
    "win": "✏️ Окно входа — сколько осталось до конца 5-минутного окна. Пришли сообщением, например: "
           "«3:00-3:10», «2:50-3:20» или одно время «3:00» (вход в эту секунду). Можно в секундах: «180-190». "
           "Пределы — от 0:05 до 4:55.",
    "price": "✏️ Цена стороны-лидера (ask) — от и до, например «0.92-0.95» (можно в центах: «92-95»). "
             "Пределы 0.50–0.99.",
    "dist": "✏️ Минимум до страйка в процентах — например «0.1» или «0.12». 0 — без условия.",
    "atr": "✏️ Минимум до страйка в ATR — например «2» или «1.8». 0 — без условия по ATR.",
    "stake": "✏️ Сумма входа в $ — например «7» или «12.5» (от 1 до 1000). Меньше 5 акций биржа не берёт: при "
             "цене 0.95 это ~$4.8 — ставка будет не меньше.",
    "stop": "✏️ Дневной стоп реальных ставок в $ — например «40». 0 — без стопа.",
}

_NUM = r"\d+(?:[.,]\d+)?"
_OFF_WORDS = ("нет", "без", "выкл", "off", "no")


def _numbers(text: str) -> list[float]:
    return [float(x.replace(",", ".")) for x in re.findall(_NUM, text or "")]


def parse_window(text: str) -> tuple[int, int]:
    """«3:00-3:10» / «3:10-3:00» / «3:00» / «180-190» / «3.00-3.10» → (от, до) секунд до конца."""
    toks = re.findall(r"\d{1,2}:\d{1,2}|" + _NUM, text or "")
    if not 1 <= len(toks) <= 2:
        raise ValueError("нужно одно время или два: например «3:00-3:10»")
    secs = []
    for tok in toks:
        tok = tok.replace(",", ".")
        m = re.fullmatch(r"(\d{1,2}):(\d{1,2})", tok) or re.fullmatch(r"(\d)\.(\d{2})", tok)
        if m:
            mi, s = int(m.group(1)), int(m.group(2))
            if s >= 60:
                raise ValueError(f"«{tok}»: секунд больше 59")
            secs.append(mi * 60 + s)
        elif re.fullmatch(r"\d+", tok):
            secs.append(int(tok))
        else:
            raise ValueError(f"«{tok}» — непонятное время; пиши как «3:10»")
    lo_lim, hi_lim = runtime_state.WINDOW_LIMITS
    for s in secs:
        if not lo_lim <= s <= hi_lim:
            raise ValueError(f"{late_bot.mmss(s)} — вне пределов {late_bot.mmss(lo_lim)}–{late_bot.mmss(hi_lim)}")
    return max(secs), min(secs)


def parse_price(text: str) -> tuple[float, float]:
    vals = [v / 100 if v > 1 else v for v in _numbers(text)]
    if len(vals) != 2:
        raise ValueError("нужно два числа — от и до, например «0.92-0.95»")
    lo_lim, hi_lim = runtime_state.PRICE_LIMITS
    for v in vals:
        if not lo_lim - 1e-9 <= v <= hi_lim + 1e-9:
            raise ValueError(f"{v:.2f} — вне пределов {lo_lim:.2f}–{hi_lim:.2f}")
    return round(min(vals), 2), round(max(vals), 2)


def parse_number(text: str, lo: float, hi: float, allow_off: bool = True) -> float:
    low = (text or "").strip().lower()
    if allow_off and any(low.startswith(w) for w in _OFF_WORDS):
        return 0.0
    vals = _numbers(low)
    if len(vals) != 1:
        raise ValueError("нужно одно число")
    v = vals[0]
    if not lo <= v <= hi:
        raise ValueError(f"{v:g} — вне пределов {lo:g}–{hi:g}")
    return v


def _rule_changed() -> str:
    late_bot.mark_rule_change()
    return " Итоги «по нынешнему правилу» — с этого момента."


def apply_window(a: int, b: int) -> str:
    runtime_state.set("late_from_sec", int(max(a, b)))
    runtime_state.set("late_to_sec", int(min(a, b)))
    return f"✅ Окно входа: {late_bot.window_phrase()}." + _rule_changed()


def apply_price(lo: float, hi: float) -> str:
    runtime_state.set("late_min_price", min(lo, hi))
    runtime_state.set("late_max_price", max(lo, hi))
    pl, ph = late_bot.price_range()
    return f"✅ Цена: {pl:.2f}–{ph:.2f}." + _rule_changed()


def apply_dist(v: float) -> str:
    runtime_state.set("late_min_dist", round(v, 4))
    return (f"✅ До страйка: не меньше {late_bot.min_dist():g}%." if v > 0 else "✅ Без условия по % до страйка.") \
        + _rule_changed()


def apply_atr(v: float) -> str:
    runtime_state.set("late_min_atr", round(v, 3))
    return (f"✅ До страйка: не меньше {late_bot.min_atr():g} ATR." if v > 0 else "✅ Без условия по ATR.") \
        + _rule_changed()


def apply_stake(v: float) -> str:
    runtime_state.set("late_stake", round(v, 2))
    return f"✅ Сумма входа: ${late_bot.stake():g} (не меньше 5 акций)."


def apply_stop(v: float) -> str:
    runtime_state.set("late_stop", round(v, 2))
    return f"✅ Дневной стоп реальных ставок: −${late_bot.stop():g}." if v > 0 else "✅ Без дневного стопа."


def apply_input(kind: str, text: str) -> str:
    """Своё значение из сообщения → строка-итог. ValueError — с понятной причиной."""
    if kind == "win":
        return apply_window(*parse_window(text))
    if kind == "price":
        return apply_price(*parse_price(text))
    if kind == "dist":
        return apply_dist(parse_number(text.replace("%", ""), 0.0, 5.0))
    if kind == "atr":
        return apply_atr(parse_number(text, 0.0, 20.0))
    if kind == "stake":
        return apply_stake(parse_number(text.replace("$", ""), *runtime_state.STAKE_LIMITS, allow_off=False))
    if kind == "stop":
        return apply_stop(parse_number(text.replace("$", ""), 0.0, 100000.0))
    raise ValueError("неизвестная настройка")


def _current_value(kind: str) -> str:
    lb = late_bot
    pl, ph = lb.price_range()
    return {"win": lb.window_phrase(), "price": f"{pl:.2f}–{ph:.2f}",
            "dist": f"≥{lb.min_dist():g}%" if lb.min_dist() > 0 else "без условия",
            "atr": lb.atr_text(), "stake": f"${lb.stake():g}",
            "stop": f"−${lb.stop():g}" if lb.stop() > 0 else "без стопа"}.get(kind, "")


def prompt_text(kind: str, head: str = "") -> str:
    return ((head + "\n\n") if head else "") + PROMPTS[kind] + f"\nСейчас: {_current_value(kind)}."


def _prompt_markup() -> InlineKeyboardMarkup:
    return _menu((Btn("❌ Отмена", callback_data="inx"),))


def handle_text(text: str) -> tuple[str, InlineKeyboardMarkup]:
    """Сообщение от владельца: значение для ✏️ (если ждём) или просто главное меню."""
    kind = _await.get("kind")
    if not kind:
        return main_text(), main_markup()
    if ":" in kind:                       # сумма / банк монеты: «cu:btc», «cp:eth», «cb:btc», «bank:sol»
        ckind, asset = kind.split(":", 1)
        try:
            head = apply_coin_input(ckind, asset, text)
        except ValueError as exc:
            return (coin_prompt_text(ckind, asset, f"⚠️ Не понял: {exc}. Попробуй ещё раз или нажми «Отмена»."),
                    _menu((Btn("❌ Отмена", callback_data=f"cs:{asset}"),)))
        _await["kind"] = None
        return coin_stake_text(asset, head), coin_stake_markup(asset)
    try:
        head = apply_input(kind, text)
    except ValueError as exc:
        return prompt_text(kind, f"⚠️ Не понял: {exc}. Попробуй ещё раз или нажми «Отмена»."), _prompt_markup()
    _await["kind"] = None
    return settings_text(head), settings_markup()


# ---------------------------------------------------------------- режим LIVE ----

def confirm_live_text() -> str:
    lb = late_bot
    coins = lb.live_coins()
    lines = ["⚠️ Включить LIVE? Бот начнёт ставить реальные деньги на Polymarket.", ""]
    if coins:
        lines.append(f"Реальные ставки пойдут по: {', '.join(a.upper() for a in coins)} — {stake_text()}; "
                     f"правило: {lb.rule_short()}.")
        test = [a.upper() for a in lb.trade_assets() if a not in coins]
        if test:
            lines.append(f"Остальные включённые монеты ({', '.join(test)}) — только тест.")
        if not lb.enabled():
            lines.append("⚠️ Стратегия сейчас ⏸ выключена — ставок не будет, пока не включишь в ⚙️ Настройках.")
    else:
        lines.append("Ни одна включённая монета не помечена 🔴 — реальных ставок не будет, пока не пометишь "
                     "в 🪙 Монетах.")
    return "\n".join(lines)


def _confirm_live_markup() -> InlineKeyboardMarkup:
    return _menu((Btn("✅ Да, включить LIVE", callback_data="lvy"), Btn("❌ Отмена", callback_data="m")))


NO_KEY_TEXT = ("❌ Нельзя включить LIVE: POLY_PRIVATE_KEY не задан в переменных окружения. "
               "Добавь ключ и передеплой бота.")


# -------------------------------------------------------------- обработка ----

def _float(s: str) -> float | None:
    try:
        return float(s)
    except ValueError:
        return None


def _handle_settings(cmd: str, parts: list[str]) -> tuple[str, InlineKeyboardMarkup] | None:
    lb = late_bot
    head = ""
    if cmd == "set":
        runtime_state.set("late_enabled", not lb.enabled())
        head = "⏱ Стратегия ✅ включена." if lb.enabled() else "⏱ Стратегия ⏸ выключена — ставок нет, окна пишутся."
    elif cmd == "sw" and len(parts) == 3:
        try:
            a, b = int(parts[1]), int(parts[2])
        except ValueError:
            a = b = -1
        cur = (int(runtime_state.get("late_from_sec")), int(runtime_state.get("late_to_sec")))
        if (a, b) in lb.WINDOW_PRESETS and (a, b) != cur:
            head = apply_window(a, b)
    elif cmd == "sp" and len(parts) == 3:
        a, b = _float(parts[1]), _float(parts[2])
        if (a is not None and b is not None and any(_same(a, x) and _same(b, y) for x, y in lb.PRICE_PRESETS)
                and (a, b) != lb.price_range()):
            head = apply_price(a, b)
    elif cmd in ("sd", "sa", "su", "ss") and len(parts) == 2:
        v = _float(parts[1])
        presets, current, fn = {"sd": (lb.DIST_PRESETS, lb.min_dist(), apply_dist),
                                "sa": (lb.ATR_PRESETS, lb.min_atr(), apply_atr),
                                "su": (lb.STAKE_PRESETS, lb.stake(), apply_stake),
                                "ss": (lb.STOP_PRESETS, lb.stop(), apply_stop)}[cmd]
        if v is not None and any(_same(v, x) for x in presets) and not _same(v, current):
            head = fn(v)
    else:
        return None
    return settings_text(head), settings_markup()


async def handle(data: str) -> tuple[str, InlineKeyboardMarkup]:
    """Нажатие кнопки → (текст, кнопки) экрана, который показать."""
    parts = data.split(":")
    cmd = parts[0]
    if cmd == "in" and len(parts) == 2 and parts[1] in PROMPTS:
        _await["kind"] = parts[1]
        return prompt_text(parts[1]), _prompt_markup()
    if cmd == "in" and len(parts) == 3 and parts[1] in COIN_PROMPTS and parts[2] in settings.ASSETS:
        _await["kind"] = f"{parts[1]}:{parts[2]}"
        return coin_prompt_text(parts[1], parts[2]), _menu((Btn("❌ Отмена", callback_data=f"cs:{parts[2]}"),))
    _await["kind"] = None                         # любая другая кнопка — ввод отменён
    if cmd == "inx":
        return settings_text("Ввод отменён."), settings_markup()
    if cmd == "se":
        return settings_text(), settings_markup()
    res = _handle_settings(cmd, parts)
    if res is not None:
        return res
    if cmd == "co":
        return coins_text(), coins_markup()
    if cmd in ("cs", "csg", "csu", "csp", "cbx") and len(parts) >= 2 and parts[1] in settings.ASSETS:
        asset, head = parts[1], ""
        if cmd == "cbx" and runtime_state.coin_bank(asset):
            head = apply_coin_bank(asset, None)
        elif cmd == "csg" and runtime_state.coin_stake(asset):
            head = apply_coin_stake(asset, None)
        elif cmd in ("csu", "csp") and len(parts) == 3:
            v = _float(parts[2])
            presets, mode = (late_bot.STAKE_PRESETS, "usd") if cmd == "csu" else (late_bot.PCT_PRESETS, "pct")
            if v is not None and any(_same(v, x) for x in presets) and runtime_state.coin_stake(asset) != (mode, v):
                head = apply_coin_stake(asset, mode, v)
        return coin_stake_text(asset, head), coin_stake_markup(asset)
    if cmd == "c" and len(parts) == 3 and parts[2] in settings.ASSETS:
        asset, head = parts[2], ""
        if parts[1] == "on":
            on = runtime_state.toggle_asset(asset)
            head = f"{asset.upper()}: {'✅ включена' if on else '⏸ выключена — ставок по ней не будет'}.\n\n"
        elif parts[1] == "lv":
            live = not runtime_state.is_asset_live(asset)
            runtime_state.set_asset_live(asset, live)
            if live:
                head = f"{asset.upper()}: 🔴 реальные ставки"
                if runtime_state.get("dry_run"):
                    head += " — пойдут после «🔴 Включить LIVE» в главном меню"
                if not runtime_state.is_asset_enabled(asset):
                    head += "; монета ⏸ выключена — сначала включи"
                head += ".\n\n"
            else:
                head = f"{asset.upper()}: 🧪 только тест.\n\n"
        return head + coins_text(), coins_markup()
    if cmd == "sta":
        return late_bot.stats_text(), _menu((Btn("🔄 Обновить", callback_data="sta"), BACK))
    if cmd == "fx":
        from src import reporting
        try:
            n = await reporting.send_all()
            head = ("📤 Отправил файлы: окна всех монет (с путём цены) и ставки — всё, что в базе." if n
                    else "📤 В базе пока пусто — нечего выгружать.")
        except Exception as exc:  # noqa: BLE001
            head = f"⚠️ Файлы не ушли: {exc}"[:300]
        return head + "\n\n" + main_text(), main_markup()
    if cmd == "lv":
        if not runtime_state.get("dry_run"):
            runtime_state.set("dry_run", True)
            return "🧪 Переключено в DRY RUN — реальные ставки остановлены.\n\n" + main_text(), main_markup()
        if not settings.POLY_PRIVATE_KEY:
            return NO_KEY_TEXT, main_markup()
        return confirm_live_text(), _confirm_live_markup()
    if cmd == "lvy":
        if not settings.POLY_PRIVATE_KEY:
            return NO_KEY_TEXT, main_markup()
        runtime_state.set("dry_run", False)
        return "🔴 LIVE включён.\n\n" + main_text(), main_markup()
    return main_text(), main_markup()


def _owner_chat_id() -> int | None:
    try:
        return int(str(settings.TELEGRAM_CHAT_ID).strip())
    except (TypeError, ValueError):
        return None


def _is_owner(update: Update) -> bool:
    """Управлять ботом может только чат из TELEGRAM_CHAT_ID (туда же идут уведомления).
    Не задан — без ограничения, чтобы не запереть владельца."""
    owner = _owner_chat_id()
    if owner is None:
        return True
    chat = getattr(update, "effective_chat", None)
    return chat is not None and getattr(chat, "id", None) == owner


async def _cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    _await["kind"] = None
    await update.message.reply_text(_fit(main_text()), reply_markup=main_markup())


async def _cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    _await["kind"] = None
    await update.message.reply_text(_fit(settings_text()), reply_markup=settings_markup())


async def _cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_fit(late_bot.stats_text()))


async def _on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update) or update.message is None:
        return
    text, markup = handle_text(update.message.text or "")
    await update.message.reply_text(_fit(text), reply_markup=markup)


async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not _is_owner(update):
        return
    text, markup = await handle(query.data or "m")
    try:
        await query.edit_message_text(_fit(text), reply_markup=markup)
    except Exception as exc:  # noqa: BLE001 — повторное нажатие той же кнопки: «message is not modified»
        if "not modified" not in str(exc).lower():
            raise


def build_app() -> Application:
    global _app
    _app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    for name in ("start", "menu", "status"):
        _app.add_handler(CommandHandler(name, _cmd_menu))
    _app.add_handler(CommandHandler("settings", _cmd_settings))
    _app.add_handler(CommandHandler("stats", _cmd_stats))
    _app.add_handler(CallbackQueryHandler(_on_callback))
    _app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _on_text))
    return _app


def startup_text(forced_dry: bool = False) -> str:
    lines = ["🤖 Бот запущен — ⏱ поздний вход 5m.", "Режим: " + late_bot.mode_text(),
             "Монеты: " + coins_line() + " (✅ вкл, 🧪 тест, 🔴 реально)",
             "Правило: " + ("✅ " if late_bot.enabled() else "⏸ выключено, ") + late_bot.rule_short()
             + f"; {stake_text()}."]
    if forced_dry:
        lines.append("⚠️ Был сохранён LIVE, но POLY_PRIVATE_KEY не задан — откатил в DRY RUN.")
    if storage.DB_WAS_NEW:
        lines.append(f"⚠️ База создана заново ({settings.DB_PATH}). Если бот уже работал — данные не сохраняются "
                     "между деплоями: подключи постоянный том (Persistent Storage) на /app/data. "
                     "Сейчас режим, монеты и настройки — по умолчанию, статистика пустая.")
    if not settings.USE_LIVE_BOOK_STREAM:
        lines.append("⚠️ USE_LIVE_BOOK_STREAM=false — без живого стакана ставок не будет.")
    lines.append("/menu — управление, /settings — настройки.")
    return "\n".join(lines)


async def notify(text: str) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID or _app is None:
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=_fit(text))


async def send_document(path: str, caption: str | None) -> None:
    """Файл (CSV / zip-отчёт) в чат."""
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID or _app is None:
        return
    with open(path, "rb") as f:
        await _app.bot.send_document(chat_id=settings.TELEGRAM_CHAT_ID, document=f, caption=caption)
