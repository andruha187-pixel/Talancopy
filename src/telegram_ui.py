"""
Telegram-интерфейс копи-бота: кнопочное меню + ввод своих значений.

Управление ТОЛЬКО из чата TELEGRAM_CHAT_ID (бот торгует деньгами — чужие
не должны иметь доступ к кнопкам, даже если найдут бота).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

from telegram import InlineKeyboardButton as Btn
from telegram import InlineKeyboardMarkup, LinkPreviewOptions, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config import settings
from src import book_stream, copier, db, notifier, positions, reports, state, trader
from src.detector import detector
from src.discovery import scanner
from src.sources import chain, poller, rtds
from src.stats import today_start, wallet_stats
from src.util import cut, esc, find_addr, fmt_age, fmt_ts, fnum, money, norm_addr, now_ms, short_addr

log = logging.getLogger("telegram")

_app: Application | None = None
_pending: dict | None = None
_last_top: list[dict] = []
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

MODE_ICON = {"off": "⏸", "paper": "🧪", "live": "🔴"}
MODE_RU = {"off": "выключен", "paper": "виртуально", "live": "реально"}

# поле -> (название, тип, мин, макс, единица)
FIELDS = {
    "size_pct": ("% от их входа", float, 0.1, 1000, "%"),
    "size_fixed": ("фиксированная сумма", float, 1, 1_000_000, "$"),
    "max_usdc": ("максимум на одну сделку", float, 1, 1_000_000, "$"),
    "min_target_usdc": ("минимум их сделки", float, 0, 10_000_000, "$"),
    "max_position_usdc": ("максимум в один рынок", float, 1, 10_000_000, "$"),
    "max_open": ("максимум открытых позиций", int, 1, 1000, ""),
    "daily_loss_limit": ("дневной стоп (реальные)", float, 0, 10_000_000, "$"),
    "max_slip_cents": ("проскальзывание в центах", float, 0, 50, "¢"),
    "max_slip_pct": ("проскальзывание в %", float, 0, 100, "%"),
    "min_price": ("минимальная цена входа", float, 0.001, 0.99, ""),
    "max_price": ("максимальная цена входа", float, 0.01, 0.999, ""),
    "sl_pct": ("стоп-лосс", float, 0, 99, "%"),
    "tp_pct": ("тейк-профит", float, 0, 10000, "%"),
    "trail_pct": ("трейлинг-стоп", float, 0, 99, "%"),
    "max_hold_h": ("максимальное удержание", float, 0, 100000, "ч"),
    "min_minutes_left": ("минимум минут до конца рынка", float, 0, 1_000_000, "мин"),
}
PRESETS = {
    "size_pct": [1, 5, 10, 25, 50, 100],
    "size_fixed": [1, 2, 5, 10, 25, 50],
    "max_usdc": [5, 10, 25, 50, 100, 250],
    "max_position_usdc": [25, 50, 100, 250, 500, 1000],
    "min_target_usdc": [0, 10, 50, 100, 500, 1000],
    "max_open": [5, 10, 20, 50],
    "daily_loss_limit": [0, 10, 25, 50, 100],
    "max_slip_cents": [1, 2, 3, 5],
    "max_slip_pct": [5, 10, 20, 50],
    "sl_pct": [0, 20, 30, 50],
    "tp_pct": [0, 25, 50, 100],
    "trail_pct": [0, 15, 25, 40],
    "max_hold_h": [0, 24, 72, 168],
    "min_minutes_left": [0, 15, 30, 60, 240],
}
PRICE_RANGES = [(0.03, 0.97), (0.05, 0.95), (0.10, 0.90), (0.20, 0.80)]
GLOBAL_FIELDS = {
    "daily_loss_limit_live": ("общий дневной стоп по реальным", 0, 10_000_000, "$", [0, 25, 50, 100, 250]),
    "max_live_exposure": ("потолок суммы открытых реальных позиций", 0, 10_000_000, "$", [0, 50, 100, 250, 500]),
    "exit_slip_cents": ("проскальзывание на выходе", 0, 50, "¢", [2, 5, 10]),
}


def _fv(value, unit: str = "") -> str:
    v = fnum(value)
    s = f"{v:g}" if abs(v - round(v)) > 1e-9 else f"{int(round(v))}"
    if unit == "$":
        return f"${s}"
    return f"{s}{unit}"


def _off_or(value, unit: str) -> str:
    return "выкл" if fnum(value) <= 0 else _fv(value, unit)


# --------------------------------------------------------------- отправка ----

async def send_text(text: str, markup=None) -> None:
    if _app is None or not settings.TELEGRAM_CHAT_ID:
        log.info("TG: %s", text[:200])
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text[:4096], parse_mode="HTML",
                                reply_markup=markup, link_preview_options=NO_PREVIEW)


async def send_document(path: str, caption: str | None = None) -> None:
    if _app is None or not settings.TELEGRAM_CHAT_ID:
        log.info("TG документ: %s", path)
        return
    extra = None
    if caption and len(caption) > 1000:
        cut_at = caption.rfind("\n", 0, 1000)
        cut_at = cut_at if cut_at > 0 else 1000
        caption, extra = caption[:cut_at], caption[cut_at:]
    with open(path, "rb") as f:
        await _app.bot.send_document(chat_id=settings.TELEGRAM_CHAT_ID, document=f,
                                     filename=os.path.basename(path), caption=caption)
    if extra:
        notifier.notify(esc(extra.strip()))


async def _edit(query, text: str, markup=None) -> None:
    try:
        await query.edit_message_text(text[:4096], parse_mode="HTML", reply_markup=markup,
                                      link_preview_options=NO_PREVIEW)
    except Exception as exc:  # noqa: BLE001
        if "not modified" in str(exc).lower():
            return
        # Сообщение слишком старое/удалено — шлём новое.
        try:
            await query.message.reply_text(text[:4096], parse_mode="HTML", reply_markup=markup,
                                           link_preview_options=NO_PREVIEW)
        except Exception as exc2:  # noqa: BLE001
            log.warning("Не удалось показать меню: %s / %s", exc, exc2)


async def _reply(message, text: str, markup=None) -> None:
    await message.reply_text(text[:4096], parse_mode="HTML", reply_markup=markup, link_preview_options=NO_PREVIEW)


def _allowed(update: Update) -> bool:
    chat = update.effective_chat
    if not settings.TELEGRAM_CHAT_ID:
        return True
    return chat is not None and str(chat.id) == str(settings.TELEGRAM_CHAT_ID)


# --------------------------------------------------------------- главное ----

def _sources_line() -> str:
    parts = []
    if chain.listeners:
        ok = any(lst.connected for lst in chain.listeners)
        parts.append(f"⛓ {'✅' if ok else '❌'}")
    if settings.RTDS_ENABLED:
        fresh = rtds.stats["connected"] and now_ms() - rtds.stats["last_msg_ms"] < 60000
        parts.append(f"📡 {'✅' if fresh else '❌'}")
    if settings.POLL_ENABLED:
        fresh = poller.stats["last_ok_ms"] and now_ms() - poller.stats["last_ok_ms"] < 60000
        parts.append(f"🔁 {'✅' if fresh or not state.active_addresses() else '❌'}")
    return " · ".join(parts) or "—"


def main_text() -> str:
    ws = state.wallets()
    counts = {m: sum(1 for w in ws if w.mode == m) for m in ("paper", "live", "off")}
    paper_today = positions.realized_today(None, "paper") + positions.unrealized_total(None, "paper")
    live_today = positions.realized_today(None, "live")
    lines = [
        "🤖 <b>Копи-бот Polymarket</b>",
        f"Статус: {'⏸ пауза (выходы работают)' if state.get('paused') else '▶️ работает'}",
        f"Реальная торговля: {'🔴 ВКЛЮЧЕНА' if state.get('live_enabled') else '⚪ выключена'}"
        + ("" if trader.live_possible() else " (нет POLY_PRIVATE_KEY)"),
        f"Кошельков: {len(ws)} (🧪 {counts['paper']}, 🔴 {counts['live']}, ⏸ {counts['off']})",
        f"Открыто: 🧪 {len(positions.open_positions(mode='paper'))} · 🔴 {len(positions.open_positions(mode='live'))}",
        f"Сегодня: 🧪 {money(paper_today, True)} · 🔴 реализовано {money(live_today, True)}",
        f"Источники: {_sources_line()}",
    ]
    if scanner.progress["running"]:
        lines.append(scanner.status_text())
    return "\n".join(lines)


def main_markup() -> InlineKeyboardMarkup:
    rows = [
        [Btn("👛 Кошельки", callback_data="m:wallets"), Btn("➕ Добавить", callback_data="m:add")],
        [Btn("📊 Статистика", callback_data="m:stats"), Btn("📂 Позиции", callback_data="m:pos:0")],
        [Btn("🔎 Поиск кошельков", callback_data="m:disc")],
        [Btn("⚙️ Настройки", callback_data="m:settings"), Btn("🩺 Источники", callback_data="m:health")],
        [Btn("📄 Отчёт сейчас", callback_data="m:report")],
        [Btn("▶️ Старт" if state.get("paused") else "⏸ Пауза", callback_data="t:pause")],
        [Btn("⚪ Выключить реальную торговлю" if state.get("live_enabled") else "🔴 Включить реальную торговлю",
             callback_data="t:live")],
    ]
    return InlineKeyboardMarkup(rows)


# --------------------------------------------------------------- кошельки ----

def wallets_markup() -> InlineKeyboardMarkup:
    rows = []
    for w in state.wallets():
        n_open = len(positions.open_positions(w.id))
        rows.append([Btn(f"{MODE_ICON.get(w.mode, '?')} {cut(w.short(), 24)} · откр. {n_open}",
                         callback_data=f"w:{w.id}")])
    rows.append([Btn("➕ Добавить кошелёк", callback_data="m:add")])
    rows.append([Btn("◀️ Меню", callback_data="m:main")])
    return InlineKeyboardMarkup(rows)


def _size_line(w) -> str:
    if w.size_mode == "fixed":
        s = f"фиксированно {_fv(w.size_fixed, '$')} за сделку"
    else:
        s = f"{_fv(w.size_pct, '%')} от их входа"
    return f"{s} (макс {_fv(w.max_usdc, '$')} за сделку, {_fv(w.max_position_usdc, '$')} на рынок)"


def wallet_text(w, s_paper: dict | None = None, s_live: dict | None = None) -> str:
    lines = [
        f"{MODE_ICON.get(w.mode)} <b>{esc(w.short())}</b> — {MODE_RU.get(w.mode)}",
        f"<code>{w.address}</code>",
        f"<a href=\"https://polymarket.com/profile/{w.address}\">профиль на Polymarket</a>",
        "",
        f"💵 Размер: {_size_line(w)}",
        f"Их сделки меньше {_fv(w.min_target_usdc, '$')} копятся по рынку (серия с паузами ≤10 мин), "
        f"копирую, когда наберётся; доли &lt;$1 "
        f"{'копятся до $1' if w.accumulate_small else 'пропускаются'}",
        f"🎯 Цена: не хуже их +{_fv(w.max_slip_cents, '¢')} и +{_fv(w.max_slip_pct, '%')}; "
        f"входы только {w.min_price:g}–{w.max_price:g}",
        f"🚪 Выходы: зеркальные продажи {'✅' if w.copy_sells else '❌'} · докупки {'✅' if w.copy_adds else '❌'}"
        f" · SL {_off_or(w.sl_pct, '%')} · TP {_off_or(w.tp_pct, '%')} · трейлинг {_off_or(w.trail_pct, '%')}"
        f" · удержание {_off_or(w.max_hold_h, 'ч')}",
        f"🧹 Фильтры: быстрые рынки {'пропуск' if w.skip_fast else 'копирую'}"
        + (f" (и &lt;{_fv(w.min_minutes_left)} мин до конца)" if w.skip_fast and w.min_minutes_left else "")
        + f" · лайв-спорт {'пропуск' if w.skip_live_sports else 'копирую'}"
        + f" · их лимитки {'копирую' if w.copy_maker_fills else 'пропуск'}",
        f"🛡 Лимиты: открытых ≤ {w.max_open} · дневной стоп {_off_or(w.daily_loss_limit, '$')}",
        f"🔔 Уведомления: {'вкл' if w.notify else 'выкл'}",
    ]
    for title, s in (("🧪 Виртуально", s_paper), ("🔴 Реально", s_live)):
        if s and (s["copies"] or s["open"] or s["realized"]):
            roi = f" (ROI {s['roi']:+.1f}%)" if s["roi"] is not None else ""
            lines.append(f"{title}: копий {s['copies']}, PnL {money(s['total'], True)}{roi}, "
                         f"открыто {s['open']} ({money(s['open_cost'])})")
    if w.mode == "live" and not state.get("live_enabled"):
        lines.append("\n⚠️ Общий выключатель реальной торговли ВЫКЛЮЧЕН — этот кошелёк сейчас не торгует.")
    return "\n".join(lines)


def wallet_markup(w) -> InlineKeyboardMarkup:
    wid = w.id

    def mode_btn(m: str, title: str) -> Btn:
        return Btn(("✅ " if w.mode == m else "") + title, callback_data=f"w:{wid}:mode:{m}")

    rows = [
        [mode_btn("off", "⏸ Выкл"), mode_btn("paper", "🧪 Вирт."), mode_btn("live", "🔴 Реал.")],
        [Btn("💵 Размер", callback_data=f"w:{wid}:size"), Btn("🛡 Лимиты", callback_data=f"w:{wid}:lim")],
        [Btn("🎯 Цена", callback_data=f"w:{wid}:slip"), Btn("🚪 Выходы", callback_data=f"w:{wid}:exit")],
        [Btn("🧹 Фильтры", callback_data=f"w:{wid}:flt"), Btn("📊 Статистика", callback_data=f"w:{wid}:st")],
        [Btn("📂 Позиции", callback_data=f"w:{wid}:pos"), Btn("🔬 Разобрать", callback_data=f"w:{wid}:an")],
        [Btn(f"🔔 Увед.: {'вкл' if w.notify else 'выкл'}", callback_data=f"w:{wid}:tg:notify"),
         Btn("✏️ Имя", callback_data=f"w:{wid}:ren")],
        [Btn("🗑 Удалить", callback_data=f"w:{wid}:del"), Btn("◀️ К списку", callback_data="m:wallets")],
    ]
    return InlineKeyboardMarkup(rows)


def _preset_row(wid: int, field: str, current, unit: str) -> list[Btn]:
    out = []
    for v in PRESETS[field]:
        mark = "✅ " if abs(fnum(current) - v) < 1e-9 else ""
        label = "выкл" if v == 0 and field in ("sl_pct", "tp_pct", "trail_pct", "max_hold_h", "daily_loss_limit",
                                                "min_target_usdc", "min_minutes_left") else _fv(v, unit)
        out.append(Btn(mark + label, callback_data=f"w:{wid}:set:{field}:{v}"))
    return out


def _custom_btn(wid: int, field: str) -> Btn:
    return Btn(f"✏️ Своё: {FIELDS[field][0]}", callback_data=f"w:{wid}:in:{field}")


def size_markup(w) -> InlineKeyboardMarkup:
    wid = w.id
    rows = [[Btn(("✅ " if w.size_mode == "pct" else "") + "% от их входа", callback_data=f"w:{wid}:sm:pct"),
             Btn(("✅ " if w.size_mode == "fixed" else "") + "Фиксированная $", callback_data=f"w:{wid}:sm:fixed")]]
    if w.size_mode == "pct":
        rows.append([Btn("— процент от суммы их входа —", callback_data="noop")])
        rows.append(_preset_row(wid, "size_pct", w.size_pct, "%"))
        rows.append([_custom_btn(wid, "size_pct")])
    else:
        rows.append([Btn("— сумма каждой нашей сделки —", callback_data="noop")])
        rows.append(_preset_row(wid, "size_fixed", w.size_fixed, "$"))
        rows.append([_custom_btn(wid, "size_fixed")])
    rows.append([Btn("— максимум на одну сделку —", callback_data="noop")])
    rows.append(_preset_row(wid, "max_usdc", w.max_usdc, "$"))
    rows.append([_custom_btn(wid, "max_usdc")])
    rows.append([Btn(("✅ " if w.accumulate_small else "❌ ") + "Копить доли меньше $1",
                     callback_data=f"w:{wid}:tg:accumulate_small")])
    rows.append([Btn("◀️ Назад", callback_data=f"w:{wid}")])
    return InlineKeyboardMarkup(rows)


def limits_markup(w) -> InlineKeyboardMarkup:
    wid = w.id
    rows = [
        [Btn("— максимум в один рынок —", callback_data="noop")],
        _preset_row(wid, "max_position_usdc", w.max_position_usdc, "$"),
        [Btn("— не копировать их сделки меньше —", callback_data="noop")],
        _preset_row(wid, "min_target_usdc", w.min_target_usdc, "$"),
        [Btn("— максимум открытых позиций —", callback_data="noop")],
        _preset_row(wid, "max_open", w.max_open, ""),
        [Btn("— дневной стоп (реальные) —", callback_data="noop")],
        _preset_row(wid, "daily_loss_limit", w.daily_loss_limit, "$"),
        [_custom_btn(wid, "max_position_usdc")],
        [_custom_btn(wid, "min_target_usdc")],
        [_custom_btn(wid, "daily_loss_limit")],
        [Btn("◀️ Назад", callback_data=f"w:{wid}")],
    ]
    return InlineKeyboardMarkup(rows)


def slip_markup(w) -> InlineKeyboardMarkup:
    wid = w.id
    rows = [
        [Btn("— не дороже их цены больше чем на (центов) —", callback_data="noop")],
        _preset_row(wid, "max_slip_cents", w.max_slip_cents, "¢"),
        [Btn("— и больше чем на (%) —", callback_data="noop")],
        _preset_row(wid, "max_slip_pct", w.max_slip_pct, "%"),
        [Btn("— копировать входы только по цене —", callback_data="noop")],
        [Btn(("✅ " if abs(w.min_price - lo) < 1e-9 and abs(w.max_price - hi) < 1e-9 else "") + f"{lo:g}–{hi:g}",
             callback_data=f"w:{wid}:pr:{lo}:{hi}") for lo, hi in PRICE_RANGES],
        [_custom_btn(wid, "max_slip_cents")],
        [_custom_btn(wid, "min_price"), _custom_btn(wid, "max_price")],
        [Btn("◀️ Назад", callback_data=f"w:{wid}")],
    ]
    return InlineKeyboardMarkup(rows)


def exits_markup(w) -> InlineKeyboardMarkup:
    wid = w.id
    rows = [
        [Btn(("✅ " if w.copy_sells else "❌ ") + "Зеркальные продажи", callback_data=f"w:{wid}:tg:copy_sells"),
         Btn(("✅ " if w.copy_adds else "❌ ") + "Докупки", callback_data=f"w:{wid}:tg:copy_adds")],
        [Btn("— стоп-лосс (от нашей цены входа) —", callback_data="noop")],
        _preset_row(wid, "sl_pct", w.sl_pct, "%"),
        [Btn("— тейк-профит —", callback_data="noop")],
        _preset_row(wid, "tp_pct", w.tp_pct, "%"),
        [Btn("— трейлинг-стоп (от максимума) —", callback_data="noop")],
        _preset_row(wid, "trail_pct", w.trail_pct, "%"),
        [Btn("— закрыть через (часов) —", callback_data="noop")],
        _preset_row(wid, "max_hold_h", w.max_hold_h, "ч"),
        [_custom_btn(wid, "sl_pct"), _custom_btn(wid, "tp_pct")],
        [_custom_btn(wid, "trail_pct"), _custom_btn(wid, "max_hold_h")],
        [Btn("◀️ Назад", callback_data=f"w:{wid}")],
    ]
    return InlineKeyboardMarkup(rows)


def filters_markup(w) -> InlineKeyboardMarkup:
    wid = w.id
    rows = [
        [Btn(("✅ " if w.skip_fast else "❌ ") + "Пропускать быстрые рынки (5м/15м/час)",
             callback_data=f"w:{wid}:tg:skip_fast")],
        [Btn("— и рынки, где до конца меньше (мин) —", callback_data="noop")],
        _preset_row(wid, "min_minutes_left", w.min_minutes_left, ""),
        [Btn(("✅ " if w.skip_live_sports else "❌ ") + "Пропускать входы в идущий матч",
             callback_data=f"w:{wid}:tg:skip_live_sports")],
        [Btn(("✅ " if w.copy_maker_fills else "❌ ") + "Копировать исполнения их лимиток",
             callback_data=f"w:{wid}:tg:copy_maker_fills")],
        [_custom_btn(wid, "min_minutes_left")],
        [Btn("◀️ Назад", callback_data=f"w:{wid}")],
    ]
    return InlineKeyboardMarkup(rows)


SUBMENU_TEXT = {
    "size": "💵 <b>Размер копии</b>\n«% от их входа»: они зашли на $1000, стоит 5% — мы заходим на $50. "
            "«Фиксированная»: каждая копия на одну и ту же сумму.\nМинимальный ордер Polymarket — $1.",
    "lim": "🛡 <b>Лимиты</b>\nМаксимум в один рынок — вместе с докупками. Их мелкие сделки (меньше порога) "
           "обычно шум — пропускаем. Дневной стоп — по реальным: убыток за сутки (UTC) больше — новые входы стоп.",
    "slip": "🎯 <b>Цена входа</b>\nПотолок = их цена + проскальзывание (берётся меньшее из ¢ и %, но не меньше "
            "1 тика). Если в стакане уже дороже — не входим (они двинули цену, догонять — значит покупать хуже).",
    "exit": "🚪 <b>Выходы</b>\nЗеркальные продажи: они продали 40% — мы продаём 40%. Стоп-лосс/тейк — от нашей "
            "средней цены по лучшему bid. Трейлинг включается после того, как позиция была в плюсе.",
    "flt": "🧹 <b>Фильтры рынков</b>\nБыстрые крипто-рынки (5м/15м/час) и рынки перед самым концом скопировать "
           "вовремя нельзя — цена уходит за секунды.\nИсполнения их лимиток: когда кто-то покупает об их "
           "выставленную заявку, они получают цену лучше рынка, а мы, копируя, платим спред и комиссию. "
           "У кошельков, которые торгуют в основном лимитками, это лучше выключить.",
}


# --------------------------------------------------------------- статистика ----

def _stats_block(title: str, s: dict) -> list[str]:
    if not (s["copies"] or s["open"] or s["realized"] or s["skipped"] or s["failed"]):
        return [f"{title}: пока пусто"]
    lines = [f"<b>{title}</b>",
             f"Копий: {s['copies']} · пропусков: {s['skipped']} · не исполнилось: {s['failed']}",
             f"PnL: {money(s['total'], True)} (реализовано {money(s['realized'], True)}, "
             f"в открытых {money(s['unrealized'], True)})"
             + (f" · ROI {s['roi']:+.1f}%" if s["roi"] is not None else ""),
             f"Закрыто позиций: {s['closed']} (плюс {s['wins']}, минус {s['losses']}) · открыто {s['open']} "
             f"на {money(s['open_cost'])}" + (f" ({s['unmarked']} без цены)" if s["unmarked"] else ""),
             f"Комиссии: {money(s['fees'])}"
             + (f" · проскальзывание в среднем {s['avg_slip']:+.2f}¢" if s["avg_slip"] is not None else "")]
    if s["lat_median"] is not None:
        lines.append(f"Замечаем сделку через {s['lat_median'] / 1000:.1f}с после блока"
                     + (f", ордер через {s['exec_median'] / 1000:.2f}с после обнаружения"
                        if s["exec_median"] is not None else ""))
    if s["reasons"]:
        lines.append("Причины пропусков: " + "; ".join(f"{esc(k)} ×{v}" for k, v in s["reasons"]))
    return lines


async def wallet_stats_text(w) -> str:
    sp = await asyncio.to_thread(wallet_stats, w.id, "paper")
    sl = await asyncio.to_thread(wallet_stats, w.id, "live")
    tp = await asyncio.to_thread(wallet_stats, w.id, "paper", today_start())
    tl = await asyncio.to_thread(wallet_stats, w.id, "live", today_start())
    lines = [f"📊 <b>{esc(w.short())}</b>", ""]
    lines += _stats_block("🔴 Реально — всего", sl)
    if tl["copies"] or tl["realized"]:
        lines.append(f"   сегодня: копий {tl['copies']}, реализовано {money(tl['realized'], True)}")
    lines.append("")
    lines += _stats_block("🧪 Виртуально — всего", sp)
    if tp["copies"] or tp["realized"]:
        lines.append(f"   сегодня: копий {tp['copies']}, реализовано {money(tp['realized'], True)}")
    return "\n".join(lines)


async def all_stats_text() -> str:
    lines = ["📊 <b>Статистика по кошелькам</b>"]
    totals = {"paper": [0.0, 0], "live": [0.0, 0]}
    for w in state.wallets():
        for mode in ("live", "paper"):
            s = await asyncio.to_thread(wallet_stats, w.id, mode)
            if not (s["copies"] or s["open"] or s["realized"]):
                continue
            totals[mode][0] += s["total"]
            totals[mode][1] += s["copies"]
            roi = f", ROI {s['roi']:+.1f}%" if s["roi"] is not None else ""
            lines.append(f"{MODE_ICON[mode]} {esc(w.short())}: копий {s['copies']}, PnL {money(s['total'], True)}"
                         f"{roi}, открыто {s['open']}, +{s['wins']}/−{s['losses']}")
    lines.append("")
    lines.append(f"Итого 🔴: {money(totals['live'][0], True)} ({totals['live'][1]} копий) · "
                 f"🧪: {money(totals['paper'][0], True)} ({totals['paper'][1]} копий)")
    return "\n".join(lines)


def health_text() -> str:
    lines = ["🩺 <b>Источники сделок</b>"]
    now = now_ms()
    for lst in chain.listeners:
        if lst.connected:
            st = "✅ подключён"
        elif lst.error or lst.reconnects:
            st = "❌ нет связи"
        else:
            st = "⏳ подключаюсь"
        head = ""
        if lst.head_lag_ms is not None:
            fresh = now - lst.last_head_ms < 30000
            head = (f", новые блоки приходят через {lst.head_lag_ms / 1000:.1f}с" if fresh
                    else f", блоков нет уже {fmt_age((now - lst.last_head_ms) / 1000)}")
        watch = f"слежу за {lst.subscribed_wallets}" if lst.subscribed_wallets else "кошельков пока нет"
        last = f", последнее {fmt_age((now - lst.last_event_ms) / 1000)} назад" if lst.last_event_ms else ""
        err = f"\n   ошибка: {esc(lst.error)}" if lst.error else ""
        lines.append(f"⛓ Блокчейн {esc(lst.name())}: {st}, {watch}{head}; "
                     f"сделок поймано {lst.events}{last}{err}")
    if not chain.listeners:
        lines.append("⛓ Блокчейн: выключен (CHAIN_ENABLED)")
    if settings.RTDS_ENABLED:
        s = rtds.stats
        age = fmt_age((now - s["last_msg_ms"]) / 1000) if s["last_msg_ms"] else "—"
        lines.append(f"📡 Поток RTDS: {'✅' if s['connected'] else '❌'} сообщений {s['messages']} "
                     f"(последнее {age} назад), orders_matched {s['matched']}, trades {s['trades']}"
                     + (f"\n   ошибка: {esc(s['error'])}" if s["error"] else ""))
    if settings.POLL_ENABLED:
        s = poller.stats
        if not state.active_addresses() and not s["polls"]:
            lines.append("🔁 Опрос Data API: ждёт кошельков (опрашивает только копируемых)")
        else:
            lines.append(f"🔁 Опрос Data API: опросов {s['polls']}, ошибок {s['errors']}"
                         + (f"\n   последняя ошибка: {esc(s['last_error'])}" if s["last_error"] else ""))
    bs = book_stream.stats
    lines.append(f"📗 Стакан WS: {'✅' if bs['connected'] else '❌'} подписок {len(book_stream._subscribed)}, "
                 f"сообщений {bs['messages']}")
    lines.append("")
    lines.append("<b>Кто первым замечает сделки</b> (за этот запуск):")
    names = {"chain": "блокчейн", "rtds": "RTDS ордера", "rtds_fills": "RTDS исполнения", "poll": "опрос API"}
    summary = detector.latency_summary()
    if not any(d["seen"] for d in summary.values()):
        lines.append("  пока не было сделок копируемых кошельков")
    for src, d in summary.items():
        if not d["seen"]:
            continue
        med = f", задержка от блока ~{d['median_ms'] / 1000:.1f}с" if d["median_ms"] is not None else ""
        lag = f", отстаёт от первого ~{d['lag_median_ms'] / 1000:.1f}с" if d["lag_median_ms"] else ""
        lines.append(f"  {names.get(src, src)}: видел {d['seen']}, первым {d['first']}{med}{lag}")
    if detector.side_mismatch:
        lines.append(f"⚠️ Расхождений стороны сделки между источниками: {detector.side_mismatch} "
                     f"(последнее: {esc(detector.side_mismatch_last)}) — пришли скрин мне.")
    lines.append("")
    if trader.live_possible():
        err = trader.client_error()
        last = trader.stats["order_ms_last"]
        bal = trader.cached_balance(120)
        lines.append("💳 Реальная торговля: ключ задан"
                     + (f", ошибка клиента: {esc(err)}" if err else "")
                     + (f", последний ордер {last} мс" if last is not None else "")
                     + (f", баланс {money(bal)}" if bal is not None else ""))
    else:
        lines.append("💳 Реальная торговля: POLY_PRIVATE_KEY не задан — только виртуальный режим")
    return "\n".join(lines)


# --------------------------------------------------------------- позиции ----

POS_PAGE = 8


def positions_view(wallet_id: int | None, page: int) -> tuple[str, InlineKeyboardMarkup]:
    items = positions.open_positions(wallet_id)
    items.sort(key=lambda p: (p.mode != "live", -p.opened_ts))
    total = len(items)
    page = max(0, min(page, max(0, (total - 1) // POS_PAGE)))
    chunk = items[page * POS_PAGE:(page + 1) * POS_PAGE]
    title = "📂 <b>Открытые позиции</b>"
    if wallet_id is not None:
        w = state.wallet(wallet_id)
        title += f" — {esc(w.short()) if w else wallet_id}"
    lines = [title + f" ({total})"]
    if not items:
        lines.append("Пока нет открытых позиций.")
    rows = []
    for p in chunk:
        w = state.wallet(p.wallet_id)
        bid = p.bid()
        u = p.unrealized()
        mark = (f"bid {bid:.3f}, {money(u, True)}" if bid is not None and u is not None else "цена неизвестна")
        lines.append(f"{MODE_ICON.get(p.mode)} #{p.id} {esc(w.short() if w else '?')}: "
                     f"<b>{esc(p.outcome or '?')}</b> {esc(cut(p.title or p.slug, 50))}\n"
                     f"   {p.shares:.2f} акц. по {p.avg_price:.3f} ({money(p.cost)}), {mark}, "
                     f"{fmt_age(time.time() - p.opened_ts)}")
        rows.append([Btn(f"❌ Закрыть #{p.id}", callback_data=f"p:{p.id}:close")])
    nav = []
    base = f"w:{wallet_id}:posp" if wallet_id is not None else "m:pos"
    if page > 0:
        nav.append(Btn("⬅️", callback_data=f"{base}:{page - 1}"))
    if (page + 1) * POS_PAGE < total:
        nav.append(Btn("➡️", callback_data=f"{base}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([Btn("◀️ Назад", callback_data=f"w:{wallet_id}" if wallet_id is not None else "m:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# --------------------------------------------------------------- поиск ----

def discovery_text() -> str:
    last = db.query_one("SELECT * FROM discovery_runs ORDER BY id DESC LIMIT 1")
    auto = fnum(state.get("discovery_auto_hours"))
    lines = [
        "🔎 <b>Поиск кошельков для копирования</b>",
        "Собираю кандидатов по всему Polymarket (лидерборды по категориям, крупные сделки, держатели топ-рынков, "
        "активные кошельки из потока) и разбираю каждого:",
        "• <b>edge</b> — насколько итог ставок лучше цены входа (z-score: навык или удача);",
        "• закрытые + непогашенные проигрыши (честный ROI), стабильность по неделям, просадка;",
        "• не бот (частота, серии сделок), не маркет-мейкер, не быстрые крипто-рынки;",
        "• время удержания (успеем ли скопировать), зависимость от 1-3 удачных ставок;",
        "• насколько уходит цена через 3/60 с после их входа (потери копирующего).",
        "",
    ]
    if last:
        lines.append(f"Последний поиск: {fmt_ts(last['finished'])} ({esc(last['mode'])}), "
                     f"кандидатов {last['n_candidates']}, разобрано {last['n_analyzed']}")
    lines.append(f"Авто-поиск: {'выкл' if auto <= 0 else f'раз в {auto:g} ч'}")
    if scanner.progress["running"]:
        lines.append("")
        lines.append(scanner.status_text())
    lines.append("\nАрхив пришли мне в чат — разберу и посоветую, кого копировать.")
    return "\n".join(lines)


def discovery_markup() -> InlineKeyboardMarkup:
    auto = fnum(state.get("discovery_auto_hours"))
    rows = []
    if scanner.progress["running"]:
        rows.append([Btn("⏹ Остановить поиск", callback_data="d:cancel")])
    else:
        rows.append([Btn("▶️ Быстрый (~15 мин)", callback_data="d:quick"),
                     Btn("▶️ Полный (~40 мин)", callback_data="d:full")])
    rows.append([Btn("🏆 Топ-10 последнего", callback_data="d:top"), Btn("📤 Последний архив", callback_data="d:last")])
    rows.append([Btn("🔍 Проверить кошелёк", callback_data="d:check")])
    rows.append([Btn(("✅ " if abs(auto - h) < 1e-9 else "") + (f"⏰ {h:g}ч" if h else "⏰ выкл"),
                     callback_data=f"d:auto:{h}") for h in (0, 24, 72)])
    rows.append([Btn("◀️ Меню", callback_data="m:main")])
    return InlineKeyboardMarkup(rows)


def _top_text_and_markup(top: list[dict]) -> tuple[str, InlineKeyboardMarkup]:
    lines = ["🏆 <b>Топ-10 последнего поиска</b>"]
    rows = []
    for i, m in enumerate(top, 1):
        tracked = state.by_address(m["wallet"])
        lines.append(
            f"\n<b>#{i}</b> {esc(m.get('name') or short_addr(m['wallet']))} — {m.get('verdict')} · score {m.get('score')}\n"
            f"<code>{m['wallet']}</code>\n"
            f"торг. PnL {money(fnum(m.get('all_time_pnl')))} (30д {money(fnum(m.get('pnl_30d')), True)}, "
            f"90д {money(fnum(m.get('pnl_90d')), True)}), "
            f"ставок {m.get('n_resolved') or 0}, ROI {fnum(m.get('roi_resolved')) * 100:+.0f}%, "
            f"edge {fnum(m.get('edge_cents')):+.1f}¢ z={fnum(m.get('z_bets')):.1f}, "
            f"удержание {fnum(m.get('median_hold_h')):.0f}ч, {esc(m.get('main_category') or '')}\n"
            f"<i>{esc(cut(m.get('reasons') or '', 300))}</i>")
        if not tracked:
            rows.append([Btn(f"➕ #{i} копировать виртуально", callback_data=f"d:addw:{m['wallet']}")])
    rows.append([Btn("◀️ Поиск", callback_data="m:disc")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def _show_top(top: list[dict]) -> None:
    global _last_top
    _last_top = top
    text, markup = _top_text_and_markup(top)
    await send_text(text, markup)


_progress_msg = {"id": None}


async def _progress_hook(text: str) -> None:
    if _app is None or not settings.TELEGRAM_CHAT_ID:
        return
    try:
        if _progress_msg["id"]:
            await _app.bot.edit_message_text(chat_id=settings.TELEGRAM_CHAT_ID, message_id=_progress_msg["id"],
                                             text=text)
        else:
            msg = await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text)
            _progress_msg["id"] = msg.message_id
    except Exception as exc:  # noqa: BLE001
        if "not modified" not in str(exc).lower():
            _progress_msg["id"] = None


async def _run_discovery(full: bool) -> None:
    _progress_msg["id"] = None
    await scanner.run(full=full)


# --------------------------------------------------------------- настройки ----

def settings_text() -> str:
    return (
        "⚙️ <b>Общие настройки</b>\n"
        f"Уведомлять о виртуальных сделках: {'да' if state.get('notify_paper') else 'нет'}\n"
        f"Уведомлять о пропусках: {'да' if state.get('notify_skips') else 'нет'} (шумно; всё равно есть в отчёте)\n"
        f"Забирать выигрыш автоматически (реальные): {'да' if state.get('auto_claim') else 'нет'}\n"
        f"Общий дневной стоп по реальным: {_off_or(state.get('daily_loss_limit_live'), '$')}\n"
        f"Потолок открытых реальных позиций: {_off_or(state.get('max_live_exposure'), '$')}\n"
        f"Проскальзывание на выходе: {_fv(state.get('exit_slip_cents'), '¢')} "
        "(с каждой неудачной попыткой продажи расширяется)\n"
        f"Отчёт каждые {settings.REPORT_INTERVAL_HOURS:g} ч"
    )


def settings_markup() -> InlineKeyboardMarkup:
    def tg(key: str, title: str) -> Btn:
        return Btn(("✅ " if state.get(key) else "❌ ") + title, callback_data=f"g:tg:{key}")

    rows = [[tg("notify_paper", "Увед. о виртуальных")], [tg("notify_skips", "Увед. о пропусках")],
            [tg("auto_claim", "Авто-забор выигрыша")]]
    for key, (title, _lo, _hi, unit, presets) in GLOBAL_FIELDS.items():
        rows.append([Btn(f"— {title} —", callback_data="noop")])
        cur = fnum(state.get(key))
        rows.append([Btn(("✅ " if abs(cur - v) < 1e-9 else "") + ("выкл" if v == 0 else _fv(v, unit)),
                         callback_data=f"g:set:{key}:{v}") for v in presets]
                    + [Btn("✏️", callback_data=f"g:in:{key}")])
    rows.append([Btn("◀️ Меню", callback_data="m:main")])
    return InlineKeyboardMarkup(rows)


# --------------------------------------------------------------- команды ----

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        await update.message.reply_text("Этот бот приватный.")
        return
    if not settings.TELEGRAM_CHAT_ID:
        await update.message.reply_text(f"ID этого чата: {update.effective_chat.id}\n"
                                        "Пропиши его в TELEGRAM_CHAT_ID и перезапусти бота.")
    await _reply(update.message, main_text(), main_markup())


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    text = " ".join(context.args or [])
    await _add_from_text(update.message, text)


async def cmd_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    addr = find_addr(" ".join(context.args or []))
    if not addr:
        await update.message.reply_text("Формат: /check 0x…")
        return
    await update.message.reply_text("🔬 Разбираю кошелёк, это 1-2 минуты…")
    asyncio.ensure_future(_check_wallet(addr))


async def _add_from_text(message, text: str) -> None:
    addr = find_addr(text)
    if not addr:
        await message.reply_text("Не вижу адреса. Пришли 0x… (40 символов) или ссылку на профиль, "
                                 "можно с именем через пробел: 0xabc… Вася")
        return
    label = text.replace(addr, "").strip()
    for junk in ("https://polymarket.com/profile/", "polymarket.com/profile/", "@"):
        label = label.replace(junk, "")
    label = label.strip()[:30]
    try:
        w = state.add_wallet(addr, label=label, mode="paper")
    except ValueError as exc:
        await message.reply_text(str(exc))
        return
    asyncio.ensure_future(copier.sync_holdings(w.address))
    await _reply(message, "✅ Кошелёк добавлен в <b>виртуальном</b> режиме — сделки копируются без денег, "
                          "статистика копится. Когда убедишься, что выгодно — переключи на 🔴 Реал.\n\n"
                 + wallet_text(w), wallet_markup(w))


async def _check_wallet(addr: str) -> None:
    try:
        m, path = await scanner.analyze_one(addr)
    except Exception as exc:  # noqa: BLE001
        notifier.notify(f"⚠️ Не удалось разобрать кошелёк: {esc(str(exc)[:200])}")
        return
    markup = None
    if not state.by_address(addr):
        markup = InlineKeyboardMarkup([[Btn("➕ Копировать виртуально", callback_data=f"d:addw:{addr}")]])
    await send_text("🔬 " + scanner.wallet_card(m), markup)
    await send_document(path, "Данные по кошельку — пришли мне, если нужен глубокий разбор.")


# --------------------------------------------------------------- текст ----

def _parse_value(raw: str, kind) -> float:
    v = float(raw.strip().replace(",", ".").replace("$", "").replace("%", "").replace("¢", ""))
    return int(round(v)) if kind is int else v


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _pending
    if not _allowed(update) or update.message is None:
        return
    text = (update.message.text or "").strip()
    if _pending is None:
        if find_addr(text):   # прислали адрес без команды — предложим добавить/проверить
            addr = find_addr(text)
            rows = [[Btn("➕ Копировать виртуально", callback_data=f"d:addw:{addr}"),
                     Btn("🔬 Разобрать", callback_data=f"d:chk:{addr}")]]
            await update.message.reply_text(f"Кошелёк {addr}: что сделать?", reply_markup=InlineKeyboardMarkup(rows))
        return
    pend, _pending = _pending, None
    kind = pend.get("kind")
    if kind == "add":
        await _add_from_text(update.message, text)
        return
    if kind == "check":
        addr = find_addr(text)
        if not addr:
            await update.message.reply_text("Это не адрес кошелька.")
            return
        await update.message.reply_text("🔬 Разбираю кошелёк, это 1-2 минуты…")
        asyncio.ensure_future(_check_wallet(addr))
        return
    if kind == "rename":
        w = state.update_wallet(pend["wid"], label=text[:30])
        if w:
            await _reply(update.message, "✅ Имя изменено.\n\n" + wallet_text(w), wallet_markup(w))
        return
    if kind == "wfield":
        field = pend["field"]
        title, typ, lo, hi, unit = FIELDS[field]
        try:
            v = _parse_value(text, typ)
            if not (lo <= v <= hi):
                raise ValueError
        except ValueError:
            _pending = pend
            await update.message.reply_text(f"Нужно число от {lo:g} до {hi:g}. Попробуй ещё раз.")
            return
        w = _apply_wallet_field(pend["wid"], field, v)
        if w:
            await _reply(update.message, f"✅ {title}: {_fv(v, unit)}\n\n" + wallet_text(w), wallet_markup(w))
        return
    if kind == "gfield":
        key = pend["key"]
        title, lo, hi, unit, _ = GLOBAL_FIELDS[key]
        try:
            v = _parse_value(text, float)
            if not (lo <= v <= hi):
                raise ValueError
        except ValueError:
            _pending = pend
            await update.message.reply_text(f"Нужно число от {lo:g} до {hi:g}.")
            return
        state.set(key, v)
        await _reply(update.message, "✅ Сохранено.\n\n" + settings_text(), settings_markup())


def _apply_wallet_field(wid: int, field: str, v):
    w = state.wallet(wid)
    if not w:
        return None
    changes = {field: v}
    if field == "min_price" and v >= w.max_price:
        changes[field] = round(w.max_price - 0.01, 3)
    if field == "max_price" and v <= w.min_price:
        changes[field] = round(w.min_price + 0.01, 3)
    return state.update_wallet(wid, **changes)


# --------------------------------------------------------------- кнопки ----

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _pending, _last_top
    query = update.callback_query
    if not _allowed(update):
        await query.answer("Нет доступа", show_alert=True)
        return
    data = query.data or ""
    try:
        await query.answer()
    except Exception:  # noqa: BLE001
        pass
    parts = data.split(":")
    head = parts[0]

    if data == "noop":
        return
    if head == "m":
        sub = parts[1]
        if sub == "main":
            await _edit(query, main_text(), main_markup())
        elif sub == "wallets":
            text = "👛 <b>Кошельки</b>\n⏸ выкл · 🧪 виртуально · 🔴 реально" if state.wallets() else \
                "👛 Пока нет кошельков. Добавь адрес или найди кандидатов в 🔎 Поиске."
            await _edit(query, text, wallets_markup())
        elif sub == "add":
            _pending = {"kind": "add"}
            await _edit(query, "➕ Пришли адрес кошелька (0x…) или ссылку на его профиль Polymarket. "
                               "Можно сразу с именем: <code>0xabc… Вася</code>",
                        InlineKeyboardMarkup([[Btn("◀️ Отмена", callback_data="m:main")]]))
        elif sub == "stats":
            await _edit(query, await all_stats_text(),
                        InlineKeyboardMarkup([[Btn("◀️ Меню", callback_data="m:main")]]))
        elif sub == "pos":
            page = int(parts[2]) if len(parts) > 2 else 0
            text, markup = positions_view(None, page)
            await _edit(query, text, markup)
        elif sub == "disc":
            await _edit(query, discovery_text(), discovery_markup())
        elif sub == "settings":
            await _edit(query, settings_text(), settings_markup())
        elif sub == "health":
            await _edit(query, health_text(), InlineKeyboardMarkup([[Btn("🔄 Обновить", callback_data="m:health"),
                                                                     Btn("◀️ Меню", callback_data="m:main")]]))
        elif sub == "report":
            await _edit(query, "📄 Собираю отчёт…", main_markup())
            asyncio.ensure_future(reports.send_report())
        return

    if head == "t":
        if parts[1] == "pause":
            state.set("paused", not state.get("paused"))
            await _edit(query, main_text(), main_markup())
        elif parts[1] == "live":
            if state.get("live_enabled"):
                state.set("live_enabled", False)
                await _edit(query, "⚪ Реальная торговля выключена. Кошельки в режиме 🔴 перестали входить "
                                   "(открытые позиции продолжают сопровождаться: выходы работают).\n\n"
                            + main_text(), main_markup())
            elif not trader.live_possible():
                await _edit(query, "❌ POLY_PRIVATE_KEY не задан в переменных окружения — реальная торговля "
                                   "невозможна.\n\n" + main_text(), main_markup())
            else:
                await _edit(query, "🔴 Включить реальную торговлю? Кошельки в режиме «Реал.» начнут покупать "
                                   "на реальные деньги.",
                            InlineKeyboardMarkup([[Btn("✅ Да, включить", callback_data="t:liveyes"),
                                                   Btn("❌ Отмена", callback_data="m:main")]]))
        elif parts[1] == "liveyes":
            try:
                await trader.get_client()
                bal = await trader.balance_usdc(max_age=0)
            except Exception as exc:  # noqa: BLE001
                await _edit(query, f"❌ Не удалось подключиться к Polymarket: {esc(str(exc)[:300])}\n"
                                   "Реальная торговля остаётся выключенной.", main_markup())
                return
            state.set("live_enabled", True)
            await _edit(query, f"🔴 Реальная торговля ВКЛЮЧЕНА. Баланс: {money(bal or 0)}\n\n" + main_text(),
                        main_markup())
        return

    if head == "w":
        await _wallet_callback(query, parts)
        return

    if head == "p":
        pid = int(parts[1])
        p = positions.get(pid)
        if p is None:
            await _edit(query, "Позиция уже закрыта.", InlineKeyboardMarkup([[Btn("◀️ Позиции",
                                                                                  callback_data="m:pos:0")]]))
            return
        if len(parts) > 2 and parts[2] == "close":
            await _edit(query, f"Закрыть позицию #{p.id} ({esc(p.outcome or '?')} {esc(cut(p.title, 60))}, "
                               f"{p.shares:.2f} акций) по рынку?",
                        InlineKeyboardMarkup([[Btn("✅ Да, продать", callback_data=f"p:{pid}:closeyes"),
                                               Btn("❌ Нет", callback_data="m:pos:0")]]))
        elif len(parts) > 2 and parts[2] == "closeyes":
            await _edit(query, f"⏳ Продаю позицию #{pid}…")
            res = await copier.sell_position(p, 1.0, "manual")
            if res is None:
                msg = "Позиция уже продаётся или закрыта."
            elif res.ok and res.shares > 0:
                msg = f"✅ Продано {res.shares:.2f} акций по {res.avg_price:.3f}."
            else:
                msg = f"❌ Не продалось: {esc(res.err or res.status)}"
            text, markup = positions_view(None, 0)
            await _edit(query, msg + "\n\n" + text, markup)
        return

    if head == "d":
        sub = parts[1]
        if sub in ("quick", "full"):
            if scanner.progress["running"]:
                await _edit(query, "Поиск уже идёт.\n\n" + discovery_text(), discovery_markup())
                return
            asyncio.ensure_future(_run_discovery(sub == "full"))
            await asyncio.sleep(0.2)
            await _edit(query, "▶️ Поиск запущен — прогресс будет отдельным сообщением, архив пришлю по готовности.\n\n"
                        + discovery_text(), discovery_markup())
        elif sub == "cancel":
            scanner.cancel()
            await _edit(query, "⏹ Останавливаю поиск…\n\n" + discovery_text(), discovery_markup())
        elif sub == "top":
            top = _last_top
            if not top:
                row = db.query_one("SELECT top_json FROM discovery_runs ORDER BY id DESC LIMIT 1")
                import json
                top = json.loads(row["top_json"]) if row and row.get("top_json") else []
                _last_top = top
            if not top:
                await _edit(query, "Поиск ещё не запускался.", discovery_markup())
                return
            text, markup = _top_text_and_markup(top)
            await _edit(query, text, markup)
        elif sub == "last":
            row = db.query_one("SELECT archive_path, finished FROM discovery_runs ORDER BY id DESC LIMIT 1")
            if row and row.get("archive_path") and os.path.exists(row["archive_path"]):
                await _edit(query, "📤 Отправляю архив последнего поиска…", discovery_markup())
                await send_document(row["archive_path"], f"🔎 Архив поиска от {fmt_ts(row['finished'])}")
            else:
                await _edit(query, "Архива нет (поиск не запускался или файл удалён при деплое).",
                            discovery_markup())
        elif sub == "check":
            _pending = {"kind": "check"}
            await _edit(query, "🔍 Пришли адрес кошелька (0x…) для разбора.",
                        InlineKeyboardMarkup([[Btn("◀️ Отмена", callback_data="m:disc")]]))
        elif sub == "chk":
            addr = norm_addr(parts[2])
            if addr:
                await _edit(query, "🔬 Разбираю кошелёк, это 1-2 минуты…")
                asyncio.ensure_future(_check_wallet(addr))
        elif sub == "auto":
            state.set("discovery_auto_hours", fnum(parts[2]))
            await _edit(query, discovery_text(), discovery_markup())
        elif sub == "addw":
            addr = norm_addr(parts[2])
            if not addr:
                return
            name = next((m.get("name") for m in _last_top if m.get("wallet") == addr), "") or ""
            w = state.add_wallet(addr, label=str(name)[:30], mode="paper")
            asyncio.ensure_future(copier.sync_holdings(w.address))
            await _edit(query, "✅ Добавлен в виртуальном режиме.\n\n" + wallet_text(w), wallet_markup(w))
        return

    if head == "g":
        sub = parts[1]
        if sub == "tg":
            state.set(parts[2], not state.get(parts[2]))
        elif sub == "set":
            state.set(parts[2], fnum(parts[3]))
        elif sub == "in":
            key = parts[2]
            _pending = {"kind": "gfield", "key": key}
            title, lo, hi, unit, _ = GLOBAL_FIELDS[key]
            await _edit(query, f"✏️ Введи {title} ({unit}), число от {lo:g} до {hi:g}. 0 — выключить.",
                        InlineKeyboardMarkup([[Btn("◀️ Отмена", callback_data="m:settings")]]))
            return
        await _edit(query, settings_text(), settings_markup())
        return


async def _wallet_callback(query, parts: list[str]) -> None:
    global _pending
    wid = int(parts[1])
    w = state.wallet(wid)
    if w is None:
        await _edit(query, "Кошелёк не найден (удалён?).", wallets_markup())
        return
    action = parts[2] if len(parts) > 2 else ""

    async def show_wallet(prefix: str = "") -> None:
        sp = await asyncio.to_thread(wallet_stats, w.id, "paper")
        sl = await asyncio.to_thread(wallet_stats, w.id, "live")
        await _edit(query, prefix + wallet_text(w, sp, sl), wallet_markup(w))

    if action == "":
        await show_wallet()
    elif action == "mode":
        mode = parts[3]
        if mode == "live" and w.mode != "live":
            await _edit(query, f"🔴 Перевести {esc(w.short())} на РЕАЛЬНЫЕ деньги?\n"
                               f"Размер: {_size_line(w)}.\n"
                               + ("" if state.get("live_enabled") else
                                  "⚠️ Общий выключатель реальной торговли сейчас выключен — кошелёк начнёт "
                                  "торговать, когда включишь его в главном меню.\n"),
                        InlineKeyboardMarkup([[Btn("✅ Да", callback_data=f"w:{wid}:liveyes"),
                                               Btn("❌ Отмена", callback_data=f"w:{wid}")]]))
            return
        state.update_wallet(wid, mode=mode)
        if mode in ("paper", "live"):
            asyncio.ensure_future(copier.sync_holdings(w.address))
        else:
            poller.forget(w.address)   # при включении снова — без «догона» старых сделок
        await show_wallet()
    elif action == "liveyes":
        if not trader.live_possible():
            await show_wallet("❌ POLY_PRIVATE_KEY не задан — реальный режим невозможен.\n\n")
            return
        state.update_wallet(wid, mode="live")
        asyncio.ensure_future(copier.sync_holdings(w.address))
        asyncio.ensure_future(trader.get_client())
        await show_wallet("🔴 Режим: реально.\n\n")
    elif action in SUBMENU_TEXT:
        markup = {"size": size_markup, "lim": limits_markup, "slip": slip_markup, "exit": exits_markup,
                  "flt": filters_markup}[action](w)
        await _edit(query, SUBMENU_TEXT[action] + "\n\n" + wallet_text(w), markup)
    elif action == "sm":
        state.update_wallet(wid, size_mode=parts[3])
        await _edit(query, SUBMENU_TEXT["size"] + "\n\n" + wallet_text(w), size_markup(w))
    elif action == "set":
        field = parts[3]
        if field not in FIELDS:
            return
        typ = FIELDS[field][1]
        _apply_wallet_field(wid, field, _parse_value(parts[4], typ))
        sub = next((k for k, fs in (("size", ("size_pct", "size_fixed", "max_usdc")),
                                    ("lim", ("max_position_usdc", "min_target_usdc", "max_open", "daily_loss_limit")),
                                    ("slip", ("max_slip_cents", "max_slip_pct", "min_price", "max_price")),
                                    ("exit", ("sl_pct", "tp_pct", "trail_pct", "max_hold_h")),
                                    ("flt", ("min_minutes_left",))) if field in fs), None)
        markup = {"size": size_markup, "lim": limits_markup, "slip": slip_markup, "exit": exits_markup,
                  "flt": filters_markup}.get(sub, wallet_markup)(w)
        await _edit(query, (SUBMENU_TEXT.get(sub, "") + "\n\n" if sub else "") + wallet_text(w), markup)
    elif action == "pr":
        state.update_wallet(wid, min_price=float(parts[3]), max_price=float(parts[4]))
        await _edit(query, SUBMENU_TEXT["slip"] + "\n\n" + wallet_text(w), slip_markup(w))
    elif action == "tg":
        field = parts[3]
        if hasattr(w, field) and isinstance(getattr(w, field), bool):
            state.update_wallet(wid, **{field: not getattr(w, field)})
        back = {"accumulate_small": ("size", size_markup), "copy_sells": ("exit", exits_markup),
                "copy_adds": ("exit", exits_markup), "skip_fast": ("flt", filters_markup),
                "skip_live_sports": ("flt", filters_markup),
                "copy_maker_fills": ("flt", filters_markup)}.get(field)
        if back:
            await _edit(query, SUBMENU_TEXT[back[0]] + "\n\n" + wallet_text(w), back[1](w))
        else:
            await show_wallet()
    elif action == "in":
        field = parts[3]
        title, typ, lo, hi, unit = FIELDS[field]
        _pending = {"kind": "wfield", "wid": wid, "field": field}
        await _edit(query, f"✏️ {title}: пришли число ({unit or 'шт'}) от {lo:g} до {hi:g}.",
                    InlineKeyboardMarkup([[Btn("◀️ Отмена", callback_data=f"w:{wid}")]]))
    elif action == "st":
        await _edit(query, await wallet_stats_text(w),
                    InlineKeyboardMarkup([[Btn("🔄 Обновить", callback_data=f"w:{wid}:st"),
                                           Btn("◀️ Назад", callback_data=f"w:{wid}")]]))
    elif action in ("pos", "posp"):
        page = int(parts[3]) if action == "posp" and len(parts) > 3 else 0
        text, markup = positions_view(wid, page)
        await _edit(query, text, markup)
    elif action == "an":
        await _edit(query, "🔬 Разбираю кошелёк (1-2 минуты), результат пришлю отдельным сообщением.",
                    wallet_markup(w))
        asyncio.ensure_future(_check_wallet(w.address))
    elif action == "ren":
        _pending = {"kind": "rename", "wid": wid}
        await _edit(query, "✏️ Пришли новое имя кошелька.",
                    InlineKeyboardMarkup([[Btn("◀️ Отмена", callback_data=f"w:{wid}")]]))
    elif action == "del":
        n_open = len(positions.open_positions(wid))
        warn = (f"\n⚠️ У кошелька {n_open} открытых позиций — они останутся и будут сопровождаться "
                "(стопы/резолюция), но новых сделок не будет.") if n_open else ""
        await _edit(query, f"🗑 Удалить {esc(w.short())} из копирования?{warn}",
                    InlineKeyboardMarkup([[Btn("✅ Да, удалить", callback_data=f"w:{wid}:delyes"),
                                           Btn("❌ Нет", callback_data=f"w:{wid}")]]))
    elif action == "delyes":
        state.remove_wallet(wid)
        poller.forget(w.address)
        await _edit(query, f"🗑 {esc(w.short())} удалён.", wallets_markup())


# --------------------------------------------------------------- сборка ----

def build_app() -> Application:
    global _app
    _app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    _app.add_handler(CommandHandler(["start", "menu"], cmd_start))
    _app.add_handler(CommandHandler("add", cmd_add))
    _app.add_handler(CommandHandler("check", cmd_check))
    _app.add_handler(CallbackQueryHandler(on_callback))
    _app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    notifier.set_sender(send_text)
    reports.set_doc_sender(send_document)
    scanner.set_hooks(progress_hook=_progress_hook, doc_sender=send_document, top_hook=_show_top)
    return _app
