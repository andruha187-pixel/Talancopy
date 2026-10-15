"""
Почему бот не входит — чтобы «не торгует полдня» было видно в Telegram, а не
только в 4-часовых CSV.

Две разные вещи:
1. Сигнал был, а сделки нет — это решает executor: лимит позиций, дневной
   стоп, мало ликвидности, ошибка ордера, пауза, нулевой банк. Считаем
   каждый рынок один раз на причину (сигнал повторяется на каждом тике).
2. Сигнала не было — это рынок: по тикам из таблицы signals смотрим, какое
   условие не выполнилось ни разу за окно входа. Отчёты 06–08.10: 11 часов
   без сделок на всех 7 монетах — бот работал, но цена в окне входа почти
   всегда была ближе 1.5 ATR к страйку (в активные часы ATR больше).
"""
from __future__ import annotations
import time
from collections import Counter

from src import runtime_state, storage
from src.timeframes import TIMEFRAMES

SKIP_LABELS = {
    "cap": "лимит позиций",
    "daily": "дневной стоп",
    "liquidity": "мало ликвидности",
    "order_error": "ошибка ордера",
    "paused": "пауза",
    "no_bank": "банк исчерпан",
    "filter_time": "фильтр: мало времени до конца",
    "filter_spread": "фильтр: широкий спред",
}

_skips: Counter = Counter()      # (asset, reason) -> число рынков
_seen: set = set()               # (slug, reason) — уже посчитанные
_last_error: dict = {}           # asset -> текст последней ошибки ордера
_since: float = time.time()


def record_skip(slug: str, asset: str, reason: str, detail: str | None = None) -> None:
    key = (slug, reason)
    if key in _seen:
        return
    if len(_seen) > 20000:
        _seen.clear()
    _seen.add(key)
    _skips[(asset, reason)] += 1
    if detail:
        _last_error[asset] = str(detail)[:200]


def skip_counts() -> Counter:
    out: Counter = Counter()
    for (_asset, reason), n in _skips.items():
        out[reason] += n
    return out


def skip_line() -> str:
    """'лимит позиций 3, ошибка ордера 1' или '' — с последнего отчёта."""
    c = skip_counts()
    return ", ".join(f"{SKIP_LABELS.get(r, r)} {n}" for r, n in c.most_common())


def last_errors() -> dict:
    return dict(_last_error)


def reset_skips() -> None:
    global _since
    _skips.clear()
    _seen.clear()
    _last_error.clear()
    _since = time.time()


def skips_since() -> float:
    return _since


# --- рынки без сигнала ---

NO_SIGNAL_ORDER = ("chainlink", "dist", "score", "below", "jump", "above", "vol")


def _no_signal_labels() -> dict:
    mult = TIMEFRAMES[0].atr_distance_mult
    return {
        "dist": f"цена ближе {mult:g} ATR к страйку",
        "score": "score ниже порога",
        "below": "цена ниже диапазона входа",
        "jump": "цена перескочила диапазон",
        "above": "цена выше диапазона входа",
        "vol": "всплеск волатильности",
        "chainlink": "нет цены или страйка Chainlink",
    }


def classify_markets(rows: list, columns: list) -> Counter:
    """Рынки, у которых были тики в окне входа: 'entered' — был сигнал, иначе
    главное условие, которое не выполнилось ни разу (см. NO_SIGNAL_ORDER)."""
    i = {c: k for k, c in enumerate(columns)}
    win_lo, win_hi = runtime_state.entry_window()
    by_market: dict[str, list] = {}
    for r in rows:
        ml = r[i["minutes_left"]]
        if ml is None or not (win_lo - 1e-9 <= ml <= win_hi + 1e-9):
            continue
        by_market.setdefault(r[i["market_slug"]], []).append(r)
    counts: Counter = Counter()
    for slug, ticks in by_market.items():
        if any(t[i["should_enter"]] for t in ticks):
            counts["entered"] += 1
            continue
        # Режим Chainlink, а его цены/страйка не было ни на одном тике
        if "price_source" in i and all(t[i["price_source"]] == "none" for t in ticks):
            counts["chainlink"] += 1
            continue
        lo, hi = runtime_state.entry_range(slug.split("-")[0])
        prices = [t[i["entry_price"]] for t in ticks if t[i["entry_price"]] is not None]
        in_rng = [t for t in ticks
                  if t[i["entry_price"]] is not None and lo - 1e-9 <= t[i["entry_price"]] <= hi + 1e-9]
        if not in_rng:
            if prices and all(p < lo for p in prices):
                counts["below"] += 1
            elif prices and all(p > hi for p in prices):
                counts["above"] += 1
            else:
                counts["jump"] += 1
            continue
        vol_ok = [t for t in in_rng if (t[i["vol_score"]] or 0) > 0]
        if not vol_ok:
            counts["vol"] += 1
            continue
        if not any((t[i["distance_score"]] or 0) > 0 for t in vol_ok):
            counts["dist"] += 1
            continue
        counts["score"] += 1
    return counts


def no_signal_line(counts: Counter, top: int = 3) -> str:
    """'Рынков в окне входа 336, с сигналом 4. Без сигнала: цена ближе 1.5 ATR
    к страйку 48%, score ниже порога 35%, …'"""
    total = sum(counts.values())
    if not total:
        return ""
    entered = counts.get("entered", 0)
    no_sig = total - entered
    labels = _no_signal_labels()
    parts = sorted(((counts.get(k, 0), labels[k]) for k in NO_SIGNAL_ORDER if counts.get(k)), reverse=True)
    tail = ", ".join(f"{label} {n * 100 / no_sig:.0f}%" for n, label in parts[:top]) if no_sig else "—"
    return f"Рынков в окне входа {total}, с сигналом {entered}. Без сигнала: {tail}"


def _ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, m = seconds // 3600, (seconds % 3600) // 60
    return f"{h} ч {m} мин" if h else f"{m} мин"


def why_text(hours: float = 4.0, now: float | None = None) -> str:
    """Текст для кнопки «🔎 Почему нет сделок»."""
    now = time.time() if now is None else now
    since = int(now - hours * 3600)
    rows = storage.get_signals_since(since)
    counts = classify_markets(rows, storage.SIGNALS_COLUMNS)
    trades = [t for t in storage.get_trades_since(since)]
    last = storage.last_trade()
    win_lo, win_hi = runtime_state.entry_window()
    lines = [f"🔎 Почему нет сделок — последние {hours:g} ч", ""]
    if last:
        slug, ts = last
        lines.append(f"Последняя сделка: {_ago(now - ts)} назад ({slug.split('-')[0].upper()}).")
    else:
        lines.append("Сделок в базе пока нет.")
    lines.append(f"Окно входа: за {win_lo:g}–{win_hi:g} мин до конца рынка, порог score "
                 f"{runtime_state.get('safety_score_threshold'):.0f}.")
    lines.append("")
    total = sum(counts.values())
    if total:
        entered = counts.get("entered", 0)
        lines.append(f"Рынков, где бот смотрел вход: {total}. С сигналом: {entered}. Сделок: {len(trades)}.")
        no_sig = total - entered
        if no_sig:
            labels = _no_signal_labels()
            lines.append(f"Почему не было сигнала ({no_sig} рынков):")
            for n, label in sorted(((counts.get(k, 0), labels[k]) for k in NO_SIGNAL_ORDER if counts.get(k)),
                                   reverse=True):
                lines.append(f"  • {label} — {n * 100 / no_sig:.0f}%")
    else:
        lines.append("За это время нет тиков в окне входа — бот только запустился или монеты выключены.")
    skips = skip_line()
    lines.append("")
    lines.append(f"Сигналы без сделки (с {time.strftime('%H:%M', time.gmtime(_since))} UTC): {skips or 'нет'}")
    for asset, err in last_errors().items():
        lines.append(f"  последняя ошибка ордера {asset.upper()}: {err}")
    try:
        from src import fast_signal  # здесь: fast_signal -> executor -> diagnostics
        lines.append("")
        lines.append(fast_signal.status_line())
        fast_entries = fast_signal.entries_line(trades, storage.TRADES_COLUMNS)
        if fast_entries:
            lines.append(fast_entries + f" за {hours:g} ч")
    except Exception:  # noqa: BLE001 — строка статуса не должна ломать экран
        pass
    try:
        from src import entry_confirm  # здесь: entry_confirm -> runtime_state -> storage
        lines.append(entry_confirm.status_line())
    except Exception:  # noqa: BLE001
        pass
    try:
        from src import chainlink_feed
        lines.append(chainlink_feed.status_line())
    except Exception:  # noqa: BLE001
        pass
    mult = TIMEFRAMES[0].atr_distance_mult
    lines += [
        "",
        f"Долгие паузы бывают: стратегия входит, только когда цена ушла от страйка "
        f"минимум на {mult:g} ATR за {win_lo:g}–{win_hi:g} мин до конца. В активные часы "
        "ATR больше, и такие моменты редки. У одного BTC по истории пауза 4–8 ч "
        "бывала примерно раз в день.",
    ]
    return "\n".join(lines)
