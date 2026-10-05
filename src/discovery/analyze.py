"""
Метрики кошелька для отбора в копирование. Чистые функции над сырыми
ответами Data API v2 — тестируются без сети.

Главная идея: на рынке предсказаний безубыточный винрейт = цена входа.
Кто покупает по 0.90 и выигрывает 92% — почти не зарабатывает и рискует
всем на редком проигрыше; кто покупает по 0.40 и выигрывает 55% — сильный.
Поэтому основа оценки — не винрейт и не сумма PnL, а «edge»: насколько
реальный результат каждой ставки лучше цены, по которой она куплена, и
насколько это статистически значимо (z-score). Остальное — копируемость:
частота сделок (бот или человек), время удержания, быстрые рынки,
маркет-мейкинг, зависимость от одной удачной ставки, проскальзывание
после их входа, стабильность по неделям.
"""
from __future__ import annotations

import math
import re
import statistics
import time
from collections import defaultdict

from src.util import fnum

FAST_RE = re.compile(
    r"(updown-(5m|15m|30m|1h|4h)-|up-or-down-.*-\d{1,2}(am|pm)-et|-above-on-.*-\d{1,2}(am|pm)-et|"
    r"-(5|15)-?min|price-.*-\d{1,2}(am|pm)-et)")

CATEGORY_RULES = [
    ("crypto", re.compile(r"bitcoin|btc|ethereum|eth-|solana|sol-|xrp|crypto|doge|bnb|hype|updown|up-or-down|"
                          r"-above-|microstrategy|coinbase|altcoin|memecoin|pump")),
    ("sports", re.compile(r"nfl|nba|mlb|nhl|wnba|epl|premier-league|champions-league|la-liga|serie-a|bundesliga|"
                          r"ligue-1|mls|ufc|boxing|tennis|atp|wta|f1|formula-1|nascar|golf|pga|cricket|ipl|ncaa|"
                          r"-vs-|super-bowl|world-cup|euro-20|copa|olympic|match|game-\d|esports|cs2|dota|lol-|valorant")),
    ("politics", re.compile(r"election|president|presidential|senate|house-|congress|governor|mayor|trump|biden|"
                            r"harris|vance|newsom|democrat|republican|gop|primary|nominee|parliament|prime-minister|"
                            r"vote|poll|cabinet|impeach|supreme-court|government-shutdown")),
    ("geopolitics", re.compile(r"war|ukraine|russia|israel|gaza|iran|china-|taiwan|nato|ceasefire|invasion|strike|"
                               r"missile|sanction|putin|zelensk|netanyahu|hamas|hezbollah")),
    ("economics", re.compile(r"fed-|fomc|rate-cut|interest-rate|inflation|cpi|gdp|recession|unemployment|jobs-report|"
                             r"payroll|tariff|treasury|powell")),
    ("finance", re.compile(r"stock|nasdaq|s-p-500|sp500|dow-|tesla|nvidia|apple|google|amazon|microsoft|meta-|"
                           r"ipo|earnings|market-cap|gold|oil|silver")),
    ("culture", re.compile(r"oscar|grammy|emmy|movie|box-office|album|song|spotify|taylor-swift|celebrity|tiktok|"
                           r"youtube|mrbeast|twitter|tweet|elon|musk|mentions|say-|said")),
    ("weather", re.compile(r"temperature|weather|hurricane|snow|rain|heat|climate")),
]


def categorize(slug: str | None, event_slug: str | None = None) -> str:
    text = f"{slug or ''} {event_slug or ''}".lower()
    for name, rx in CATEGORY_RULES:
        if rx.search(text):
            return name
    return "other"


def is_fast(slug: str | None, event_slug: str | None = None) -> bool:
    return bool(FAST_RE.search(f"{slug or ''} {event_slug or ''}".lower()))


def _ts(v) -> float:
    t = fnum(v)
    if t > 1e14:     # микросекунды
        return t / 1e6
    if t > 1e12:     # миллисекунды
        return t / 1e3
    return t


def _median(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    k = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[k]


# ------------------------------------------------------- позиции (edge) ----

def position_pnl(p: dict) -> float:
    for key in ("total_pnl",):
        if p.get(key) is not None:
            return fnum(p.get(key))
    return fnum(p.get("realized_pnl")) + fnum(p.get("unrealized_pnl"))


def resolved_positions(closed: list[dict], redeemable: list[dict]) -> list[dict]:
    """Закрытые + разрешённые, но не погашённые (иначе проигрыши, которые
    человек не стал «забирать», выпадают из статистики и она выглядит
    лучше, чем есть). Дубли по token_id убираем."""
    out: dict[str, dict] = {}
    for p in list(closed) + list(redeemable):
        key = str(p.get("token_id") or p.get("asset") or "") or str(id(p))
        if key not in out:
            out[key] = p
    return list(out.values())


def edge_metrics(resolved: list[dict], now: float | None = None) -> dict:
    now = now or time.time()
    bets = []
    for p in resolved:
        cost = fnum(p.get("entry_cost_usdc")) or fnum(p.get("initial_value")) or fnum(p.get("total_bought"))
        price = fnum(p.get("avg_price"))
        if cost <= 0 or not (0.0 < price < 1.0):
            continue
        pnl = position_pnl(p)
        y = min(1.0, max(0.0, price * (1.0 + pnl / cost)))
        bets.append({"cost": cost, "p": price, "pnl": pnl, "y": y,
                     "ts": _ts(p.get("last_event_at") or p.get("timestamp")),
                     "slug": p.get("slug"), "event_slug": p.get("event_slug")})
    n = len(bets)
    m: dict = {"n_resolved": n}
    if n == 0:
        return m
    cost_total = sum(b["cost"] for b in bets)
    pnl_total = sum(b["pnl"] for b in bets)
    wins = [b for b in bets if b["pnl"] > 0]
    gains = sorted((b["pnl"] for b in wins), reverse=True)
    losses = [b["pnl"] for b in bets if b["pnl"] < 0]
    var_bets = sum(b["p"] * (1 - b["p"]) for b in bets)
    var_usd = sum(b["cost"] ** 2 * (1 - b["p"]) / b["p"] for b in bets)
    m.update({
        "cost_resolved": cost_total,
        "pnl_resolved": pnl_total,
        "roi_resolved": pnl_total / cost_total if cost_total > 0 else None,
        "win_rate": len(wins) / n,
        "avg_entry": sum(b["p"] * b["cost"] for b in bets) / cost_total if cost_total else None,
        "edge_cents": 100.0 * sum(b["y"] - b["p"] for b in bets) / n,
        "z_bets": sum(b["y"] - b["p"] for b in bets) / math.sqrt(var_bets) if var_bets > 0 else None,
        "z_usd": pnl_total / math.sqrt(var_usd) if var_usd > 0 else None,
        "profit_factor": (sum(gains) / -sum(losses)) if losses else (math.inf if gains else None),
        "top1_share": gains[0] / sum(gains) if gains else None,
        "top3_share": sum(gains[:3]) / sum(gains) if gains else None,
        "pnl_ex_top3": pnl_total - sum(gains[:3]),
        "median_bet": _median([b["cost"] for b in bets]),
        "fav_share": sum(b["cost"] for b in bets if b["p"] >= 0.9) / cost_total if cost_total else None,
        "longshot_share": sum(b["cost"] for b in bets if b["p"] <= 0.1) / cost_total if cost_total else None,
        "fast_share_pos": sum(b["cost"] for b in bets if is_fast(b["slug"], b["event_slug"])) / cost_total
        if cost_total else None,
    })
    recent = [b for b in bets if b["ts"] and now - b["ts"] <= 30 * 86400]
    rc = sum(b["cost"] for b in recent)
    m["n_resolved_30d"] = len(recent)
    m["roi_resolved_30d"] = sum(b["pnl"] for b in recent) / rc if rc > 0 else None
    # Калибровка по корзинам цены: где у человека реальное преимущество.
    buckets = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 0.9), (0.9, 1.0)]
    parts = []
    for lo, hi in buckets:
        bb = [b for b in bets if lo <= b["p"] < hi]
        if bb:
            e = 100.0 * sum(b["y"] - b["p"] for b in bb) / len(bb)
            parts.append(f"{int(lo * 100)}-{int(hi * 100)}:{e:+.1f}c/n{len(bb)}")
    m["edge_by_price"] = " | ".join(parts)
    cats: dict[str, list] = defaultdict(lambda: [0.0, 0.0, 0])
    for b in bets:
        c = cats[categorize(b["slug"], b["event_slug"])]
        c[0] += b["cost"]
        c[1] += b["pnl"]
        c[2] += 1
    m["categories"] = " | ".join(
        f"{k}:{v[0] / cost_total * 100:.0f}% roi{(v[1] / v[0] * 100 if v[0] else 0):+.0f}%"
        for k, v in sorted(cats.items(), key=lambda kv: -kv[1][0]))
    m["main_category"] = max(cats.items(), key=lambda kv: kv[1][0])[0] if cats else None
    return m


# ------------------------------------------------------ PnL по дням ----

def pnl_series_metrics(points: list[dict], now: float | None = None) -> dict:
    now = now or time.time()
    pts = []
    for p in points or []:
        ts = _ts(p.get("timestamp"))
        v = p.get("total_pnl")
        if v is None:
            v = p.get("cumulative_pnl")
        if ts and v is not None:
            pts.append((ts, fnum(v), p))
    pts.sort(key=lambda x: x[0])
    m: dict = {"pnl_days": len(pts)}
    if len(pts) < 2:
        return m
    vals = [v for _, v, _ in pts]
    last_ts, last_v, last_p = pts[-1]

    def value_at(age_days: float) -> float:
        target = now - age_days * 86400
        prior = [v for ts, v, _ in pts if ts <= target]
        return prior[-1] if prior else vals[0]

    m["pnl_total"] = last_v
    m["pnl_7d"] = last_v - value_at(7)
    m["pnl_30d"] = last_v - value_at(30)
    m["pnl_90d"] = last_v - value_at(90)
    peak = -math.inf
    max_dd = 0.0
    for v in vals:
        peak = max(peak, v)
        max_dd = max(max_dd, peak - v)
    m["max_dd"] = max_dd
    recent = [(ts, v) for ts, v, _ in pts if ts >= now - 90 * 86400]
    if len(recent) >= 2:
        peak = -math.inf
        dd90 = 0.0
        for _, v in recent:
            peak = max(peak, v)
            dd90 = max(dd90, peak - v)
        m["max_dd_90d"] = dd90
        diffs = [b[1] - a[1] for a, b in zip(recent, recent[1:])]
        if len(diffs) >= 5:
            sd = statistics.pstdev(diffs)
            m["daily_sharpe"] = (statistics.mean(diffs) / sd) if sd > 0 else None
            gains = [d for d in diffs if d > 0]
            m["best_day_share"] = max(gains) / sum(gains) if gains else None
        weeks: dict[int, float] = defaultdict(float)
        for (ts_a, va), (ts_b, vb) in zip(recent, recent[1:]):
            weeks[int(ts_b // (7 * 86400))] += vb - va
        wk = [v for v in weeks.values() if abs(v) > 1e-9]
        m["weeks_90d"] = len(wk)
        m["pos_weeks_share"] = sum(1 for v in wk if v > 0) / len(wk) if wk else None
    m["first_pnl_ts"] = pts[0][0]
    m["rebates"] = fnum(last_p.get("rebates"))
    m["rewards"] = fnum(last_p.get("rewards")) + fnum(last_p.get("yield"))
    m["fees_paid"] = fnum(last_p.get("fees_paid"))
    m["volume_usdc"] = fnum(last_p.get("volume_usdc"))
    m["lp_pnl"] = fnum(last_p.get("realized_lp_pnl"))
    denom = max(abs(last_v), 1.0)
    m["mm_income_share"] = (m["rebates"] + m["rewards"] + max(m["lp_pnl"], 0.0)) / denom
    return m


# ------------------------------------------------------ сделки ----

def trade_metrics(trades: list[dict], taker_trades: list[dict] | None, resolved: list[dict],
                  now: float | None = None) -> dict:
    now = now or time.time()
    rows = [t for t in trades or [] if str(t.get("type") or "TRADE").upper() == "TRADE"]
    m: dict = {"n_trades_sample": len(rows)}
    if not rows:
        return m
    # Склейка исполнений одного ордера (одна транзакция) — это одно решение.
    orders: dict[tuple, dict] = {}
    for t in rows:
        key = (str(t.get("transaction_hash") or t.get("timestamp")), str(t.get("token_id")), str(t.get("side")))
        o = orders.get(key)
        usdc = fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
        if o is None:
            orders[key] = {"ts": _ts(t.get("timestamp")), "side": str(t.get("side") or "").upper(),
                           "token": str(t.get("token_id")), "cond": str(t.get("condition_id") or ""),
                           "oi": t.get("outcome_index"), "usdc": usdc, "shares": fnum(t.get("size")),
                           "slug": t.get("slug"), "event_slug": t.get("event_slug")}
        else:
            o["usdc"] += usdc
            o["shares"] += fnum(t.get("size"))
    ol = sorted(orders.values(), key=lambda o: o["ts"])
    ts_list = [o["ts"] for o in ol if o["ts"]]
    span_days = max((ts_list[-1] - ts_list[0]) / 86400, 1 / 24) if len(ts_list) >= 2 else None
    days = defaultdict(int)
    for o in ol:
        if o["ts"]:
            days[int(o["ts"] // 86400)] += 1
    per_day = list(days.values())
    gaps = [b - a for a, b in zip(ts_list, ts_list[1:])]
    sizes = [o["usdc"] for o in ol if o["usdc"] > 0]
    buys = [o for o in ol if o["side"] == "BUY"]
    buy_usdc = sum(o["usdc"] for o in buys)
    m.update({
        "n_orders_sample": len(ol),
        "sample_span_days": span_days,
        "orders_per_active_day": _median(per_day),
        "orders_per_day_p90": _pct(per_day, 0.9),
        "active_days_sample": len(per_day),
        "median_gap_min": (_median(gaps) / 60.0) if gaps else None,
        "burst_share": sum(1 for g in gaps if g < 10) / len(gaps) if gaps else None,
        "median_order_usdc": _median(sizes),
        "p90_order_usdc": _pct(sizes, 0.9),
        "buy_share": len(buys) / len(ol),
        "avg_buy_price": (sum(o["usdc"] for o in buys) / sum(o["shares"] for o in buys))
        if buys and sum(o["shares"] for o in buys) > 0 else None,
        "fast_share_trades": sum(o["usdc"] for o in ol if is_fast(o["slug"], o["event_slug"])) /
        max(sum(o["usdc"] for o in ol), 1e-9),
        "last_trade_age_h": (now - ts_list[-1]) / 3600 if ts_list else None,
        "markets_in_sample": len({o["cond"] for o in ol if o["cond"]}),
    })
    if buy_usdc > 0:
        by_event: dict[str, float] = defaultdict(float)
        for o in buys:
            by_event[o["event_slug"] or o["slug"] or o["cond"]] += o["usdc"]
        m["top_event_share"] = max(by_event.values()) / buy_usdc
        mids = [o for o in buys if o["shares"] > 0 and 0.2 <= o["usdc"] / o["shares"] <= 0.8]
        m["mid_price_share"] = sum(o["usdc"] for o in mids) / buy_usdc
    # Обе стороны одного рынка — признак маркет-мейкера/арбитража.
    sides_by_cond: dict[str, set] = defaultdict(set)
    for o in buys:
        if o["cond"] and o["oi"] is not None:
            sides_by_cond[o["cond"]].add(o["oi"])
    if sides_by_cond:
        m["both_sides_share"] = sum(1 for s in sides_by_cond.values() if len(s) > 1) / len(sides_by_cond)
    # Время удержания: первая покупка токена → первая продажа после неё
    # (или последнее событие по разрешённой позиции).
    first_buy: dict[str, float] = {}
    first_sell: dict[str, float] = {}
    for o in ol:
        if o["side"] == "BUY":
            first_buy.setdefault(o["token"], o["ts"])
        elif o["side"] == "SELL" and o["token"] in first_buy and o["token"] not in first_sell:
            first_sell[o["token"]] = o["ts"]
    end_by_token = {str(p.get("token_id")): _ts(p.get("last_event_at")) for p in resolved or []}
    holds = []
    sold_early = 0
    for tok, t0 in first_buy.items():
        if tok in first_sell:
            holds.append(first_sell[tok] - t0)
            sold_early += 1
        elif end_by_token.get(tok):
            holds.append(max(0.0, end_by_token[tok] - t0))
    if holds:
        m["median_hold_h"] = _median(holds) / 3600.0
        m["short_hold_share"] = sum(1 for h in holds if h < 1800) / len(holds)
    if first_buy:
        m["sell_before_end_share"] = sold_early / len(first_buy)
    # Доля мейкерских сделок: сравниваем тейкерские сделки с общими за то же окно.
    if taker_trades is not None and ts_list:
        lo = ts_list[0]
        tk = [t for t in taker_trades if _ts(t.get("timestamp")) >= lo]
        if rows:
            all_in = [t for t in rows if _ts(t.get("timestamp")) >= lo]
            if all_in:
                m["taker_share"] = min(1.0, len(tk) / len(all_in))
    return m


# ------------------------------------------------------ копируемость ----

def drift_metrics(samples: list[dict]) -> dict:
    """samples: [{'drift_3s': ¢, 'drift_60s': ¢}] — насколько цена ушла
    против нас через 3 и 60 секунд после их покупки."""
    d3 = [s["drift_3s"] for s in samples if s.get("drift_3s") is not None]
    d60 = [s["drift_60s"] for s in samples if s.get("drift_60s") is not None]
    return {"drift_samples": len(d3), "drift_3s_cents": _median(d3), "drift_60s_cents": _median(d60)}


# ------------------------------------------------------ итог ----

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_wallet(m: dict) -> tuple[float, str, list[str]]:
    """Оценка 0-100, вердикт и причины (по-русски)."""
    reasons: list[str] = []
    hard: list[str] = []
    n = m.get("n_resolved") or 0
    if n < 20:
        hard.append(f"мало закрытых ставок ({n} < 20) — не отличить навык от удачи")
    opd = m.get("orders_per_active_day")
    if (opd is not None and opd > 60) or (m.get("burst_share") or 0) > 0.5:
        hard.append(f"похоже на бота: ~{opd or 0:.0f} ордеров в активный день, "
                    f"{(m.get('burst_share') or 0) * 100:.0f}% подряд быстрее 10 с")
    fast = max(m.get("fast_share_trades") or 0, m.get("fast_share_pos") or 0)
    if fast > 0.5:
        hard.append(f"{fast * 100:.0f}% денег в быстрых крипто-рынках (5м/15м/час) — не успеть")
    mm = max(m.get("both_sides_share") or 0, 0)
    if mm > 0.35 or (m.get("mm_income_share") or 0) > 0.35:
        hard.append("маркет-мейкер/арбитраж (покупает обе стороны или живёт на ребейтах)")
    if (m.get("taker_share") is not None and m["taker_share"] < 0.15 and (m.get("n_orders_sample") or 0) > 50):
        hard.append(f"почти все сделки — лимитками-мейкером ({(1 - m['taker_share']) * 100:.0f}%)")
    if (m.get("top1_share") or 0) > 0.5 or ((m.get("pnl_ex_top3") or 0) <= 0 and n >= 20):
        hard.append("прибыль держится на 1-3 удачных ставках")
    roi = m.get("roi_resolved")
    if roi is not None and roi <= 0:
        hard.append(f"в минусе по закрытым ставкам (ROI {roi * 100:.1f}%)")
    if (m.get("pnl_90d") is not None and m["pnl_90d"] < 0):
        hard.append(f"последние 90 дней в минусе ({m['pnl_90d']:+.0f}$)")
    age = m.get("last_trade_age_h")
    if age is not None and age > 14 * 24:
        hard.append(f"не торгует {age / 24:.0f} дн.")
    hold = m.get("median_hold_h")
    if hold is not None and hold < 0.5:
        hard.append(f"держит позиции ~{hold * 60:.0f} мин — скальпер, не успеть")

    # --- баллы ---
    z = m.get("z_bets") or 0.0
    zu = m.get("z_usd") or 0.0
    s_skill = 30 * _clip01(min(z, zu + 1.0) / 4.0)
    s_roi = 15 * _clip01((roi or 0) / 0.30)
    pw = m.get("pos_weeks_share")
    s_weeks = 10 * _clip01(((pw if pw is not None else 0.5) - 0.45) / 0.35)
    dd = m.get("max_dd_90d")
    p90 = m.get("pnl_90d")
    if dd is not None and p90 and p90 > 0:
        s_dd = 10 * _clip01(1.0 - (dd / p90 - 0.2) / 0.8)
    else:
        s_dd = 3.0
    if hold is None:
        s_hold = 4.0
    else:
        s_hold = 10 * _clip01(math.log10(max(hold, 0.01) / 0.5) / math.log10(48 / 0.5))
    opd_v = opd if opd is not None else 5
    s_freq = 5.0 if 0.2 <= opd_v <= 25 else (2.0 if opd_v <= 60 else 0.0)
    d3 = m.get("drift_3s_cents")
    s_drift = 5 * _clip01(1.0 - ((d3 if d3 is not None else 1.5) - 0.5) / 3.0)
    top1 = m.get("top1_share")
    s_robust = 10 * (_clip01(1.0 - ((top1 if top1 is not None else 0.5) - 0.15) / 0.35)
                     if (m.get("pnl_ex_top3") or 0) > 0 else 0.0)
    s_recent = 5.0 if (age is not None and age <= 72) else (2.0 if age is not None and age <= 14 * 24 else 0.0)
    score = s_skill + s_roi + s_weeks + s_dd + s_hold + s_freq + s_drift + s_robust + s_recent
    fav = m.get("fav_share") or 0
    if fav > 0.6:
        score -= 10
        reasons.append(f"{fav * 100:.0f}% денег во входах ≥0.90 — мало апсайда, редкий проигрыш съедает всё")
    if (m.get("longshot_share") or 0) > 0.5:
        score -= 5
        reasons.append("в основном лотерейки ≤0.10 — результат очень шумный")
    if d3 is not None and d3 > 2:
        reasons.append(f"после их входа цена за 3 с уходит на {d3:.1f}¢ — копия будет хуже")
    if z >= 2 and zu >= 1.5:
        reasons.append(f"edge значим: {m.get('edge_cents', 0):+.1f}¢ на ставку, z={z:.1f}")
    elif n >= 20:
        reasons.append(f"edge {m.get('edge_cents', 0):+.1f}¢ на ставку, z={z:.1f} — пока не доказано")
    if hold is not None and hold >= 24:
        reasons.append(f"держит позиции ~{hold / 24:.1f} дн. — копировать легко")
    score = max(0.0, min(100.0, score))
    if hard:
        verdict = "❌ не копировать"
        reasons = hard + reasons
    elif score >= 65 and z >= 2 and zu >= 1.5:
        verdict = "✅ кандидат"
    elif score >= 45:
        verdict = "🟡 наблюдать (виртуально)"
    else:
        verdict = "⚪ слабый"
    return round(score, 1), verdict, reasons


def analyze_wallet(raw: dict, now: float | None = None) -> dict:
    """raw: stats, pnl_points, closed, redeemable, open, trades, taker_trades, drift."""
    now = now or time.time()
    resolved = resolved_positions(raw.get("closed") or [], raw.get("redeemable") or [])
    m: dict = {}
    m.update(edge_metrics(resolved, now))
    m.update(pnl_series_metrics(raw.get("pnl_points") or [], now))
    m.update(trade_metrics(raw.get("trades") or [], raw.get("taker_trades"), resolved, now))
    m.update(drift_metrics(raw.get("drift") or []))
    stats = raw.get("stats") or {}
    m["all_time_pnl"] = fnum(stats.get("all_time_pnl")) if stats.get("all_time_pnl") is not None else m.get("pnl_total")
    m["trades_lifetime"] = stats.get("trades")
    jd = stats.get("join_date")
    if jd:
        from src.markets import parse_iso
        jts = parse_iso(jd) if isinstance(jd, str) else _ts(jd)
        m["account_age_days"] = (now - jts) / 86400 if jts else None
    open_pos = raw.get("open") or []
    m["open_positions"] = len(open_pos)
    m["open_value"] = sum(fnum(p.get("current_value")) for p in open_pos)
    score, verdict, reasons = score_wallet(m)
    m["score"] = score
    m["verdict"] = verdict
    m["reasons"] = "; ".join(reasons)
    return m
