"""
Сбор кандидатов и сырых данных по кошелькам из Data API v2 / Gamma.
Все запросы идут через отдельный ограничитель частоты (discovery_limiter),
чтобы долгий поиск не мешал опросу сделок копируемых.
"""
from __future__ import annotations

import asyncio
import logging
import time

from src import db, http
from src.discovery.analyze import decisions, is_fast, stats_summary
from src.util import fnum, norm_addr

log = logging.getLogger("discovery.collect")

LEADERBOARD_CATEGORIES = ["overall", "politics", "sports", "crypto", "culture", "economics", "finance",
                          "tech", "geopolitics", "weather", "mentions", "esports"]

# Сколько строк тянуть по кошельку. Закрытые/непогашенные отсортированы по
# последнему событию; analyze.sample_window() по этим лимитам понимает, за
# какой период выборка полная.
CAP_CLOSED = 1000
CAP_REDEEMABLE = 500
CAP_OPEN = 300
CAP_ACTIVITY = 1000
CAP_TAKER = 1000


def stats_pnl(stats: dict | None, default: float | None = None) -> float | None:
    """Торговый PnL за всё время из /v2/user-stats (там all_time_pnl —
    объект, а не число)."""
    v = stats_summary(stats)["trading_pnl"]
    return default if v is None else v


async def _get(path: str, params: dict, retries: int = 2):
    return await http.data_get(path, params, limiter=http.discovery_limiter, retries=retries, timeout=20.0)


async def paged(path: str, params: dict, max_rows: int, page: int = 500) -> list[dict]:
    out: list[dict] = []
    cursor = None
    while len(out) < max_rows:
        p = dict(params)
        p["limit"] = min(page, max_rows - len(out))
        if cursor:
            p["cursor"] = cursor
        payload = await _get(path, p)
        items = http.data_items(payload)
        out.extend(items)
        cursor = http.next_cursor(payload)
        if not cursor or not items:
            break
    return out[:max_rows]


# --------------------------------------------------------------- кандидаты ----

async def leaderboard(time_period: str, category: str, rows: int, sort_by: str = "PNL") -> list[dict]:
    try:
        items = await paged("/v2/leaderboard", {"time_period": time_period, "category": category,
                                                "sort_by": sort_by}, rows, page=100)
    except http.ApiError as exc:
        log.debug("leaderboard %s/%s: %s", time_period, category, exc)
        return []
    out = []
    for it in items:
        addr = norm_addr(it.get("user_id") or it.get("proxy_wallet") or it.get("proxyWallet"))
        if addr:
            out.append({"wallet": addr, "name": it.get("user_name") or it.get("userName") or "",
                        "pnl": fnum(it.get("pnl")), "volume": fnum(it.get("volume")),
                        "rank": it.get("rank"), "src": f"lb:{time_period}:{category}"})
    return out


async def big_trades(min_usdc: float = 2000, rows: int = 1000) -> list[dict]:
    try:
        items = await paged("/v2/trades", {"filter_type": "CASH", "filter_amount": min_usdc}, rows, page=500)
    except http.ApiError as exc:
        log.debug("big trades: %s", exc)
        return []
    agg: dict[str, dict] = {}
    for t in items:
        addr = norm_addr(t.get("proxy_wallet") or t.get("proxyWallet"))
        if not addr or is_fast(t.get("slug"), t.get("event_slug")):
            continue
        a = agg.setdefault(addr, {"wallet": addr, "name": t.get("name") or t.get("pseudonym") or "",
                                  "usdc": 0.0, "n": 0, "src": "big_trades"})
        a["usdc"] += fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
        a["n"] += 1
    return list(agg.values())


async def top_markets(n_events: int = 25) -> list[str]:
    """condition_id крупнейших живых рынков (кроме быстрых крипто)."""
    try:
        payload = await http.gamma_get("/events", {"active": "true", "closed": "false", "order": "volume24hr",
                                                   "ascending": "false", "limit": n_events}, timeout=15.0)
    except Exception as exc:  # noqa: BLE001
        log.debug("gamma events: %s", exc)
        return []
    events = payload if isinstance(payload, list) else (payload or {}).get("events") or (payload or {}).get("data") or []
    cids = []
    for ev in events:
        if is_fast(ev.get("slug")):
            continue
        markets = sorted(ev.get("markets") or [], key=lambda m: -fnum(m.get("volume24hr") or m.get("volumeNum")))
        for m in markets[:3]:
            if m.get("conditionId") and not m.get("closed"):
                cids.append(m["conditionId"])
    return list(dict.fromkeys(cids))


async def holders(condition_ids: list[str], per_market: int = 20) -> list[dict]:
    out: dict[str, dict] = {}
    for cid in condition_ids:
        try:
            payload = await _get("/v2/holders", {"condition_id": cid, "limit": per_market, "include_pnl": "true"},
                                 retries=1)
        except http.ApiError as exc:
            log.debug("holders %s: %s", cid[:10], exc)
            continue
        for item in http.data_items(payload):
            group = item.get("holders") if isinstance(item.get("holders"), list) else [item]
            for h in group:
                addr = norm_addr(h.get("proxy_wallet") or h.get("proxyWallet"))
                if not addr:
                    continue
                rec = out.setdefault(addr, {"wallet": addr, "name": h.get("name") or h.get("pseudonym") or "",
                                            "market_pnl": 0.0, "n": 0, "src": "holders"})
                rec["market_pnl"] += fnum(h.get("total_pnl"))
                rec["n"] += 1
    return list(out.values())


async def biggest_winners(time_period: str = "month", rows: int = 100) -> list[dict]:
    try:
        items = await paged("/v2/biggest-winners", {"time_period": time_period}, rows, page=100)
    except http.ApiError as exc:
        log.debug("biggest winners: %s", exc)
        return []
    out = []
    for it in items:
        addr = norm_addr(it.get("proxy_wallet") or it.get("user_id") or it.get("wallet"))
        if addr:
            out.append({"wallet": addr, "name": it.get("user_name") or it.get("name") or "", "src": "winners"})
    return out


def census_candidates(limit: int = 300) -> list[dict]:
    since = int(time.time()) - 14 * 86400
    rows = db.query("SELECT wallet, name, n_trades, usdc, big_trades FROM census WHERE last_ts >= ? "
                    "AND n_trades >= 5 ORDER BY usdc DESC LIMIT ?", (since, limit))
    return [{"wallet": r["wallet"], "name": r["name"] or "", "usdc": r["usdc"], "n": r["n_trades"],
             "src": "census"} for r in rows]


# --------------------------------------------------------------- по кошельку ----

async def user_stats(wallet: str) -> dict | None:
    try:
        payload = await _get("/v2/user-stats", {"user": wallet}, retries=1)
    except http.ApiError as exc:
        if exc.status == 404:
            return None
        raise
    data = payload.get("data") if isinstance(payload, dict) else payload
    return data if isinstance(data, dict) else None


async def pnl_points(wallet: str) -> list[dict]:
    try:
        payload = await _get("/v2/user-pnl", {"user": wallet, "interval": "all", "fidelity": "1d"})
    except http.ApiError as exc:
        log.debug("user-pnl %s: %s", wallet[:10], exc)
        return []
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict):
        pts = data.get("points")
        return pts if isinstance(pts, list) else []
    return data if isinstance(data, list) else []


async def positions(wallet: str, status: str, rows: int, sort_by: str = "TIMESTAMP") -> list[dict]:
    """status: OPEN (держит сейчас, включая разрешённые непогашенные),
    CLOSED (продал или погасил), REDEEMABLE (разрешённые непогашенные,
    в том числе проигранные — REDEEMABLE_LOST их подмножество)."""
    try:
        return await paged("/v2/positions", {"user": wallet, "status": status, "sort_by": sort_by,
                                             "sort_direction": "DESC"}, rows, page=500)
    except http.ApiError as exc:
        log.debug("positions %s %s: %s", status, wallet[:10], exc)
        return []


async def activity(wallet: str, rows: int = 1000) -> list[dict]:
    try:
        return await paged("/v2/activity", {"user": wallet, "type": "TRADE"}, rows, page=1000)
    except http.ApiError as exc:
        log.debug("activity %s: %s", wallet[:10], exc)
        return []


async def taker_trades(wallet: str, start_ts: int | None, rows: int = 1000) -> list[dict]:
    params = {"user": wallet, "taker_only": "true"}
    if start_ts:
        params["start"] = int(start_ts)
    try:
        return await paged("/v2/trades", params, rows, page=1000)
    except http.ApiError as exc:
        log.debug("taker trades %s: %s", wallet[:10], exc)
        return []


async def market_trades_after(condition_id: str, start_ts: int, seconds: int = 90) -> list[dict]:
    """Сделки рынка в окне [start_ts, start_ts + seconds]. Data API отдаёт
    последние сделки до end (start, судя по ответам, не применяет), поэтому
    окно режем сами."""
    try:
        payload = await _get("/v2/trades", {"condition_id": condition_id, "start": int(start_ts) - 600,
                                            "end": int(start_ts + seconds), "limit": 300, "taker_only": "true"},
                             retries=1)
    except http.ApiError:
        return []
    out = []
    for r in http.data_items(payload):
        t = fnum(r.get("timestamp"))
        t = t / 1000 if t > 1e12 else t
        if start_ts <= t <= start_ts + seconds:
            out.append(r)
    return out


async def drift_samples(trades: list[dict], max_samples: int = 12, min_usdc: float = 20.0) -> list[dict]:
    """Насколько цена уходит против копирующего: берём их последние
    покупки-решения (склейка исполнений одного токена, не быстрые рынки,
    от $20) и смотрим цену того же исхода в сделках других людей через
    ≥3 с и в течение минуты после."""
    rows = [t for t in trades if str(t.get("side") or "").upper() == "BUY"]
    picks = []
    for d in sorted(decisions(rows), key=lambda d: -d["t0"]):
        if d["usdc"] < min_usdc or d["shares"] <= 0 or is_fast(d["slug"], d["event_slug"]) or not d["cond"]:
            continue
        picks.append(d)
        if len(picks) >= max_samples:
            break
    own_tx = {str(t.get("transaction_hash") or "").lower() for t in trades}
    out = []
    for d in picks:
        ts = d["t0"]
        token = d["token"]
        price = d["usdc"] / d["shares"]
        after = await market_trades_after(d["cond"], int(ts), 90)
        pts = []
        for r in after:
            rts = fnum(r.get("timestamp"))
            rts = rts / 1000 if rts > 1e12 else rts
            if rts < ts + 3 or str(r.get("transaction_hash") or "").lower() in own_tx:
                continue
            p = fnum(r.get("price"))
            if p <= 0:
                continue
            same = str(r.get("token_id") or r.get("asset") or "") == token
            pts.append((rts, p if same else 1.0 - p))
        pts.sort()
        if not pts:
            out.append({"ts": ts, "token": token, "price": price, "slug": d["slug"],
                        "drift_3s": None, "drift_60s": None})
            continue
        first = pts[0][1]
        within = [p for rts, p in pts if rts <= ts + 60]
        mid = sorted(within)[len(within) // 2] if within else None
        out.append({"ts": ts, "token": token, "price": price, "slug": d["slug"],
                    "drift_3s": round((first - price) * 100, 3),
                    "drift_60s": round((mid - price) * 100, 3) if mid is not None else None})
    return out


async def fetch_wallet(wallet: str, stats: dict | None = None, with_drift: bool = False,
                       detail_rows: int = CAP_CLOSED) -> dict:
    """Всё сырьё по кошельку для analyze_wallet()."""
    if stats is None:
        try:
            stats = await user_stats(wallet)
        except Exception:  # noqa: BLE001
            stats = None
    pts_t = asyncio.create_task(pnl_points(wallet))
    closed_t = asyncio.create_task(positions(wallet, "CLOSED", detail_rows))
    redeem_t = asyncio.create_task(positions(wallet, "REDEEMABLE", CAP_REDEEMABLE))
    open_t = asyncio.create_task(positions(wallet, "OPEN", CAP_OPEN, sort_by="CURRENT_VALUE"))
    act_t = asyncio.create_task(activity(wallet, CAP_ACTIVITY))
    trades = await act_t
    lo = None
    if trades:
        tss = [fnum(t.get("timestamp")) for t in trades if t.get("timestamp")]
        lo = int(min(tss) / 1000 if tss and min(tss) > 1e12 else min(tss)) if tss else None
    taker = await taker_trades(wallet, lo, CAP_TAKER)
    raw = {"wallet": wallet, "stats": stats or {}, "pnl_points": await pts_t, "closed": await closed_t,
           "redeemable": await redeem_t, "open": await open_t, "trades": trades, "taker_trades": taker,
           "caps": {"closed": detail_rows, "redeemable": CAP_REDEEMABLE, "taker": CAP_TAKER}}
    raw["drift"] = await drift_samples(trades) if with_drift else []
    return raw
