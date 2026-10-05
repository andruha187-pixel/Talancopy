"""Статистика по каждому кошельку (виртуальные и реальные — раздельно)."""
from __future__ import annotations

import re
import time

from src import db, positions


def _norm_reason(text: str | None) -> str:
    """'цена ушла: ask 0.47 > потолок 0.45' -> 'цена ушла'; числа убираем,
    чтобы одинаковые причины группировались."""
    t = (text or "—").split(":")[0]
    t = re.sub(r"\(.*?\)", "", t)
    t = re.sub(r"[\d$.,%¢+−-]+", "", t)
    return re.sub(r"\s+", " ", t).strip()[:45] or "—"


def wallet_stats(wallet_id: int, mode: str, since_ts: int = 0) -> dict:
    """Синхронная функция (ходит в БД) — вызывать через asyncio.to_thread."""
    rows = db.query(
        "SELECT action, status, skip_reason, fill_usdc, fee_usdc, slippage_cents, detect_latency_ms, "
        "exec_ms, pnl_usdc, detect_source FROM copies WHERE wallet_id = ? AND mode = ? AND ts >= ?",
        (wallet_id, mode, since_ts))
    buys = [r for r in rows if r["action"] == "BUY"]
    filled = [r for r in buys if r["status"] in ("filled", "partial")]
    skipped = [r for r in buys if r["status"] in ("skipped", "accumulating")]
    failed = [r for r in buys if r["status"] not in ("filled", "partial", "skipped", "accumulating", "delayed")]
    realized = sum(r["pnl_usdc"] or 0.0 for r in rows if r["pnl_usdc"] is not None)
    fees = sum(r["fee_usdc"] or 0.0 for r in rows if r["fee_usdc"])
    invested = sum((r["fill_usdc"] or 0.0) + (r["fee_usdc"] or 0.0) for r in filled)
    slips = [r["slippage_cents"] for r in filled if r["slippage_cents"] is not None]
    lats = sorted(r["detect_latency_ms"] for r in filled if r["detect_latency_ms"] is not None)
    execs = sorted(r["exec_ms"] for r in filled if r["exec_ms"] is not None)
    sources: dict[str, int] = {}
    for r in filled:
        sources[r["detect_source"] or "—"] = sources.get(r["detect_source"] or "—", 0) + 1
    reasons: dict[str, int] = {}
    for r in skipped + failed:
        k = _norm_reason(r["skip_reason"] or r["status"])
        reasons[k] = reasons.get(k, 0) + 1

    closed = db.query(
        "SELECT realized_pnl FROM positions WHERE wallet_id = ? AND mode = ? AND status = 'closed' "
        "AND COALESCE(closed_ts, 0) >= ?", (wallet_id, mode, since_ts))
    wins = sum(1 for r in closed if (r["realized_pnl"] or 0) > 0.0001)
    losses = sum(1 for r in closed if (r["realized_pnl"] or 0) < -0.0001)

    open_pos = positions.open_positions(wallet_id, mode)
    open_cost = sum(p.cost for p in open_pos)
    unreal = 0.0
    unmarked = 0
    for p in open_pos:
        u = p.unrealized()
        if u is None:
            unmarked += 1
        else:
            unreal += u
    total = realized + unreal
    return {
        "copies": len(filled), "skipped": len(skipped), "failed": len(failed),
        "realized": realized, "unrealized": unreal, "total": total, "fees": fees,
        "invested": invested, "roi": (total / invested * 100.0) if invested > 0 else None,
        "open": len(open_pos), "open_cost": open_cost, "unmarked": unmarked,
        "closed": len(closed), "wins": wins, "losses": losses,
        "avg_slip": (sum(slips) / len(slips)) if slips else None,
        "lat_median": lats[len(lats) // 2] if lats else None,
        "exec_median": execs[len(execs) // 2] if execs else None,
        "sources": sources,
        "reasons": sorted(reasons.items(), key=lambda kv: -kv[1])[:5],
    }


def today_start() -> int:
    return int(time.time() // 86400) * 86400
