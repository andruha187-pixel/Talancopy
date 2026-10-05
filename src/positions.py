"""
Книга наших позиций (виртуальных и реальных) — в памяти, с записью в БД
при каждом изменении. Одна открытая позиция на (кошелёк, режим, токен);
докупки усредняют цену, продажи фиксируют PnL пропорционально.

PnL честный: в себестоимость входит комиссия тейкера при покупке, из
выручки вычитается комиссия при продаже; выплата по резолюции — без
комиссии (так устроен Polymarket).
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from src import book_stream, db

DUST_SHARES = 0.01


@dataclass
class Position:
    id: int
    wallet_id: int
    mode: str                 # paper | live
    token_id: str
    condition_id: str | None = None
    slug: str | None = None
    title: str | None = None
    outcome: str | None = None
    end_ts: float | None = None
    shares: float = 0.0
    cost: float = 0.0         # остаток себестоимости открытой части (с комиссиями)
    fees: float = 0.0         # все уплаченные комиссии (вход + выход)
    proceeds: float = 0.0     # получено с продаж/резолюции (за вычетом комиссий)
    realized_pnl: float = 0.0
    opened_ts: int = 0
    closed_ts: int | None = None
    status: str = "open"
    close_reason: str | None = None
    target_avg_price: float = 0.0   # средняя цена копируемого по скопированным входам
    peak_bid: float = 0.0
    last_bid: float = 0.0
    last_bid_ts: float = 0.0
    entries: int = 0
    redeemed: int = 0
    invested: float = 0.0     # всего вложено за жизнь позиции (для ROI)
    exiting: bool = False     # идёт продажа — второй раз не запускаем
    exit_fail_ts: float = 0.0
    target_gone_seen: int = 0  # сколько сверок подряд копируемого уже нет в позиции

    @property
    def avg_price(self) -> float:
        return self.cost / self.shares if self.shares > 0 else 0.0

    def bid(self) -> float | None:
        b = book_stream.best_bid(self.token_id) if book_stream.is_fresh(self.token_id, 15000) else None
        if b is not None:
            self.last_bid = b
            self.last_bid_ts = time.time()
            return b
        return self.last_bid if self.last_bid_ts and time.time() - self.last_bid_ts < 120 else None

    def mark_value(self) -> float | None:
        b = self.bid()
        return None if b is None else self.shares * b

    def unrealized(self) -> float | None:
        v = self.mark_value()
        return None if v is None else v - self.cost

    def to_row(self) -> dict:
        return {
            "id": self.id, "wallet_id": self.wallet_id, "mode": self.mode, "token_id": self.token_id,
            "condition_id": self.condition_id, "slug": self.slug, "title": self.title,
            "outcome": self.outcome, "end_ts": self.end_ts, "shares": round(self.shares, 6),
            "cost_usdc": round(self.cost, 6), "fees_usdc": round(self.fees, 6),
            "proceeds_usdc": round(self.proceeds, 6), "realized_pnl": round(self.realized_pnl, 6),
            "opened_ts": self.opened_ts, "closed_ts": self.closed_ts, "status": self.status,
            "close_reason": self.close_reason, "target_avg_price": round(self.target_avg_price, 6),
            "peak_bid": self.peak_bid, "last_bid": self.last_bid, "entries": self.entries,
            "redeemed": self.redeemed,
        }


_positions: dict[int, Position] = {}
_open_idx: dict[tuple, int] = {}
_next_id = 1
# Реализованный PnL за сутки UTC: (день, режим, кошелёк) -> $. Держим в
# памяти — дневной стоп проверяется на каждой сделке, в БД ходить нельзя.
_daily_realized: dict[tuple, float] = {}


def _day() -> int:
    return int(time.time() // 86400)


def _book_realized(p: "Position", pnl: float) -> None:
    key = (_day(), p.mode, p.wallet_id)
    _daily_realized[key] = _daily_realized.get(key, 0.0) + pnl


def load() -> None:
    global _next_id
    _positions.clear()
    _open_idx.clear()
    row = db.query_one("SELECT MAX(id) AS m FROM positions")
    _next_id = int((row or {}).get("m") or 0) + 1
    for r in db.query("SELECT * FROM positions WHERE status = 'open'"):
        p = Position(
            id=r["id"], wallet_id=r["wallet_id"], mode=r["mode"], token_id=r["token_id"],
            condition_id=r["condition_id"], slug=r["slug"], title=r["title"], outcome=r["outcome"],
            end_ts=r["end_ts"], shares=r["shares"] or 0, cost=r["cost_usdc"] or 0, fees=r["fees_usdc"] or 0,
            proceeds=r["proceeds_usdc"] or 0, realized_pnl=r["realized_pnl"] or 0,
            opened_ts=r["opened_ts"] or 0, status="open", target_avg_price=r["target_avg_price"] or 0,
            peak_bid=r["peak_bid"] or 0, last_bid=r["last_bid"] or 0, entries=r["entries"] or 0,
            redeemed=r["redeemed"] or 0, invested=(r["cost_usdc"] or 0))
        _positions[p.id] = p
        _open_idx[(p.wallet_id, p.mode, p.token_id)] = p.id
        book_stream.subscribe([p.token_id])
    _daily_realized.clear()
    day_start = _day() * 86400
    for r in db.query("SELECT mode, wallet_id, COALESCE(SUM(pnl_usdc), 0) AS s FROM copies "
                      "WHERE ts >= ? AND pnl_usdc IS NOT NULL GROUP BY mode, wallet_id", (day_start,)):
        _daily_realized[(_day(), r["mode"], r["wallet_id"])] = float(r["s"] or 0.0)


def persist(p: Position) -> None:
    db.upsert_row("positions", p.to_row())


def get(pid: int) -> Position | None:
    return _positions.get(int(pid))


def get_open(wallet_id: int, mode: str, token_id: str) -> Position | None:
    pid = _open_idx.get((wallet_id, mode, token_id))
    return _positions.get(pid) if pid else None


def open_positions(wallet_id: int | None = None, mode: str | None = None) -> list[Position]:
    out = [p for p in _positions.values() if p.status == "open"]
    if wallet_id is not None:
        out = [p for p in out if p.wallet_id == wallet_id]
    if mode is not None:
        out = [p for p in out if p.mode == mode]
    return sorted(out, key=lambda p: p.opened_ts)


def open_cost(wallet_id: int | None = None, mode: str | None = None) -> float:
    return sum(p.cost for p in open_positions(wallet_id, mode))


def add_buy(wallet_id: int, mode: str, meta, shares: float, usdc: float, fee: float,
            target_price: float) -> Position:
    """Покупка: новая позиция или докупка в открытую."""
    global _next_id
    p = get_open(wallet_id, mode, meta.token_id)
    if p is None:
        p = Position(id=_next_id, wallet_id=wallet_id, mode=mode, token_id=meta.token_id,
                     condition_id=meta.condition_id, slug=meta.slug, title=meta.title,
                     outcome=meta.outcome, end_ts=meta.end_ts, opened_ts=int(time.time()))
        _next_id += 1
        _positions[p.id] = p
        _open_idx[(wallet_id, mode, meta.token_id)] = p.id
        book_stream.subscribe([meta.token_id])
    # Средняя цена копируемого — взвешиваем по нашим акциям.
    total = p.shares + shares
    if total > 0 and target_price > 0:
        p.target_avg_price = (p.target_avg_price * p.shares + target_price * shares) / total
    p.shares = total
    p.cost += usdc + fee
    p.invested += usdc + fee
    p.fees += fee
    p.entries += 1
    p.condition_id = p.condition_id or meta.condition_id
    p.slug = p.slug or meta.slug
    p.title = p.title or meta.title
    p.outcome = p.outcome or meta.outcome
    p.end_ts = p.end_ts or meta.end_ts
    persist(p)
    return p


def add_sell(p: Position, shares: float, usdc: float, fee: float, reason: str) -> float:
    """Продажа части/всей позиции. Возвращает реализованный PnL этой продажи."""
    shares = min(shares, p.shares)
    if shares <= 0:
        return 0.0
    frac = shares / p.shares if p.shares > 0 else 1.0
    basis = p.cost * frac
    net = usdc - fee
    pnl = net - basis
    p.shares -= shares
    p.cost -= basis
    p.fees += fee
    p.proceeds += net
    p.realized_pnl += pnl
    _book_realized(p, pnl)
    if p.shares < DUST_SHARES or (p.shares * max(p.last_bid, 0.01) < 0.05 and frac > 0.9):
        _close(p, reason)
    persist(p)
    return pnl


def settle(p: Position, payout: float, reason: str = "резолюция") -> float:
    """Рынок разрешён: каждая акция стоит payout (1, 0 или 0.5)."""
    value = p.shares * payout
    pnl = value - p.cost
    p.proceeds += value
    p.realized_pnl += pnl
    _book_realized(p, pnl)
    p.shares = 0.0
    p.cost = 0.0
    _close(p, reason)
    persist(p)
    return pnl


def _close(p: Position, reason: str) -> None:
    p.status = "closed"
    p.closed_ts = int(time.time())
    p.close_reason = reason
    p.shares = max(p.shares, 0.0)
    _open_idx.pop((p.wallet_id, p.mode, p.token_id), None)
    _positions.pop(p.id, None)
    still_used = any(x.token_id == p.token_id for x in _positions.values() if x.status == "open")
    if not still_used:
        book_stream.unsubscribe([p.token_id])


def realized_today(wallet_id: int | None = None, mode: str = "live") -> float:
    """Реализованный PnL за текущие сутки UTC (по кошельку или по всем)."""
    day = _day()
    return sum(v for (d, m, w), v in _daily_realized.items()
               if d == day and m == mode and (wallet_id is None or w == wallet_id))


def unrealized_total(wallet_id: int | None = None, mode: str = "live") -> float:
    total = 0.0
    for p in open_positions(wallet_id, mode):
        u = p.unrealized()
        if u is not None:
            total += u
    return total
