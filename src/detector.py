"""
Склейка и дедупликация сделок копируемых кошельков из трёх источников.

Одна и та же сделка приходит до трёх раз: из блокчейна (самый быстрый),
из потока RTDS и из опроса Data API. Ключ сделки —
(кошелёк, хеш транзакции, токен, сторона). Кто прислал первым — тот и
запускает копирование; остальные только дописывают свою задержку в
статистику (по ней видно, какой источник реально быстрее на твоём сервере).

Частичные исполнения: крупный ордер копируемого может исполниться об
несколько встречных ордеров. Если источник присылает исполнение целиком
(тейкер-ордер в блокчейне, orders_matched в RTDS, ответ Data API) —
копируем сразу. Если по кусочкам — ждём AGGREGATE_WINDOW_MS и копируем
сумму одним ордером (иначе вышла бы пачка ордеров меньше $1).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field

from config import settings
from src import db, state
from src.util import now_ms

log = logging.getLogger("detector")

SOURCES = ("chain", "rtds", "rtds_fills", "poll")
_DB_COL = {"chain": "chain_ms", "rtds": "rtds_ms", "rtds_fills": "rtds_ms", "poll": "poll_ms"}


@dataclass
class TargetFill:
    wallet: str
    token_id: str
    side: str            # BUY | SELL — сторона копируемого
    shares: float
    usdc: float
    tx_hash: str
    source: str
    detected_ms: int
    uid: str
    trade_ts: float | None = None
    role: str | None = None   # taker | maker
    complete: bool = False
    condition_id: str | None = None
    slug: str | None = None
    title: str | None = None
    outcome: str | None = None
    event_slug: str | None = None


@dataclass
class TargetTrade:
    """Сделка копируемого после склейки — то, что получает копировщик."""
    wallet: str
    token_id: str
    side: str
    tx_hash: str
    shares: float = 0.0
    usdc: float = 0.0
    first_source: str = ""
    first_ms: int = 0
    trade_ts: float | None = None
    role: str | None = None
    condition_id: str | None = None
    slug: str | None = None
    title: str | None = None
    outcome: str | None = None
    event_slug: str | None = None
    uids: set = field(default_factory=set)
    src_ms: dict = field(default_factory=dict)
    flushed: bool = False
    timer: asyncio.TimerHandle | None = None

    @property
    def key(self) -> tuple:
        return (self.wallet, self.tx_hash, self.token_id, self.side)

    @property
    def price(self) -> float:
        return self.usdc / self.shares if self.shares > 0 else 0.0

    def hint(self) -> dict:
        return {"condition_id": self.condition_id, "slug": self.slug, "title": self.title,
                "outcome": self.outcome, "event_slug": self.event_slug}

    def age_sec(self) -> float | None:
        return (time.time() - self.trade_ts) if self.trade_ts else None


class Detector:
    def __init__(self):
        self.handler = None           # async fn(TargetTrade)
        self.pending: dict[tuple, TargetTrade] = {}
        self.done: "OrderedDict[tuple, TargetTrade]" = OrderedDict()
        self.first_wins = {s: 0 for s in SOURCES}
        self.seen = {s: 0 for s in SOURCES}
        self.latency = {s: deque(maxlen=300) for s in SOURCES}   # мс от времени блока
        self.lag_vs_first = {s: deque(maxlen=300) for s in SOURCES}  # мс отставания от первого
        # Контроль: одна транзакция с разной стороной из разных источников —
        # значит, какой-то источник трактует сторону иначе (см. «Источники»).
        self.side_seen: "OrderedDict[tuple, tuple]" = OrderedDict()
        self.side_mismatch = 0
        self.side_mismatch_last = ""

    # ------------------------------------------------------------ приём ----
    def submit(self, fill: TargetFill) -> None:
        if fill.shares <= 0 or fill.usdc <= 0:
            return
        if fill.wallet not in state.active_addresses():
            return
        key = (fill.wallet, fill.tx_hash, fill.token_id, fill.side)
        self.seen[fill.source] = self.seen.get(fill.source, 0) + 1
        self._check_side(fill)
        if fill.source == "rtds_fills" and not settings.RTDS_FILLS_TRIGGER and key not in self.done \
                and key not in self.pending:
            return

        tt = self.pending.get(key) or self.done.get(key)
        if tt is None:
            tt = TargetTrade(wallet=fill.wallet, token_id=fill.token_id, side=fill.side,
                             tx_hash=fill.tx_hash, first_source=fill.source, first_ms=fill.detected_ms)
            self._absorb(tt, fill, add_amounts=True)
            if fill.complete or settings.AGGREGATE_WINDOW_MS <= 0:
                self._flush(tt)
            else:
                self.pending[key] = tt
                loop = asyncio.get_running_loop()
                tt.timer = loop.call_later(settings.AGGREGATE_WINDOW_MS / 1000.0, self._flush, tt)
            return

        same_source = fill.source == tt.first_source
        if same_source and fill.uid in tt.uids:
            return  # точный дубль
        if same_source and not tt.flushed:
            self._absorb(tt, fill, add_amounts=True)
            if fill.complete:
                if tt.timer:
                    tt.timer.cancel()
                self._flush(tt)
            return
        if same_source and tt.flushed:
            # Поздний кусок того же ордера из того же источника — копируем
            # только приращение (как докупку/допродажу).
            inc = TargetTrade(wallet=tt.wallet, token_id=tt.token_id, side=tt.side,
                              tx_hash=tt.tx_hash, first_source=fill.source, first_ms=fill.detected_ms)
            self._absorb(inc, fill, add_amounts=True)
            tt.uids.add(fill.uid)
            tt.shares += fill.shares
            tt.usdc += fill.usdc
            inc.flushed = True
            self._dispatch(inc)
            return
        # Другой источник — только статистика задержек и подсказки.
        self._absorb(tt, fill, add_amounts=False)

    def _check_side(self, fill: TargetFill) -> None:
        k = (fill.wallet, fill.tx_hash, fill.token_id)
        prev = self.side_seen.get(k)
        if prev is None:
            self.side_seen[k] = (fill.side, fill.source)
            while len(self.side_seen) > 5000:
                self.side_seen.popitem(last=False)
        elif prev[0] != fill.side and prev[1] != fill.source:
            self.side_mismatch += 1
            self.side_mismatch_last = f"{prev[1]}={prev[0]} vs {fill.source}={fill.side} tx {fill.tx_hash[:12]}"
            log.warning("Разная сторона сделки у источников: %s", self.side_mismatch_last)

    def _absorb(self, tt: TargetTrade, fill: TargetFill, add_amounts: bool) -> None:
        if add_amounts:
            tt.shares += fill.shares
            tt.usdc += fill.usdc
            tt.uids.add(fill.uid)
        tt.role = tt.role or fill.role
        tt.condition_id = tt.condition_id or fill.condition_id
        tt.slug = tt.slug or fill.slug
        tt.title = tt.title or fill.title
        tt.outcome = tt.outcome or fill.outcome
        tt.event_slug = tt.event_slug or fill.event_slug
        new_ts = fill.trade_ts and not tt.trade_ts
        if fill.trade_ts and not tt.trade_ts:
            tt.trade_ts = fill.trade_ts
        if fill.source not in tt.src_ms:
            tt.src_ms[fill.source] = fill.detected_ms
            self.lag_vs_first[fill.source].append(fill.detected_ms - tt.first_ms)
            if tt.trade_ts:
                self.latency[fill.source].append(fill.detected_ms - tt.trade_ts * 1000)
            if tt.flushed:
                self._db_late_source(tt, fill)
        if new_ts:
            # Время блока стало известно позже — досчитываем задержки тех,
            # кто уже успел прислать.
            for src, ms in tt.src_ms.items():
                if src != fill.source:
                    self.latency[src].append(ms - tt.trade_ts * 1000)

    def _flush(self, tt: TargetTrade) -> None:
        if tt.flushed:
            return
        tt.flushed = True
        tt.timer = None
        self.pending.pop(tt.key, None)
        self.done[tt.key] = tt
        self.first_wins[tt.first_source] = self.first_wins.get(tt.first_source, 0) + 1
        self._db_insert(tt)
        self._dispatch(tt)

    def _dispatch(self, tt: TargetTrade) -> None:
        if self.handler is None:
            return
        task = asyncio.get_running_loop().create_task(self.handler(tt))
        task.add_done_callback(_log_task_error)

    # ------------------------------------------------------------- БД ----
    def _db_insert(self, tt: TargetTrade) -> None:
        w = state.by_address(tt.wallet)
        col = _DB_COL.get(tt.first_source, "poll_ms")
        row = {
            "wallet_id": w.id if w else None, "wallet": tt.wallet, "tx_hash": tt.tx_hash,
            "token_id": tt.token_id, "side": tt.side, "shares": round(tt.shares, 6),
            "usdc": round(tt.usdc, 6), "price": round(tt.price, 6), "trade_ts": tt.trade_ts,
            "first_source": tt.first_source, "first_ms": tt.first_ms, col: tt.first_ms,
            "role": tt.role, "condition_id": tt.condition_id, "slug": tt.slug,
            "title": tt.title, "outcome": tt.outcome,
        }
        cols = ",".join(row.keys())
        marks = ",".join("?" for _ in row)
        db.write(f"INSERT OR IGNORE INTO target_trades ({cols}) VALUES ({marks})", list(row.values()))

    def _db_late_source(self, tt: TargetTrade, fill: TargetFill) -> None:
        col = _DB_COL.get(fill.source, "poll_ms")
        db.write(
            f"UPDATE target_trades SET {col} = COALESCE({col}, ?), trade_ts = COALESCE(trade_ts, ?), "
            "condition_id = COALESCE(condition_id, ?), slug = COALESCE(slug, ?), "
            "title = COALESCE(title, ?), outcome = COALESCE(outcome, ?), role = COALESCE(role, ?) "
            "WHERE wallet = ? AND tx_hash = ? AND token_id = ? AND side = ?",
            (fill.detected_ms, fill.trade_ts, fill.condition_id, fill.slug, fill.title, fill.outcome,
             fill.role, tt.wallet, tt.tx_hash, tt.token_id, tt.side))

    # ----------------------------------------------------------- служебное ----
    def prune(self, max_age_sec: float = 3 * 3600) -> None:
        cutoff = now_ms() - max_age_sec * 1000
        while self.done:
            key, tt = next(iter(self.done.items()))
            if tt.first_ms >= cutoff:
                break
            self.done.popitem(last=False)

    def latency_summary(self) -> dict:
        out = {}
        for s in SOURCES:
            lat = sorted(self.latency[s])
            lag = sorted(self.lag_vs_first[s])
            out[s] = {
                "seen": self.seen.get(s, 0),
                "first": self.first_wins.get(s, 0),
                "median_ms": lat[len(lat) // 2] if lat else None,
                "lag_median_ms": lag[len(lag) // 2] if lag else None,
            }
        return out


def _log_task_error(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        log.error("Ошибка обработки сделки копируемого: %r", exc, exc_info=exc)


detector = Detector()


async def prune_loop() -> None:
    while True:
        await asyncio.sleep(60)
        detector.prune()
