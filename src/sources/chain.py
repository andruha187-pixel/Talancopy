"""
Источник №1 — блокчейн Polygon: подписка на событие OrderFilled биржевых
контрактов Polymarket V2, отфильтрованное по адресу копируемых кошельков.

Почему это самый быстрый публичный путь: Polymarket сводит ордера у себя и
сразу отправляет сделку в блокчейн; новый блок Polygon — каждые ~2 с, и
WebSocket-провайдер присылает лог в момент появления блока. Data API
индексирует то же самое позже, а RTDS — непредсказуемо.

Событие (github.com/Polymarket/ctf-exchange-v2, ITrading.sol):
  OrderFilled(bytes32 indexed orderHash, address indexed maker,
              address indexed taker, uint8 side, uint256 tokenId,
              uint256 makerAmountFilled, uint256 takerAmountFilled,
              uint256 fee, bytes32 builder, bytes32 metadata)
maker — кошелёк, чей ордер исполнился (копируемый, неважно, был он тейкером
или мейкером). Если taker == адрес биржи — это его тейкер-ордер целиком.
BUY: makerAmount — USDC, takerAmount — акции. SELL: наоборот. Всё в 1e6.

Можно указать несколько провайдеров (POLYGON_WSS_URLS через запятую) — бот
слушает все параллельно, дубли склеивает детектор, а в статистике видно,
кто быстрее.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
from collections import OrderedDict

import websockets

from config import settings
from src import state
from src.detector import TargetFill, detector
from src.util import dumps, loads, now_ms

log = logging.getLogger("chain")

TOPIC_ORDER_FILLED_V2 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
HEARTBEAT_TIMEOUT_SEC = 40


def pad_topic(addr: str) -> str:
    return "0x" + "0" * 24 + addr.lower()[2:]


def topic_to_addr(topic: str) -> str:
    return "0x" + topic.lower()[-40:]


def parse_log(entry: dict, block_ts: dict | None = None) -> TargetFill | None:
    topics = entry.get("topics") or []
    if len(topics) < 4 or str(topics[0]).lower() != TOPIC_ORDER_FILLED_V2 or entry.get("removed"):
        return None
    data = str(entry.get("data") or "")
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 64 * 5:
        return None
    try:
        side_i, token_id, maker_amt, taker_amt, fee = (int(data[i:i + 64], 16) for i in range(0, 64 * 5, 64))
    except ValueError:
        return None
    maker = topic_to_addr(topics[2])
    taker = topic_to_addr(topics[3])
    if side_i == 0:
        side, usdc, shares = "BUY", maker_amt / 1e6, taker_amt / 1e6
    else:
        side, shares, usdc = "SELL", maker_amt / 1e6, taker_amt / 1e6
    role = "taker" if taker in settings.EXCHANGE_ADDRESSES else "maker"
    tx = str(entry.get("transactionHash") or "").lower()
    try:
        log_index = int(str(entry.get("logIndex") or "0x0"), 16)
        block = int(str(entry.get("blockNumber") or "0x0"), 16)
    except ValueError:
        log_index, block = 0, 0
    trade_ts = block_ts.get(block) if block_ts else None
    return TargetFill(wallet=maker, token_id=str(token_id), side=side, shares=shares, usdc=usdc,
                      tx_hash=tx, source="chain", detected_ms=now_ms(), uid=f"{tx}:{log_index}",
                      trade_ts=trade_ts, role=role, complete=(role == "taker"))


class ChainListener:
    def __init__(self, url: str, index: int):
        self.url = url
        self.index = index
        self.changed = asyncio.Event()
        self.connected = False
        self.subscribed_wallets = 0
        self.events = 0
        self.last_event_ms = 0
        self.last_head_ms = 0
        self.head_lag_ms: int | None = None
        self.error = ""
        self.reconnects = 0
        self._ids = itertools.count(1)
        self._logs_sub: str | None = None
        self._pending_logs_req: int | None = None
        self._block_ts: "OrderedDict[int, float]" = OrderedDict()

    def name(self) -> str:
        host = self.url.split("//")[-1].split("/")[0]
        return f"#{self.index + 1} {host}"

    def _logs_filter(self, wallets: set[str]) -> dict:
        return {"address": settings.EXCHANGE_ADDRESSES,
                "topics": [TOPIC_ORDER_FILLED_V2, None, [pad_topic(w) for w in sorted(wallets)]]}

    async def _subscribe_logs(self, ws) -> None:
        wallets = state.active_addresses()
        if self._logs_sub:
            await ws.send(dumps({"jsonrpc": "2.0", "id": next(self._ids), "method": "eth_unsubscribe",
                                 "params": [self._logs_sub]}))
            self._logs_sub = None
        self.subscribed_wallets = len(wallets)
        if not wallets:
            return
        req_id = next(self._ids)
        self._pending_logs_req = req_id
        await ws.send(dumps({"jsonrpc": "2.0", "id": req_id, "method": "eth_subscribe",
                             "params": ["logs", self._logs_filter(wallets)]}))

    async def _resubscriber(self, ws) -> None:
        while True:
            await self.changed.wait()
            self.changed.clear()
            await self._subscribe_logs(ws)

    async def _heartbeat_guard(self, ws) -> None:
        while True:
            await asyncio.sleep(10)
            if self.last_head_ms and now_ms() - self.last_head_ms > HEARTBEAT_TIMEOUT_SEC * 1000:
                log.warning("Polygon %s: нет новых блоков %ds — переподключаюсь", self.name(),
                            HEARTBEAT_TIMEOUT_SEC)
                await ws.close()
                return

    def _on_head(self, head: dict) -> None:
        try:
            number = int(head.get("number"), 16)
            ts = int(head.get("timestamp"), 16)
        except (TypeError, ValueError):
            return
        self.last_head_ms = now_ms()
        self.head_lag_ms = self.last_head_ms - ts * 1000
        self._block_ts[number] = float(ts)
        while len(self._block_ts) > 64:
            self._block_ts.popitem(last=False)

    async def run_forever(self) -> None:
        backoff = 1.0
        while True:
            if not state.active_addresses():
                self.connected = False
                await self.changed.wait()
                continue
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20,
                                              max_size=2 ** 22, open_timeout=15) as ws:
                    self.connected = True
                    self.error = ""
                    self._logs_sub = None
                    self.changed.clear()
                    heads_req = next(self._ids)
                    await ws.send(dumps({"jsonrpc": "2.0", "id": heads_req, "method": "eth_subscribe",
                                         "params": ["newHeads"]}))
                    await self._subscribe_logs(ws)
                    heads_sub = None
                    tasks = [asyncio.create_task(self._resubscriber(ws)),
                             asyncio.create_task(self._heartbeat_guard(ws))]
                    backoff = 1.0
                    try:
                        async for raw in ws:
                            msg = loads(raw)
                            if "id" in msg and "method" not in msg:
                                if msg.get("error"):
                                    self.error = str(msg["error"])[:200]
                                    log.warning("Polygon %s: ошибка подписки %s", self.name(), self.error)
                                elif msg.get("id") == heads_req:
                                    heads_sub = msg.get("result")
                                elif msg.get("id") == self._pending_logs_req:
                                    self._logs_sub = msg.get("result")
                                    log.info("Polygon %s: слежу за %d кошельками", self.name(),
                                             self.subscribed_wallets)
                                continue
                            params = msg.get("params") or {}
                            result = params.get("result")
                            if not isinstance(result, dict):
                                continue
                            sub = params.get("subscription")
                            if sub == heads_sub or ("number" in result and "topics" not in result):
                                self._on_head(result)
                                continue
                            fill = parse_log(result, self._block_ts)
                            if fill:
                                self.events += 1
                                self.last_event_ms = fill.detected_ms
                                detector.submit(fill)
                    finally:
                        for t in tasks:
                            t.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.error = f"{type(exc).__name__}: {exc}"[:200]
                log.warning("Polygon %s отключился: %s", self.name(), self.error)
            self.connected = False
            self.reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)


listeners: list[ChainListener] = []


def build_listeners() -> list[ChainListener]:
    listeners.clear()
    if settings.CHAIN_ENABLED:
        for i, url in enumerate(settings.POLYGON_WSS_URLS):
            listeners.append(ChainListener(url, i))
    return listeners


def wallets_changed() -> None:
    for lst in listeners:
        lst.changed.set()
