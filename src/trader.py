"""
Исполнение сделок: реальные (через официальный SDK polymarket-client) и
виртуальные (по живому стакану — так виртуальные результаты честные:
проскальзывание и комиссия считаются так же, как были бы в реальности).

Скорость реального ордера:
  * клиент SDK создаётся один раз и держит соединение (прогрев каждые 20 с);
  * метаданные рынка (тик, neg-risk, комиссия) SDK кэширует сам — мы
    прогреваем этот кэш заранее для токенов, которые держат копируемые, и
    параллельно с фильтрами для нового токена;
  * ордер — «защищённый» рыночный FAK с max_price: SDK не ходит за стаканом,
    а сразу подписывает и отправляет (1 запрос). FAK исполняет всё, что
    есть по цене не хуже потолка, остаток отменяется — лучше частичное
    исполнение, чем никакого (FOK на тонком стакане часто не исполняется).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from config import settings
from src import book_stream, http
from src.util import clamp_price, floor_tick, fnum, taker_fee

log = logging.getLogger("trader")


@dataclass
class ExecResult:
    ok: bool
    status: str = ""          # filled | partial | unfilled | rejected | error | delayed
    shares: float = 0.0
    usdc: float = 0.0         # сколько USDC ушло (BUY) / пришло (SELL), без комиссии
    fee: float = 0.0
    order_id: str = ""
    err: str = ""
    best_quote: float | None = None
    book_source: str = ""

    @property
    def avg_price(self) -> float:
        return self.usdc / self.shares if self.shares > 0 else 0.0


# ------------------------------------------------------------------ стакан ----

def _levels(raw, reverse: bool) -> list[tuple[float, float]]:
    out = []
    for lvl in raw or []:
        try:
            p = float(lvl.get("price") if isinstance(lvl, dict) else lvl[0])
            s = float(lvl.get("size") if isinstance(lvl, dict) else lvl[1])
        except (TypeError, ValueError, IndexError, AttributeError):
            continue
        if s > 0:
            out.append((p, s))
    return sorted(out, reverse=reverse)


async def get_book(token_id: str, allow_rest: bool = True) -> dict | None:
    """Стакан: живой из WebSocket, если свежий, иначе REST /book.
    {"asks": [(p, s)...] по возрастанию, "bids": [...] по убыванию, "tick", "source"}"""
    if book_stream.is_fresh(token_id):
        return {"asks": book_stream.asks(token_id), "bids": book_stream.bids(token_id),
                "tick": book_stream.tick_size(token_id), "source": "ws"}
    book_stream.subscribe([token_id])
    if not allow_rest:
        return None
    try:
        raw = await http.clob_get("/book", {"token_id": token_id}, timeout=3.0)
    except Exception as exc:  # noqa: BLE001
        log.debug("REST стакан %s: %s", token_id[:12], exc)
        return None
    if not isinstance(raw, dict):
        return None
    return {"asks": _levels(raw.get("asks"), reverse=False), "bids": _levels(raw.get("bids"), reverse=True),
            "tick": fnum(raw.get("tick_size"), 0.0) or None, "source": "rest",
            "neg_risk": raw.get("neg_risk"), "condition_id": raw.get("market")}


def simulate_buy(asks: list[tuple[float, float]], amount_usdc: float, cap: float,
                 fee_rate: float, fee_exp: float = 1.0) -> ExecResult:
    """Виртуальная покупка на amount_usdc по стакану, не дороже cap (как FAK)."""
    left = amount_usdc
    shares = usdc = fee = 0.0
    best = asks[0][0] if asks else None
    for price, size in asks:
        if price > cap + 1e-9 or left <= 1e-9:
            break
        take_usdc = min(left, price * size)
        sh = take_usdc / price
        shares += sh
        usdc += take_usdc
        fee += taker_fee(sh, price, fee_rate, fee_exp)
        left -= take_usdc
    if shares <= 0:
        return ExecResult(ok=False, status="unfilled", best_quote=best,
                          err="нет предложения до потолка цены" if asks else "стакан пуст")
    status = "filled" if left <= 0.01 else "partial"
    return ExecResult(ok=True, status=status, shares=shares, usdc=usdc, fee=fee, best_quote=best)


def simulate_sell(bids: list[tuple[float, float]], shares: float, min_price: float,
                  fee_rate: float, fee_exp: float = 1.0) -> ExecResult:
    left = shares
    sold = usdc = fee = 0.0
    best = bids[0][0] if bids else None
    for price, size in bids:
        if price < min_price - 1e-9 or left <= 1e-9:
            break
        sh = min(left, size)
        sold += sh
        usdc += sh * price
        fee += taker_fee(sh, price, fee_rate, fee_exp)
        left -= sh
    if sold <= 0:
        return ExecResult(ok=False, status="unfilled", best_quote=best,
                          err="нет спроса выше минимальной цены" if bids else "стакан пуст")
    status = "filled" if left <= 1e-6 else "partial"
    return ExecResult(ok=True, status=status, shares=sold, usdc=usdc, fee=fee, best_quote=best)


# ------------------------------------------------------------- реальные ----

_client = None
_client_lock = asyncio.Lock()
_client_error = ""
_balance_cache: tuple[float, float] | None = None   # (значение, время)
_warm_tokens: dict[str, float] = {}
stats = {"orders": 0, "order_ms_last": None, "warm_calls": 0, "prewarm_ok": 0}


def live_possible() -> bool:
    return bool(settings.POLY_PRIVATE_KEY)


def client_error() -> str:
    return _client_error


async def get_client():
    global _client, _client_error
    if _client is not None:
        return _client
    if not settings.POLY_PRIVATE_KEY:
        raise RuntimeError("POLY_PRIVATE_KEY не задан — реальная торговля невозможна")
    async with _client_lock:
        if _client is not None:
            return _client
        from polymarket import AsyncSecureClient  # тяжёлый импорт — только когда нужен
        kwargs = {"private_key": settings.POLY_PRIVATE_KEY}
        if settings.POLY_FUNDER_ADDRESS:
            kwargs["wallet"] = settings.POLY_FUNDER_ADDRESS
        try:
            _client = await AsyncSecureClient.create(**kwargs)
            _client_error = ""
            log.info("SDK клиент готов, кошелёк %s (%s)", getattr(_client, "wallet", "?"),
                     getattr(_client, "wallet_type", "?"))
        except Exception as exc:
            _client_error = f"{type(exc).__name__}: {exc}"[:300]
            raise
    return _client


def _field(obj, *names):
    for n in names:
        if obj is None:
            return None
        v = obj.get(n) if isinstance(obj, dict) else getattr(obj, n, None)
        if v is not None:
            return v
    return None


def _dec(x: float, places: str = "0.01") -> Decimal:
    return Decimal(str(x)).quantize(Decimal(places), rounding=ROUND_DOWN)


def _price_str(price: float, tick: float) -> str:
    tick_dec = Decimal(str(tick or 0.01)).normalize()
    exp = tick_dec.as_tuple().exponent
    places = Decimal(1).scaleb(exp) if exp < 0 else Decimal(1)
    return str(Decimal(str(price)).quantize(places, rounding=ROUND_DOWN))


async def warm(token_id: str) -> None:
    """Прогрев кэша метаданных рынка внутри SDK — чтобы в момент сделки
    SDK не делал 2 лишних запроса (markets-by-token + clob-markets)."""
    if not live_possible():
        return
    last = _warm_tokens.get(token_id, 0)
    if time.time() - last < 300:
        return
    _warm_tokens[token_id] = time.time()
    try:
        c = await get_client()
        ctx = getattr(c, "_ctx", None)
        cache = getattr(ctx, "order_metadata", None)
        if cache is not None and hasattr(cache, "resolve_market"):
            await cache.resolve_market(ctx, token_id=token_id)
        else:  # публичный путь: подписать (не отправляя) копеечный ордер
            await c.create_market_order(token_id=token_id, side="BUY", amount="1", max_price="0.5",
                                        order_type="FAK")
        stats["warm_calls"] += 1
    except Exception as exc:  # noqa: BLE001
        log.debug("Прогрев %s не удался: %s", token_id[:12], exc)


def _parse_response(resp, side: str) -> ExecResult:
    ok = _field(resp, "ok")
    if ok is None:
        ok = bool(_field(resp, "success")) and not _field(resp, "error_msg", "errorMsg")
    if not ok:
        code = str(_field(resp, "code") or "")
        msg = str(_field(resp, "message", "error_msg", "errorMsg") or code or "отклонён")
        if code == "not_enough_balance" or "balance" in msg.lower():
            return ExecResult(ok=False, status="rejected", err="недостаточно баланса (USDC или акций)")
        status = "unfilled" if code in ("fak_not_filled", "fok_not_filled", "unmatched") else "rejected"
        return ExecResult(ok=False, status=status, err=f"{code}: {msg}" if code else msg)
    making = fnum(_field(resp, "making_amount", "makingAmount"))
    taking = fnum(_field(resp, "taking_amount", "takingAmount"))
    order_id = str(_field(resp, "order_id", "orderID") or "")
    status = str(_field(resp, "status") or "").lower()
    if side == "BUY":
        usdc, shares = making, taking
    else:
        shares, usdc = making, taking
    if status == "delayed":
        return ExecResult(ok=True, status="delayed", order_id=order_id, shares=shares, usdc=usdc)
    if shares <= 0:
        return ExecResult(ok=False, status="unfilled", order_id=order_id, err="не исполнился")
    return ExecResult(ok=True, status="filled", shares=shares, usdc=usdc, order_id=order_id)


async def live_buy(token_id: str, amount_usdc: float, cap: float, tick: float,
                   fee_rate: float, fee_exp: float = 1.0) -> ExecResult:
    t0 = time.perf_counter()
    try:
        c = await get_client()
        cap = clamp_price(floor_tick(cap, tick), tick)
        resp = await c.place_market_order(token_id=token_id, side="BUY", amount=str(_dec(amount_usdc)),
                                          max_price=_price_str(cap, tick), order_type="FAK")
    except Exception as exc:  # noqa: BLE001
        return ExecResult(ok=False, status="error", err=_err_text(exc))
    finally:
        stats["orders"] += 1
        stats["order_ms_last"] = int((time.perf_counter() - t0) * 1000)
    res = _parse_response(resp, "BUY")
    if res.ok and res.shares > 0:
        res.fee = taker_fee(res.shares, res.avg_price, fee_rate, fee_exp)
        if res.status == "filled" and res.usdc < amount_usdc * 0.98:
            res.status = "partial"
    return res


async def live_sell(token_id: str, shares: float, min_price: float, tick: float,
                    fee_rate: float, fee_exp: float = 1.0) -> ExecResult:
    t0 = time.perf_counter()
    try:
        c = await get_client()
        min_price = clamp_price(floor_tick(min_price, tick), tick)
        resp = await c.place_market_order(token_id=token_id, side="SELL", shares=str(_dec(shares)),
                                          min_price=_price_str(min_price, tick), order_type="FAK")
    except Exception as exc:  # noqa: BLE001
        return ExecResult(ok=False, status="error", err=_err_text(exc))
    finally:
        stats["orders"] += 1
        stats["order_ms_last"] = int((time.perf_counter() - t0) * 1000)
    res = _parse_response(resp, "SELL")
    if res.ok and res.shares > 0:
        res.fee = taker_fee(res.shares, res.avg_price, fee_rate, fee_exp)
        if res.status == "filled" and res.shares < shares * 0.98:
            res.status = "partial"
    return res


def _err_text(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    low = text.lower()
    if "balance" in low or "allowance" in low:
        return "недостаточно USDC на балансе (или нет разрешения на списание)"
    if "geoblock" in low or "restricted" in low or "region" in low:
        return "площадка недоступна из региона сервера (геоблок)"
    return text[:300]


async def order_fill(order_id: str) -> tuple[float, float] | None:
    """(исполнено акций, цена ордера) — для ордеров со статусом delayed."""
    try:
        c = await get_client()
        o = await c.get_order(order_id=order_id)
        return fnum(_field(o, "size_matched")), fnum(_field(o, "price"))
    except Exception as exc:  # noqa: BLE001
        log.debug("get_order %s: %s", order_id, exc)
        return None


def cached_balance(max_age: float = 60.0) -> float | None:
    """Баланс из кэша без сетевого запроса (горячий путь); None — устарел."""
    if _balance_cache and time.time() - _balance_cache[1] < max_age:
        return _balance_cache[0]
    return None


async def balance_usdc(max_age: float = 5.0) -> float | None:
    global _balance_cache
    if _balance_cache and time.time() - _balance_cache[1] < max_age:
        return _balance_cache[0]
    try:
        c = await get_client()
        ba = await c.get_balance_allowance(asset_type="COLLATERAL")
        value = fnum(_field(ba, "balance")) / 1e6
        _balance_cache = (value, time.time())
        return value
    except Exception as exc:  # noqa: BLE001
        log.debug("Баланс: %s", exc)
        return None


async def token_balance(token_id: str) -> float | None:
    try:
        c = await get_client()
        ba = await c.get_balance_allowance(asset_type="CONDITIONAL", token_id=token_id)
        return fnum(_field(ba, "balance")) / 1e6
    except Exception as exc:  # noqa: BLE001
        log.debug("Баланс токена: %s", exc)
        return None


async def redeem(condition_id: str) -> str:
    """Забрать выигрыш по разрешённому рынку (gasless через релейер SDK)."""
    c = await get_client()
    handle = await c.redeem_positions(condition_id=condition_id)
    wait = getattr(handle, "wait", None)
    if wait is None:
        return "отправлено"
    outcome = wait()
    if asyncio.iscoroutine(outcome):
        outcome = await asyncio.wait_for(outcome, timeout=120)
    return str(_field(outcome, "status", "state") or outcome)[:120]


async def prewarm_loop(need_live) -> None:
    """Держим TLS-соединение SDK тёплым, пока есть реальные кошельки."""
    while True:
        await asyncio.sleep(20)
        if not live_possible() or not need_live():
            continue
        if await balance_usdc(max_age=0) is not None:
            stats["prewarm_ok"] += 1
