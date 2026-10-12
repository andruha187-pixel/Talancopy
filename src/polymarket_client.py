"""
Обёртка над polymarket-client — официальным "унифицированным" Python SDK
Polymarket (import как `polymarket`).

История пакетов для этого проекта (все — реальные ошибки, с которыми
столкнулись на практике, не гипотетические):
  py-clob-client (v1)     -> архивирован, шлёт заведомо отклоняемые ордера
  py-clob-client-v2       -> сам Polymarket в официальном migration guide
                              велит с него уходить; плюс открытые баги с
                              "maker address not allowed" для части кошельков
  polymarket-client       -> актуальный официальный SDK, САМ определяет тип
                              кошелька (EOA/прокси/deposit wallet) вместо
                              того, чтобы просить вручную угадывать
                              signature_type — это как раз обходит класс
                              багов из py-clob-client-v2. Асинхронный.

Инициализация клиента ленивая — если PRIVATE_KEY не задан (например, ты
сначала хочешь погонять бота в DRY_RUN на паблик-данных), модуль всё
равно позволяет читать orderbook без авторизации через AsyncPublicClient.

ВАЖНО: SDK находится в статусе beta (это подтверждено самой документацией
Polymarket) — если после обновления вылезет что-то новое в духе смены
формата ответа, это ожидаемо для этой стадии проекта, не признак ошибки
в самом боте. Перед LIVE обязательно прогони scripts/test_live_order.py.
"""
from __future__ import annotations
import inspect
import math
import time
from dataclasses import dataclass

from config import settings
from src import book_stream

_client = None
_last_prewarm_ms = 0
PREWARM_INTERVAL_MS = 20_000


@dataclass
class OrderBookSnapshot:
    best_bid: float | None
    best_ask: float | None
    ask_liquidity_usdc: float  # сумма price*size по верхним уровням asks
    tick_size: float = 0.01
    source: str = "rest"       # "ws" (живой стакан) или "rest" (фолбэк)
    # Уровни asks [(цена, размер), ...] по возрастанию цены — только у REST-
    # снимка (у WS их берём из book_stream). Нужны для fillable_usdc.
    asks: list | None = None


async def _get_client():
    global _client
    if _client is not None:
        return _client

    if not settings.POLY_PRIVATE_KEY:
        # Read-only режим — публичные эндпоинты без авторизации
        from polymarket import AsyncPublicClient
        _client = AsyncPublicClient()
        return _client

    from polymarket import AsyncSecureClient

    kwargs = dict(private_key=settings.POLY_PRIVATE_KEY)
    if settings.POLY_FUNDER_ADDRESS:
        # В этом SDK параметр называется `wallet`, не `funder` (как в
        # py-clob-client-v2) — это адрес аккаунта, которым торгуешь, если
        # он отличается от адреса, выведенного из приватного ключа.
        kwargs["wallet"] = settings.POLY_FUNDER_ADDRESS

    # Сознательно НЕ передаём signature_type — SDK определяет тип кошелька
    # сам (виден в client.wallet_type после создания).
    _client = await AsyncSecureClient.create(**kwargs)
    return _client


def _field(obj, key):
    """Достаём поле независимо от того, dict это или объект с атрибутами —
    разные версии/эндпоинты SDK отдают то так, то так."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


# Публичный алиас — executor.py читает поля ответа ордера тем же способом,
# не дублируя логику dict-vs-объект у себя.
response_field = _field


def _to_level(lvl) -> tuple[float, float] | None:
    price = _field(lvl, "price")
    size = _field(lvl, "size")
    try:
        return float(price), float(size)
    except (TypeError, ValueError):
        return None


async def get_orderbook(token_id: str, depth_levels: int = 5) -> OrderBookSnapshot:
    """REST-фолбэк (публичный эндпоинт). Используется, только если живой
    WS-стакан ещё не прогрелся или устарел — см. get_orderbook_cached."""
    client = await _get_client()
    book = await client.get_order_book(token_id=token_id)

    raw_asks = _field(book, "asks") or []
    raw_bids = _field(book, "bids") or []

    asks = sorted((lv for lv in (_to_level(l) for l in raw_asks) if lv), key=lambda x: x[0])
    bids = sorted((lv for lv in (_to_level(l) for l in raw_bids) if lv), key=lambda x: x[0], reverse=True)

    best_ask = asks[0][0] if asks else None
    best_bid = bids[0][0] if bids else None

    ask_liquidity = sum(price * size for price, size in asks[:depth_levels])
    tick_raw = _field(book, "tick_size") or _field(book, "tickSize")
    tick = float(tick_raw) if tick_raw else 0.01

    return OrderBookSnapshot(best_bid=best_bid, best_ask=best_ask, ask_liquidity_usdc=ask_liquidity,
                              tick_size=tick, source="rest", asks=asks)


def ws_snapshot(token_id: str, depth_levels: int = 5,
                max_age_ms: int = book_stream.MAX_BOOK_AGE_MS) -> OrderBookSnapshot | None:
    """Стакан только из живого WS-кэша, без запроса. None — если по токену
    нет свежего стакана или в нём нет asks. Быстрая проверка сигнала
    (src/fast_signal.py) зовёт это до трёх раз в секунду на монету, поэтому
    в REST отсюда не ходим никогда."""
    if not book_stream.is_fresh(token_id, max_age_ms):
        return None
    best_ask = book_stream.best_ask(token_id)
    if best_ask is None:
        return None
    return OrderBookSnapshot(best_bid=book_stream.best_bid(token_id), best_ask=best_ask,
                             ask_liquidity_usdc=book_stream.ask_liquidity_usdc(token_id, depth_levels),
                             tick_size=book_stream.tick_size(token_id), source="ws")


async def get_orderbook_cached(token_id: str, depth_levels: int = 5) -> OrderBookSnapshot:
    """
    Стакан "из прогрева": сначала смотрим в живой WS-кэш (book_stream) — там
    цена обновляется пушем с сервера без дополнительного сетевого раунд-трипа
    в момент принятия решения. Если кэш пуст или протух — падаем в REST.
    """
    snap = ws_snapshot(token_id, depth_levels)
    if snap is not None:
        return snap
    return await get_orderbook(token_id, depth_levels)


def fillable_usdc(token_id: str, max_price: float, book: OrderBookSnapshot) -> float:
    """Сколько USDC реально можно купить FOK-ордером с потолком max_price:
    сумма price*size по уровням asks не дороже потолка. Берём уровни из того
    же снимка, что уже получен (REST) или из живого WS-стакана — без лишнего
    запроса в момент входа."""
    if book.asks is not None:
        return sum(price * size for price, size in book.asks if price <= max_price + 1e-9)
    if book.source == "ws":
        return book_stream.ask_liquidity_upto(token_id, max_price)
    return book.ask_liquidity_usdc


def round_price_for_buy(price: float, tick_size: float) -> float:
    """
    Выравниваем цену BUY по шагу тика ВНИЗ (никогда не платим больше, чем
    планировали). Используется как верхний предел (worst-price) для
    market-ордера — защита от слиппеджа.
    """
    if tick_size <= 0:
        return round(price, 2)
    steps = math.floor(price / tick_size + 1e-9)
    return round(steps * tick_size, 6)


async def prewarm_transport() -> bool:
    """
    Прогрев авторизованного HTTP-транспорта: безобидный read-only запрос
    баланса, который выполняет тот же путь (соединение, TLS, аутентификация),
    что и реальный ордер, но не имеет торгового эффекта. Вызывается фоновой
    задачей (fire-and-forget), только когда DRY_RUN=false и ключ настроен.
    """
    global _last_prewarm_ms
    now = int(time.time() * 1000)
    if now - _last_prewarm_ms < PREWARM_INTERVAL_MS:
        return False
    if not settings.POLY_PRIVATE_KEY:
        return False
    try:
        client = await _get_client()
        await client.get_balance_allowance(asset_type="COLLATERAL")
        _last_prewarm_ms = now
        return True
    except Exception:
        _last_prewarm_ms = now
        return False


async def place_buy_order(token_id: str, price_cap: float, amount_usdc: float, tick_size: float = 0.01,
                          order_type: str = "FOK") -> dict:
    """
    Market-ордер BUY. amount_usdc — ДОЛЛАРОВАЯ сумма к трате (для BUY
    market-ордеров в этом SDK amount — USD-номинал до комиссии, а не
    количество акций). max_price — худшая допустимая цена исполнения:
    с ним SDK ставит ордер ровно по этой цене и не запрашивает стакан.

    order_type:
      "FOK" — всё или ничего (так работают хедж и копитрейдинг);
      "FAK" — исполнить сколько есть по цене не хуже потолка, остаток
              отменить (так входит стратегия — см. executor._execute_live_buy).
    """
    client = await _get_client()
    return await client.place_market_order(
        token_id=token_id,
        side="BUY",
        amount=str(round(amount_usdc, 2)),
        max_price=str(price_cap),
        order_type=order_type,
    )


def filled_amounts(resp) -> tuple[float, float] | None:
    """(USDC потрачено, акций получено) из ответа на BUY-ордер: makingAmount /
    takingAmount (в SDK — making_amount / taking_amount). None — если сумм в
    ответе нет или они не числа."""
    making = _field(resp, "making_amount")
    if making is None:
        making = _field(resp, "makingAmount")
    taking = _field(resp, "taking_amount")
    if taking is None:
        taking = _field(resp, "takingAmount")
    try:
        return float(making), float(taking)
    except (TypeError, ValueError):
        return None


_NO_FILL_MARKERS = ("fully filled", "no orders found", "no match", "insufficientliquidity",
                    "insufficient liquidity", "couldn't be", "could not be", "fok order", "fak order")


def is_no_fill_error(exc: BaseException) -> bool:
    """Ордер отклонён, потому что по нашей цене в стакане уже нечего купить
    (цена ушла, заявки сняли) — такое имеет смысл повторить по свежему
    стакану. Ошибки баланса, подписи и т.п. повторять бессмысленно."""
    text = f"{type(exc).__name__}: {exc}".lower()
    return any(marker in text for marker in _NO_FILL_MARKERS)


_warmed_tokens: set[str] = set()


async def prewarm_market(token_ids: list[str]) -> int:
    """Прогрев рынка в SDK для токенов нового окна. Перед первым ордером по
    токену SDK запрашивает метаданные рынка (шаг цены, тип рынка, комиссию) —
    лишний сетевой запрос ровно в момент сигнала, а 5m-рынок новый каждые
    5 минут. create_market_order подписывает ордер локально и НЕ отправляет
    его, но по пути кладёт метаданные в кэш SDK. Ошибки игнорируем: прогрев
    необязателен."""
    if not settings.POLY_PRIVATE_KEY:
        return 0
    try:
        client = await _get_client()
    except Exception:  # noqa: BLE001
        return 0
    create = getattr(client, "create_market_order", None)
    if create is None:
        return 0
    if len(_warmed_tokens) > 5000:
        _warmed_tokens.clear()
    warmed = 0
    for token_id in token_ids:
        if not token_id or token_id in _warmed_tokens:
            continue
        _warmed_tokens.add(token_id)
        try:
            res = create(token_id=token_id, side="BUY", amount="5", max_price="0.5", order_type="FAK")
            if inspect.isawaitable(res):
                await res
            warmed += 1
        except Exception:  # noqa: BLE001
            pass
    return warmed


async def place_sell_order(token_id: str, shares: float, min_price: float | None = None) -> dict:
    """
    Market-ордер SELL с исполнением FOK — досрочное закрытие позиции
    (стоп-лосс по проценту, см. executor.check_position_stop_losses).

    ВАЖНО: для SELL этот SDK использует параметр `shares` (количество акций),
    а не `amount`, как для BUY (доллары) — это подтверждено официальной
    документацией Polymarket отдельно от BUY-примеров, не мой домысел по
    аналогии. min_price — защита от слиппеджа на продаже (не даём продать
    дешевле этой цены); если конкретная версия SDK не примет этот kwarg,
    отправляем без него — не хотим падать всей функцией из-за
    необязательного параметра защиты в SDK, который всё ещё в статусе beta.
    """
    # Округляем через Decimal, а не float: 14.29*0.94 как float даёт
    # 13.432599999999999 (артефакт двоичного представления десятичных
    # дробей), а не чисто 13.4326 — биржа отклоняет такие "грязные" числа
    # (реальный случай на хедж-боте, 2026-09-22: "invalid amounts...
    # max accuracy of 2 decimals... max of 4 decimals").
    from decimal import Decimal, ROUND_DOWN
    shares_dec = Decimal(str(shares)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    client = await _get_client()
    kwargs = dict(token_id=token_id, side="SELL", shares=str(shares_dec), order_type="FOK")
    if min_price is not None:
        price_dec = Decimal(str(min_price)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        kwargs["min_price"] = str(price_dec)
    try:
        return await client.place_market_order(**kwargs)
    except TypeError:
        kwargs.pop("min_price", None)
        return await client.place_market_order(**kwargs)
