"""
Цена Chainlink — та, по которой Polymarket закрывает Up/Down-рынки.

Зачем. Рынок «Up» выигрывает, если цена Chainlink в конце окна не ниже, чем
в начале. Бот же мерил расстояние до страйка по Binance (BTCUSDT): и страйк,
и текущая цена — с другой биржи и в USDT. Разница с Chainlink бывает в
несколько bp — ровно столько, сколько отделяет вход по 0.90–0.95 от
проигрыша. По 122 реальным сделкам 09–12.10 проигрышей было 16 при 8.5
заложенных в цену — бот входил, когда «по Binance» было далеко от страйка,
а по Chainlink, на которую смотрят остальные участники рынка, — нет.

Теперь в режиме Chainlink (по умолчанию):
  - текущая цена — последняя цена Chainlink;
  - страйк — цена Chainlink на момент начала окна (из истории потока);
  - Binance остаётся для свечей: ATR, EMA, MACD;
  - нет свежей цены Chainlink или бот не застал начало окна — сигнала нет,
    вход запрещён (в отчёте причина).

Источники — оба от самого Polymarket, без сторонних ключей:
1. PolyBolt — новый поток цен Polymarket: wss://ws-live-v2.polymarket.com/ws,
   канал price.crypto (источник по умолчанию — Chainlink), символы btcusd,
   ethusd, … Нужны API-ключи CLOB (apiKey/secret/passphrase): берём из
   POLY_API_KEY/POLY_API_SECRET/POLY_API_PASSPHRASE, иначе из SDK-клиента или
   выводим из POLY_PRIVATE_KEY (только чтение существующих ключей —
   GET /auth/derive-api-key, новых не создаём).
2. Старый RTDS: wss://ws-live-data.polymarket.com, тема crypto_prices_chainlink,
   символы btc/usd, … Без ключей. Polymarket объявил его устаревшим и
   отключит — пока работает, держим запасным.
Цены обоих складываются в одну историю по монете (время — метка оракула).
"""
from __future__ import annotations
import asyncio
import bisect
import json
import logging
import random
import time
from dataclasses import dataclass, field

import websockets

from config import settings
from src import runtime_state

log = logging.getLogger("chainlink_feed")

HISTORY_SEC = 20 * 60          # сколько истории держим по монете
STRIKE_TOLERANCE_SEC = 5.0     # страйк: точка не дальше 5 с от начала окна
RTDS_TOPIC = "crypto_prices_chainlink"
RTDS_PING_SEC = 5              # старый RTDS требует текстовый PING каждые ~5 с
RTDS_STALL_SEC = 15            # 04.10 старый поток молчал по 30–100 минут, не закрывая сокет
POLYBOLT_PING_SEC = 20
POLYBOLT_STALL_SEC = 30
AUTH_TIMEOUT_SEC = 10
AUTH_FAIL_RETRY_SEC = 600      # ключи не подошли — не долбим сервер, пробуем раз в 10 мин
MAX_BACKOFF_SEC = 60
MAX_BACKOFF_DEAD_SEC = 300     # старый RTDS после отключения — раз в 5 мин


def _now() -> float:
    return time.time()


@dataclass
class Transport:
    name: str
    connected: bool = False
    last_data: float = 0.0
    reconnects: int = 0
    last_error: str = ""
    disabled: str = ""            # почему не запускается (нет ключей, выключен)
    symbols: set = field(default_factory=set)
    points: int = 0               # сколько точек цены получено всего


_hist_ts: dict[str, list[float]] = {}
_hist_val: dict[str, list[float]] = {}
_last_rx: dict[str, float] = {}      # монета -> наше время последней точки
_source: dict[str, str] = {}         # монета -> поставщик цены (chainlink, …)
_strikes: dict[str, float] = {}      # слаг рынка -> страйк Chainlink
_transports: dict[str, Transport] = {"polybolt": Transport("polybolt"), "rtds": Transport("rtds")}
# Кому ещё отдавать точки цены: fn(монета, время оракула с, цена, наше время с).
# Сейчас — ⚡ лаборатории скорости (src/speed_lab.py).
_listeners: list = []


def add_listener(fn) -> None:
    if fn not in _listeners:
        _listeners.append(fn)


def remove_listener(fn) -> None:
    if fn in _listeners:
        _listeners.remove(fn)


def reset() -> None:
    """Сброс состояния (для тестов)."""
    _hist_ts.clear()
    _hist_val.clear()
    _last_rx.clear()
    _source.clear()
    _strikes.clear()
    for k in list(_transports):
        _transports[k] = Transport(k)


# --- разбор сообщений ---

def asset_from_symbol(symbol) -> str | None:
    """"btc/usd", "btcusd", "BTCUSD", "btc-usd", "btcusdt" -> "btc"."""
    s = str(symbol or "").lower().strip()
    if not s:
        return None
    for sep in ("/", "-", "_"):
        if sep in s:
            return s.split(sep, 1)[0] or None
    for suffix in ("usdt", "usd"):
        if s.endswith(suffix) and len(s) > len(suffix):
            return s[: -len(suffix)]
    return s


def _to_sec(ts) -> float | None:
    try:
        v = float(ts)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v / 1000.0 if v > 1e11 else v


# PolyBolt отдаёт цену Chainlink Data Streams целым числом с 18 знаками
# (84019.04 приходит как 8.4019e22). 12.10 бот смешивал такие точки с
# обычными из старого RTDS: страйк и цена оказывались в разных единицах,
# расстояние до страйка — «бесконечным», фильтр по ATR не работал, и бот
# входил почти в каждый рынок, где одна из сторон стоила 0.90–0.94.
SCALED_ABOVE = 1e15          # ни одна наша монета не стоит дороже $1e15
SCALE = 1e18


def normalize_price(v: float) -> float:
    return v / SCALE if v > SCALED_ABOVE else v


def _to_price(p: dict):
    # value — обычные единицы; full_accuracy_value — запасной (может быть ×10^18)
    for key in ("value", "price", "full_accuracy_value", "fullAccuracyValue"):
        if p.get(key) is not None:
            try:
                v = float(p[key])
            except (TypeError, ValueError):
                continue
            if v > 0:
                return normalize_price(v)
    return None


def _point(p, symbol_default=None, source_default=None):
    """(монета, время с, цена, поставщик) из словаря точки или None."""
    if not isinstance(p, dict):
        return None
    asset = asset_from_symbol(p.get("symbol") or p.get("feed") or symbol_default)
    ts = _to_sec(p.get("timestamp") or p.get("ts") or p.get("t"))
    val = _to_price(p)
    if not asset or ts is None or val is None:
        return None
    src = str(p.get("source") or p.get("provider") or source_default or "").lower() or None
    return asset, ts, val, src


def _points_from_payload(payload, symbol_default=None, source_default=None) -> list:
    out = []
    if isinstance(payload, list):
        for p in payload:
            out += _points_from_payload(p, symbol_default, source_default)
        return out
    if not isinstance(payload, dict):
        return out
    sym = payload.get("symbol") or symbol_default
    src = payload.get("source") or payload.get("provider") or source_default
    for key in ("data", "points", "history", "prices"):
        if isinstance(payload.get(key), list):
            for p in payload[key]:
                pt = _point(p, sym, src)
                if pt:
                    out.append(pt)
            return out
    pt = _point(payload, sym, src)
    if pt:
        out.append(pt)
    return out


def parse_rtds(msg) -> list:
    """Старый RTDS: {"topic": "crypto_prices_chainlink", "type": "update",
    "payload": {"symbol": "btc/usd", "timestamp": ms, "value": float}};
    снапшот при подписке — payload с "data": [{timestamp, value}, …]."""
    if isinstance(msg, list):
        out = []
        for m in msg:
            out += parse_rtds(m)
        return out
    if not isinstance(msg, dict):
        return []
    topic = msg.get("topic")
    if topic and topic != RTDS_TOPIC:
        return []
    if "payload" in msg:
        return _points_from_payload(msg["payload"], source_default="chainlink")
    if "symbol" in msg:
        return _points_from_payload(msg, source_default="chainlink")
    return []


def parse_polybolt(msg) -> list:
    """PolyBolt: конверт {"v":1,"channel":"price.crypto","seq","ts","snapshot"?,
    "payload":{"symbol":"btcusd","value":…,"full_accuracy_value":"…","timestamp":ms,
    "source":"chainlink"}}; снапшот может прийти списком точек. TWAP и прочие
    каналы пропускаем."""
    if isinstance(msg, list):
        out = []
        for m in msg:
            out += parse_polybolt(m)
        return out
    if not isinstance(msg, dict) or "payload" not in msg:
        return []
    ch = str(msg.get("channel") or msg.get("topic") or "")
    if ch and ("twap" in ch or "crypto" not in ch):
        return []
    sym = msg.get("symbol")
    flt = msg.get("filter")
    if not sym and isinstance(flt, dict):
        sym = flt.get("symbol")
    return _points_from_payload(msg["payload"], sym)


def add_point(asset: str, ts_sec: float, value: float, source: str | None = None, rx: float | None = None) -> None:
    asset = asset.lower()
    tss = _hist_ts.setdefault(asset, [])
    vals = _hist_val.setdefault(asset, [])
    if not tss or ts_sec > tss[-1]:
        tss.append(ts_sec)
        vals.append(value)
    else:
        i = bisect.bisect_left(tss, ts_sec)
        if i < len(tss) and tss[i] == ts_sec:
            vals[i] = value
        else:
            tss.insert(i, ts_sec)
            vals.insert(i, value)
    _last_rx[asset] = _now() if rx is None else rx
    if source:
        _source[asset] = source
    cutoff = tss[-1] - HISTORY_SEC
    drop = bisect.bisect_left(tss, cutoff)
    if drop > 0:
        del tss[:drop]
        del vals[:drop]


def _store(points, transport: Transport) -> int:
    rx = _now()
    for asset, ts, val, src in points:
        add_point(asset, ts, val, src, rx=rx)
        transport.symbols.add(asset)
        for fn in _listeners:
            try:
                fn(asset, ts, val, rx)
            except Exception as exc:  # noqa: BLE001 — слушатель не должен ронять поток
                log.debug("Слушатель Chainlink упал: %s", exc)
    if points:
        transport.last_data = rx
        transport.points += len(points)
    return len(points)


# --- чтение ---

def latest(asset: str, now: float | None = None) -> tuple[float | None, float | None]:
    """(последняя цена, сколько секунд назад она пришла) или (None, None)."""
    a = asset.lower()
    vals = _hist_val.get(a)
    if not vals:
        return None, None
    now = _now() if now is None else now
    return vals[-1], max(0.0, now - _last_rx.get(a, 0.0))


def price_at(asset: str, ts_sec: float, tolerance_sec: float = STRIKE_TOLERANCE_SEC) -> float | None:
    """Цена на момент ts_sec: первая точка не раньше ts_sec (не дальше
    tolerance), иначе последняя точка чуть раньше (тоже в пределах). Нет —
    None: не гадаем."""
    a = asset.lower()
    tss = _hist_ts.get(a)
    if not tss:
        return None
    vals = _hist_val[a]
    i = bisect.bisect_left(tss, ts_sec)
    if i < len(tss) and tss[i] - ts_sec <= tolerance_sec:
        return vals[i]
    if i > 0 and ts_sec - tss[i - 1] <= tolerance_sec:
        return vals[i - 1]
    return None


def price_after(asset: str, ts_sec: float, tolerance_sec: float = STRIKE_TOLERANCE_SEC) -> float | None:
    """Первая точка не раньше ts_sec (не дальше tolerance). В отличие от
    price_at не берёт точку ДО ts_sec: цена на конце окна известна только
    когда пришла точка после его конца (иначе это ещё не цена закрытия)."""
    a = asset.lower()
    tss = _hist_ts.get(a)
    if not tss:
        return None
    i = bisect.bisect_left(tss, ts_sec)
    if i < len(tss) and tss[i] - ts_sec <= tolerance_sec:
        return _hist_val[a][i]
    return None


def strike_for(slug: str, asset: str, start_ts: float) -> float | None:
    """Страйк рынка по Chainlink: цена на момент начала окна. Найденный
    запоминаем — дальше рынок его не меняет."""
    v = _strikes.get(slug)
    if v is not None:
        return v
    v = price_at(asset, start_ts)
    if v is not None:
        if len(_strikes) > 2000:
            _strikes.clear()
        _strikes[slug] = v
    return v


def source_of(asset: str) -> str | None:
    return _source.get(asset.lower())


def max_age_sec() -> float:
    return max(0.5, float(settings.CHAINLINK_MAX_AGE_SEC))


# Цена и страйк Chainlink сверяются с Binance: обычная разница — десятые доли
# процента (USD против USDT). Больше 1% — значит, что-то не так с данными
# (единицы, зависший поток), и входить по ним нельзя.
MAX_BINANCE_GAP = 0.01


def _gap(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or not b:
        return None
    return abs(a / b - 1)


def mode() -> str:
    return "chainlink" if str(runtime_state.get("price_source") or "").lower() == "chainlink" else "binance"


@dataclass
class PriceView:
    """Цена и страйк, по которым считается сигнал."""
    source: str                   # "chainlink" | "binance"
    price: float | None
    strike: float | None
    binance_price: float | None
    binance_strike: float | None
    problem: str | None = None    # почему сигнал не считается (режим Chainlink без данных)

    @property
    def log_source(self) -> str:
        return "none" if self.problem else self.source


def view(asset: str, market, binance_price: float | None, now: float | None = None) -> PriceView:
    """Режим Binance — как раньше. Режим Chainlink — цена и страйк Chainlink;
    если их нет или цена несвежая — problem (вход запрещён)."""
    b_strike = getattr(market, "strike_price", None)
    if mode() != "chainlink":
        return PriceView("binance", binance_price, b_strike, binance_price, b_strike)
    price, age = latest(asset, now)
    strike = strike_for(market.slug, asset, market.start_time)
    problem = None
    if price is None:
        problem = "нет цены Chainlink"
    elif age > max_age_sec():
        problem = f"цена Chainlink старше {max_age_sec():g} с"
    elif strike is None:
        problem = "нет страйка Chainlink (бот не застал начало окна)"
    elif (_gap(price, binance_price) or 0) > MAX_BINANCE_GAP:
        problem = f"цена Chainlink не сходится с Binance ({price:.6g} против {binance_price:.6g})"
    elif (_gap(strike, b_strike) or 0) > MAX_BINANCE_GAP:
        problem = f"страйк Chainlink не сходится с Binance ({strike:.6g} против {b_strike:.6g})"
    return PriceView("chainlink", price, strike, binance_price, b_strike, problem)


def status_line(assets=None, now: float | None = None) -> str:
    """Строка для меню, «🔎 Почему нет сделок» и отчёта (без Markdown-символов)."""
    if mode() != "chainlink":
        return "📡 Цена сигнала: Binance (как раньше)"
    now = _now() if now is None else now
    assets = [a for a in (assets if assets is not None else settings.ASSETS) if runtime_state.is_asset_enabled(a)]
    parts = []
    for a in assets:
        price, age = latest(a, now)
        ok = price is not None and age <= max_age_sec()
        parts.append(f"{a.upper()} {'✅' if ok else '❌'}")
    tr = []
    for t in _transports.values():
        if t.disabled:
            tr.append(f"{'PolyBolt' if t.name == 'polybolt' else 'старый RTDS'}: {t.disabled}")
        else:
            alive = t.connected and now - t.last_data <= 30
            tr.append(f"{'PolyBolt' if t.name == 'polybolt' else 'старый RTDS'} {'✅' if alive else '❌'}")
    line = "📡 Цена сигнала: Chainlink — " + (", ".join(parts) if parts else "нет включённых монет")
    return (line + " | " + ", ".join(tr)).replace("_", " ")


def transports() -> dict:
    return dict(_transports)


# --- ключи CLOB для PolyBolt ---

_KEY_NAMES = ("api_key", "apikey", "apiKey")
_SECRET_NAMES = ("api_secret", "secret", "apiSecret", "secret_key")
_PASS_NAMES = ("api_passphrase", "passphrase", "apiPassphrase")


def _pick(obj, names):
    for n in names:
        v = obj.get(n) if isinstance(obj, dict) else getattr(obj, n, None)
        if isinstance(v, str) and v:
            return v
    return None


def _creds_from_obj(obj, depth: int = 0):
    """Ищем в объекте SDK-клиента (или вложенных) тройку ключ/секрет/пароль."""
    if obj is None or depth > 2:
        return None
    k, s, p = _pick(obj, _KEY_NAMES), _pick(obj, _SECRET_NAMES), _pick(obj, _PASS_NAMES)
    if k and s and p:
        return {"apiKey": k, "secret": s, "passphrase": p}
    try:
        items = obj.items() if isinstance(obj, dict) else vars(obj).items()
    except TypeError:
        return None
    for name, val in items:
        n = str(name).lower()
        if any(w in n for w in ("cred", "auth", "api", "l2", "key")) and not isinstance(val, (str, bytes, int, float)):
            found = _creds_from_obj(val, depth + 1)
            if found:
                return found
    return None


async def _derive_creds():
    """GET /auth/derive-api-key с подписью L1 (EIP-712 ClobAuth) — читает уже
    существующие ключи этого кошелька, ничего не создаёт. Нужен eth_account
    (обычно стоит вместе с SDK Polymarket); нет — пропускаем."""
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data
        import httpx
    except Exception:  # noqa: BLE001
        return None
    acct = Account.from_key(settings.POLY_PRIVATE_KEY)
    ts = str(int(_now()))
    typed = {
        "types": {
            "EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                             {"name": "chainId", "type": "uint256"}],
            "ClobAuth": [{"name": "address", "type": "address"}, {"name": "timestamp", "type": "string"},
                         {"name": "nonce", "type": "uint256"}, {"name": "message", "type": "string"}],
        },
        "primaryType": "ClobAuth",
        "domain": {"name": "ClobAuthDomain", "version": "1", "chainId": int(settings.POLY_CHAIN_ID)},
        "message": {"address": acct.address, "timestamp": ts, "nonce": 0,
                    "message": "This message attests that I control the given wallet"},
    }
    sig = Account.sign_message(encode_typed_data(full_message=typed), private_key=settings.POLY_PRIVATE_KEY).signature
    sig_hex = sig.hex() if hasattr(sig, "hex") else str(sig)
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex
    headers = {"POLY_ADDRESS": acct.address, "POLY_SIGNATURE": sig_hex, "POLY_TIMESTAMP": ts, "POLY_NONCE": "0"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{settings.POLY_HOST.rstrip('/')}/auth/derive-api-key", headers=headers)
        if resp.status_code != 200:
            return None
        data = resp.json()
    return _creds_from_obj(data)


async def find_creds():
    """Ключи CLOB для PolyBolt: переменные окружения, SDK-клиент, вывод из
    приватного ключа. Нет — None (тогда только старый RTDS)."""
    env = {"apiKey": settings.POLY_API_KEY, "secret": settings.POLY_API_SECRET,
           "passphrase": settings.POLY_API_PASSPHRASE}
    if all(env.values()):
        return env, "переменные окружения"
    if not settings.POLY_PRIVATE_KEY:
        return None, "нет POLY_PRIVATE_KEY и POLY_API_KEY"
    try:
        from src import polymarket_client
        client = await polymarket_client._get_client()
        found = _creds_from_obj(client)
        if found:
            return found, "SDK"
    except Exception as exc:  # noqa: BLE001
        log.info("PolyBolt: ключи из SDK не достали (%s)", type(exc).__name__)
    try:
        found = await _derive_creds()
        if found:
            return found, "выведены из приватного ключа"
    except Exception as exc:  # noqa: BLE001
        log.info("PolyBolt: вывести ключи не вышло (%s)", type(exc).__name__)
    return None, "нет API-ключей CLOB (задай POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE)"


# --- соединения ---

def _err(exc: BaseException) -> str:
    return (f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__)[:200]


async def _stall_watchdog(ws, t: Transport, stall_sec: float) -> None:
    while True:
        await asyncio.sleep(2)
        if _now() - t.last_data > stall_sec:
            t.last_error = f"нет цен {stall_sec:g} с"
            log.warning("Chainlink %s: %s — переподключаю", t.name, t.last_error)
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
            return


async def _rtds_pinger(ws) -> None:
    while True:
        await asyncio.sleep(RTDS_PING_SEC)
        await ws.send("PING")


async def run_rtds() -> None:
    t = _transports["rtds"]
    if not settings.CHAINLINK_USE_RTDS:
        t.disabled = "выкл"
        return
    backoff = 1.0
    sub = {"action": "subscribe", "subscriptions": [{"topic": RTDS_TOPIC, "type": "*"}]}
    while True:
        started = _now()
        try:
            async with websockets.connect(settings.CHAINLINK_RTDS_URL, ping_interval=None, max_size=2 ** 22,
                                          open_timeout=10, close_timeout=2) as ws:
                await ws.send(json.dumps(sub))
                t.connected = True
                t.last_data = _now()          # даём серверу время на первые цены
                log.info("Chainlink: старый RTDS подключён")
                tasks = [asyncio.create_task(_rtds_pinger(ws)),
                         asyncio.create_task(_stall_watchdog(ws, t, RTDS_STALL_SEC))]
                try:
                    async for raw in ws:
                        if not raw or raw in ("PONG", "pong"):
                            continue
                        try:
                            msg = json.loads(raw)
                        except (TypeError, ValueError):
                            continue
                        _store(parse_rtds(msg), t)
                finally:
                    for task in tasks:
                        task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — обрыв потока не должен ронять бота
            t.last_error = _err(exc)
            log.warning("Chainlink: старый RTDS оборвался (%s)", t.last_error)
        finally:
            t.connected = False
        t.reconnects += 1
        if _now() - started > 60:
            backoff = 1.0
        # Долго не подключается (отключили) — пробуем реже
        limit = MAX_BACKOFF_DEAD_SEC if t.reconnects > 10 and _now() - t.last_data > 600 else MAX_BACKOFF_SEC
        await asyncio.sleep(backoff)
        backoff = min(limit, backoff * 2)


async def _polybolt_pinger(ws) -> None:
    while True:
        await asyncio.sleep(POLYBOLT_PING_SEC)
        await ws.send(json.dumps({"op": "ping"}))


async def _polybolt_session(ws, t: Transport, creds: dict, assets) -> None:
    """Авторизация, подписка, чтение. Исключение AuthError — ключи не подошли."""
    await ws.send(json.dumps({"op": "auth", "rid": "auth-1", "auth": creds}))
    deadline = _now() + AUTH_TIMEOUT_SEC
    authed = False
    while _now() < deadline and not authed:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - _now()))
        except asyncio.TimeoutError:
            break
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            continue
        op = str(msg.get("op") or msg.get("type") or "").lower() if isinstance(msg, dict) else ""
        if op in ("authed", "auth_ok", "authenticated") or (op == "auth" and msg.get("ok")):
            authed = True
        elif op == "error" or (isinstance(msg, dict) and msg.get("error")):
            raise AuthError(str(msg.get("error") or msg.get("message") or msg)[:200])
    if not authed:
        log.warning("PolyBolt: нет ответа на авторизацию за %d с — подписываюсь всё равно", AUTH_TIMEOUT_SEC)
    # По одной подписке на кадр: неизвестный серверу символ (например, у монеты
    # нет цены Chainlink) не сломает подписку на остальные
    for i, a in enumerate(assets, 1):
        await ws.send(json.dumps({"op": "subscribe", "rid": f"sub-{i}",
                                  "subscriptions": [{"channel": "price.crypto", "filter": {"symbol": f"{a}usd"}}]}))
    t.connected = True
    t.last_data = _now()
    log.info("Chainlink: PolyBolt подключён (%s)", ", ".join(a.upper() for a in assets))
    tasks = [asyncio.create_task(_polybolt_pinger(ws)),
             asyncio.create_task(_stall_watchdog(ws, t, POLYBOLT_STALL_SEC))]
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if isinstance(msg, dict) and str(msg.get("op") or "").lower() == "error":
                t.last_error = str(msg.get("error") or msg.get("message") or msg)[:200]
                log.warning("PolyBolt: ошибка %s", t.last_error)
                continue
            _store(parse_polybolt(msg), t)
    finally:
        for task in tasks:
            task.cancel()


class AuthError(Exception):
    pass


def _close_code(obj):
    """Код закрытия соединения из сокета или исключения websockets (разные версии)."""
    for attr in ("rcvd", "sent"):
        frame = getattr(obj, attr, None)
        code = getattr(frame, "code", None) if frame is not None else None
        if code:
            return code
    code = getattr(obj, "close_code", None) or getattr(obj, "code", None)
    return code if isinstance(code, int) else None


def _wait_for_close(code, t: Transport):
    """Пауза перед переподключением по коду закрытия PolyBolt (None — обычная)."""
    if code == 4001:
        t.last_error = "авторизация: сервер отклонил ключи (4001)"
        return AUTH_FAIL_RETRY_SEC
    if code == 4003:
        return random.uniform(1, 5)      # сервер перезапускается — переподключиться один раз
    if code == 4008:
        return 60                         # превышены лимиты
    return None


async def run_polybolt() -> None:
    t = _transports["polybolt"]
    if not settings.CHAINLINK_USE_POLYBOLT:
        t.disabled = "выкл"
        return
    creds, how = await find_creds()
    while creds is None:
        t.disabled = how
        log.warning("Chainlink: PolyBolt не запущен — %s", how)
        await asyncio.sleep(AUTH_FAIL_RETRY_SEC)
        creds, how = await find_creds()
    t.disabled = ""
    log.info("Chainlink: ключи для PolyBolt — %s (%s)", how, creds_masked(creds))
    backoff = 1.0
    empty_sessions = 0
    while True:
        started = _now()
        wait = None
        points_before = t.points
        assets = [a.lower() for a in settings.ASSETS]
        try:
            async with websockets.connect(settings.CHAINLINK_POLYBOLT_URL, ping_interval=20, ping_timeout=20,
                                          max_size=2 ** 22, open_timeout=10, close_timeout=2) as ws:
                await _polybolt_session(ws, t, creds, assets)
                wait = _wait_for_close(_close_code(ws), t)
        except asyncio.CancelledError:
            raise
        except AuthError as exc:
            t.last_error = f"авторизация: {exc}"
            log.warning("Chainlink: PolyBolt — %s", t.last_error)
            wait = AUTH_FAIL_RETRY_SEC
        except Exception as exc:  # noqa: BLE001
            t.last_error = _err(exc)
            log.warning("Chainlink: PolyBolt оборвался (%s)", t.last_error)
            wait = _wait_for_close(_close_code(exc), t)
        finally:
            t.connected = False
        t.reconnects += 1
        if _now() - started > 60:
            backoff = 1.0
        # Подключились, но цен не пришло ни одной (формат подписки поменялся,
        # символов нет) — не переподключаемся каждые 30 с, а всё реже, до 10 мин
        if t.points == points_before:
            empty_sessions += 1
            wait = max(wait or 0, min(AUTH_FAIL_RETRY_SEC, 30 * 2 ** (empty_sessions - 1)))
        else:
            empty_sessions = 0
        await asyncio.sleep(wait if wait is not None else backoff)
        backoff = min(MAX_BACKOFF_SEC, backoff * 2)


async def run_forever() -> None:
    await asyncio.gather(run_polybolt(), run_rtds())


def creds_masked(creds: dict | None) -> str:
    """Для логов: ключ без секретов."""
    if not creds:
        return "нет"
    k = creds.get("apiKey", "")
    return (k[:4] + "…" + k[-2:]) if len(k) > 8 else "есть"

