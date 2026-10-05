"""Мелкие общие помощники: время, адреса, форматирование, комиссии, тики."""
from __future__ import annotations

import html
import json
import math
import re
import time

try:  # orjson в 3-10 раз быстрее на разборе потоков (RTDS, логи блокчейна)
    import orjson as _orjson

    def loads(raw):
        return _orjson.loads(raw)

    def dumps(obj) -> str:
        return _orjson.dumps(obj).decode()
except Exception:  # noqa: BLE001 — локально/в тестах orjson может не быть
    def loads(raw):
        return json.loads(raw)

    def dumps(obj) -> str:
        return json.dumps(obj, separators=(",", ":"))


ADDR_RE = re.compile(r"0x[0-9a-fA-F]{40}")


def now_ms() -> int:
    return int(time.time() * 1000)


def now_s() -> float:
    return time.time()


def norm_addr(addr: str | None) -> str | None:
    """'0xAbC…' -> '0xabc…'; None, если это не адрес."""
    if not addr:
        return None
    m = ADDR_RE.fullmatch(addr.strip())
    return m.group(0).lower() if m else None


def find_addr(text: str | None) -> str | None:
    """Первый адрес в произвольном тексте (в т.ч. в ссылке на профиль)."""
    if not text:
        return None
    m = ADDR_RE.search(text)
    return m.group(0).lower() if m else None


def short_addr(addr: str) -> str:
    return f"{addr[:6]}…{addr[-4:]}" if addr and len(addr) > 12 else (addr or "")


def fnum(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def esc(text) -> str:
    return html.escape(str(text if text is not None else ""), quote=False)


def money(x: float, sign: bool = False) -> str:
    x = fnum(x)
    if sign:
        return f"{'+' if x >= 0 else '−'}${abs(x):,.2f}"
    return f"${x:,.2f}"


def cut(text: str | None, n: int = 48) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1] + "…"


def fmt_age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}с"
    if seconds < 5400:
        return f"{seconds // 60}м"
    if seconds < 172800:
        return f"{seconds / 3600:.1f}ч"
    return f"{seconds / 86400:.1f}д"


def fmt_ts(ts: float | None) -> str:
    if not ts:
        return "—"
    return time.strftime("%d.%m %H:%M", time.gmtime(ts)) + " UTC"


# --- Цена и тик -------------------------------------------------------------

def floor_tick(price: float, tick: float) -> float:
    if tick <= 0:
        tick = 0.01
    steps = math.floor(price / tick + 1e-6)
    return round(steps * tick, 6)


def ceil_tick(price: float, tick: float) -> float:
    if tick <= 0:
        tick = 0.01
    steps = math.ceil(price / tick - 1e-6)
    return round(steps * tick, 6)


def clamp_price(price: float, tick: float) -> float:
    """Цена ордера должна лежать в [tick, 1 − tick]."""
    return round(min(max(price, tick), 1.0 - tick), 6)


# --- Комиссии Polymarket ------------------------------------------------------
# fee = shares × rate × (p × (1 − p))^exponent, платит только тейкер
# (docs.polymarket.com/trading/fees). Ставки по категориям — на случай,
# если у рынка не пришёл feeSchedule.

CATEGORY_FEE_RATES = {
    "crypto": 0.07, "sports": 0.05, "finance": 0.04, "politics": 0.04,
    "economics": 0.05, "culture": 0.05, "weather": 0.05, "mentions": 0.04,
    "tech": 0.04, "geopolitics": 0.0,
}
DEFAULT_FEE_RATE = 0.05


def taker_fee(shares: float, price: float, rate: float, exponent: float = 1.0) -> float:
    if shares <= 0 or rate <= 0 or not (0 < price < 1):
        return 0.0
    fee = shares * rate * (price * (1.0 - price)) ** exponent
    return 0.0 if fee < 0.0001 else fee
