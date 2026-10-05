"""
Общие HTTP-клиенты (keep-alive: TLS-рукопожатие один раз, а не на каждый
запрос — это 50-150 мс экономии на каждом обращении) + ограничитель
частоты запросов к Data API. При 429 сервер присылает Retry-After —
выдерживаем паузу на ограничителе, чтобы не получить бан.
"""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

from config import settings
from src.util import loads

log = logging.getLogger("http")

_clients: dict[str, httpx.AsyncClient] = {}
# Для тестов: подменяем транспорт (httpx.MockTransport).
_transport_override = None


class ApiError(Exception):
    def __init__(self, status: int, text: str, url: str = ""):
        super().__init__(f"HTTP {status} {url}: {text[:200]}")
        self.status = status
        self.text = text
        self.url = url


class RateLimiter:
    """Токен-бакет: не больше rps запросов в секунду, всплеск до burst."""

    def __init__(self, rps: float, burst: float | None = None):
        self.rps = max(0.1, rps)
        self.capacity = burst if burst is not None else max(1.0, rps)
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.blocked_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                if now < self.blocked_until:
                    await asyncio.sleep(self.blocked_until - now)
                    continue
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rps)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rps)

    def penalize(self, seconds: float) -> None:
        self.blocked_until = max(self.blocked_until, time.monotonic() + seconds)
        self.tokens = 0


# Опрос сделок (горячий путь) и поиск кошельков — разные «кошельки» лимита,
# чтобы долгий поиск не задерживал обнаружение сделок копируемых.
poll_limiter = RateLimiter(settings.DATA_API_MAX_RPS, burst=settings.DATA_API_MAX_RPS)
discovery_limiter = RateLimiter(settings.DISCOVERY_RPS, burst=2)


def client(base_url: str) -> httpx.AsyncClient:
    c = _clients.get(base_url)
    if c is None or c.is_closed:
        kwargs = dict(base_url=base_url, timeout=httpx.Timeout(10.0, connect=5.0),
                      limits=httpx.Limits(max_connections=20, max_keepalive_connections=10,
                                          keepalive_expiry=120),
                      headers={"User-Agent": "pm-copy-bot/1.0", "Accept": "application/json"})
        if _transport_override is not None:
            kwargs["transport"] = _transport_override
        c = httpx.AsyncClient(**kwargs)
        _clients[base_url] = c
    return c


async def close_all() -> None:
    for c in list(_clients.values()):
        try:
            await c.aclose()
        except Exception:  # noqa: BLE001
            pass
    _clients.clear()


def _retry_after(resp: httpx.Response, default: float) -> float:
    try:
        return max(0.5, min(120.0, float(resp.headers.get("Retry-After", default))))
    except (TypeError, ValueError):
        return default


async def request_json(base_url: str, method: str, path: str, *, params: dict | None = None,
                       json_body=None, limiter: RateLimiter | None = None, retries: int = 2,
                       timeout: float | None = None):
    """GET/POST и JSON-ответ. 429/503/сетевые ошибки — повтор с паузой;
    остальные 4xx — ApiError сразу (повтор бессмысленен)."""
    attempt = 0
    while True:
        if limiter is not None:
            await limiter.acquire()
        try:
            req_kwargs = {"params": params}
            if json_body is not None:
                req_kwargs["json"] = json_body
            if timeout is not None:
                req_kwargs["timeout"] = timeout
            resp = await client(base_url).request(method, path, **req_kwargs)
        except (httpx.TransportError, asyncio.TimeoutError) as exc:
            if attempt >= retries:
                raise ApiError(0, f"{type(exc).__name__}: {exc}", path) from exc
            attempt += 1
            await asyncio.sleep(0.3 * attempt)
            continue
        if resp.status_code in (429, 503, 502, 504):
            wait = _retry_after(resp, 2.0 * (attempt + 1))
            if attempt >= retries:
                if limiter is not None and resp.status_code == 429:
                    limiter.penalize(wait)
                raise ApiError(resp.status_code, resp.text, path)
            attempt += 1
            log.debug("HTTP %s на %s — жду %.1fs", resp.status_code, path, wait)
            if limiter is not None and resp.status_code == 429:
                limiter.penalize(wait)   # следующий acquire() сам выждет паузу
            else:
                await asyncio.sleep(wait)
            continue
        if resp.status_code >= 400:
            raise ApiError(resp.status_code, resp.text, path)
        if not resp.content:
            return None
        return loads(resp.content)


async def data_get(path: str, params: dict | None = None, *, limiter: RateLimiter | None = None,
                   retries: int = 2, timeout: float | None = None):
    return await request_json(settings.DATA_API_HOST, "GET", path, params=params,
                              limiter=limiter, retries=retries, timeout=timeout)


async def gamma_get(path: str, params: dict | None = None, *, timeout: float | None = None, retries: int = 1):
    return await request_json(settings.GAMMA_HOST, "GET", path, params=params,
                              retries=retries, timeout=timeout)


async def clob_get(path: str, params: dict | None = None, *, timeout: float | None = None, retries: int = 1):
    return await request_json(settings.CLOB_HOST, "GET", path, params=params,
                              retries=retries, timeout=timeout)


async def clob_post(path: str, body, *, timeout: float | None = None, retries: int = 1):
    return await request_json(settings.CLOB_HOST, "POST", path, json_body=body,
                              retries=retries, timeout=timeout)


def data_items(payload) -> list:
    """Data API v2 оборачивает ответ в {"data": [...], "pagination": {...}};
    на всякий случай понимаем и голый список (как в v1)."""
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []
    if isinstance(payload, list):
        return payload
    return []


def next_cursor(payload) -> str | None:
    if isinstance(payload, dict):
        pag = payload.get("pagination") or {}
        if isinstance(pag, dict) and pag.get("has_more", True):
            return pag.get("next_cursor") or None
    return None
