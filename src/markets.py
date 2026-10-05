"""
Кэш метаданных рынков по token_id: вопрос, исход, время окончания, тик,
комиссия, neg-risk, старт матча. Нужен фильтрам копирования («быстрый
рынок», «до конца < N минут», «матч уже идёт») и для расчёта комиссии.

Источник — Gamma (/markets?clob_token_ids=…). Один запрос отдаёт сразу оба
токена рынка, кэшируем оба. Параллельные запросы по одному токену
склеиваются в один (single-flight) — при всплеске сделок не долбим API.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime

from src import http
from src.util import CATEGORY_FEE_RATES, DEFAULT_FEE_RATE, fnum, loads

log = logging.getLogger("markets")


@dataclass
class TokenMeta:
    token_id: str
    condition_id: str | None = None
    slug: str | None = None
    title: str | None = None
    outcome: str | None = None
    outcome_index: int | None = None
    event_slug: str | None = None
    end_ts: float | None = None
    neg_risk: bool | None = None
    tick: float = 0.01
    min_order_size: float = 0.0
    fee_rate: float = DEFAULT_FEE_RATE
    fee_exp: float = 1.0
    game_start_ts: float | None = None
    seconds_delay: float = 0.0
    closed: bool = False
    accepting: bool = True
    other_token: str | None = None
    category: str | None = None
    tags: tuple = ()
    fetched: float = 0.0
    complete: bool = False   # True — пришло из Gamma; False — собрано по подсказкам

    def minutes_left(self, now: float | None = None) -> float | None:
        if not self.end_ts:
            return None
        return (self.end_ts - (now or time.time())) / 60.0


_cache: dict[str, TokenMeta] = {}
_inflight: dict[str, asyncio.Task] = {}
TTL_SEC = 600  # статус рынка (closed/accepting) иногда меняется — обновляем


def parse_iso(value) -> float | None:
    if not value:
        return None
    try:
        s = str(value).strip().replace("Z", "+00:00")
        if len(s) == 10:  # только дата
            s += "T00:00:00+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def _seq(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            out = loads(value)
            return out if isinstance(out, list) else []
        except ValueError:
            return []
    return []


def _category(m: dict) -> str | None:
    cat = (m.get("category") or "").strip().lower() or None
    tags = [str(t.get("slug") or t.get("label") or "").lower() for t in (m.get("tags") or []) if isinstance(t, dict)]
    for name in CATEGORY_FEE_RATES:
        if cat == name or name in tags:
            return name
    if m.get("gameStartTime") or m.get("sportsMarketType"):
        return "sports"
    return cat


def metas_from_market(m: dict) -> list[TokenMeta]:
    tokens = [str(t) for t in _seq(m.get("clobTokenIds"))]
    outcomes = [str(o) for o in _seq(m.get("outcomes"))]
    events = m.get("events") or []
    event_slug = events[0].get("slug") if events and isinstance(events[0], dict) else None
    fee = m.get("feeSchedule") if isinstance(m.get("feeSchedule"), dict) else {}
    category = _category(m)
    if m.get("feesEnabled") is False:
        rate = 0.0
    elif fee.get("rate") not in (None, ""):
        rate = fnum(fee.get("rate"), DEFAULT_FEE_RATE)
    else:
        rate = CATEGORY_FEE_RATES.get(category or "", DEFAULT_FEE_RATE)
    tags = tuple(str(t.get("slug") or "").lower() for t in (m.get("tags") or []) if isinstance(t, dict))
    out = []
    for i, tok in enumerate(tokens):
        out.append(TokenMeta(
            token_id=tok,
            condition_id=(m.get("conditionId") or None),
            slug=m.get("slug"),
            title=m.get("question") or m.get("title"),
            outcome=outcomes[i] if i < len(outcomes) else None,
            outcome_index=i,
            event_slug=event_slug,
            end_ts=parse_iso(m.get("endDate") or m.get("endDateIso")),
            neg_risk=bool(m.get("negRisk")) if m.get("negRisk") is not None else None,
            tick=fnum(m.get("orderPriceMinTickSize"), 0.01) or 0.01,
            min_order_size=fnum(m.get("orderMinSize"), 0.0),
            fee_rate=rate,
            fee_exp=fnum(fee.get("exponent"), 1.0) or 1.0,
            game_start_ts=parse_iso(m.get("gameStartTime")),
            seconds_delay=fnum(m.get("secondsDelay"), 0.0),
            closed=bool(m.get("closed")),
            accepting=m.get("acceptingOrders") is not False,
            other_token=tokens[1 - i] if len(tokens) == 2 else None,
            category=category,
            tags=tags,
            fetched=time.time(),
            complete=True,
        ))
    return out


def _markets_list(payload) -> list[dict]:
    if isinstance(payload, list):
        return [m for m in payload if isinstance(m, dict)]
    if isinstance(payload, dict):
        for key in ("markets", "data"):
            if isinstance(payload.get(key), list):
                return [m for m in payload[key] if isinstance(m, dict)]
    return []


async def _fetch_by_token(token_id: str) -> TokenMeta | None:
    payload = await http.gamma_get("/markets", {"clob_token_ids": token_id, "include_tag": "true"},
                                   timeout=4.0)
    found = None
    for m in _markets_list(payload):
        for meta in metas_from_market(m):
            _cache[meta.token_id] = meta
            if meta.token_id == token_id:
                found = meta
    return found


def get(token_id: str) -> TokenMeta | None:
    return _cache.get(str(token_id))


def put_hint(token_id: str, hint: dict | None) -> TokenMeta:
    """Сохраняем то, что знаем о рынке из самой сделки (RTDS/Data API
    присылают slug/вопрос/исход) — пока Gamma не ответила, этого хватает
    для уведомления и части фильтров."""
    token_id = str(token_id)
    meta = _cache.get(token_id)
    if meta is None:
        meta = TokenMeta(token_id=token_id)
        _cache[token_id] = meta
    if hint and not meta.complete:
        meta.condition_id = meta.condition_id or hint.get("condition_id")
        meta.slug = meta.slug or hint.get("slug")
        meta.title = meta.title or hint.get("title")
        meta.outcome = meta.outcome or hint.get("outcome")
        meta.event_slug = meta.event_slug or hint.get("event_slug")
    return meta


def _start_fetch(token_id: str) -> asyncio.Task:
    task = _inflight.get(token_id)
    if task is None or task.done():
        task = asyncio.create_task(_fetch_by_token(token_id))
        _inflight[token_id] = task
        task.add_done_callback(_fetch_done(token_id))
    return task


def _fetch_done(token_id: str):
    def cb(t: asyncio.Task) -> None:
        _inflight.pop(token_id, None)
        if not t.cancelled() and t.exception() is not None:
            log.debug("Gamma по токену %s: %s", token_id[:12], t.exception())
    return cb


async def ensure(token_id: str, hint: dict | None = None, timeout: float = 2.0) -> TokenMeta:
    """Метаданные токена: из кэша, иначе из Gamma (не дольше timeout).
    Никогда не бросает исключение — в худшем случае вернёт то, что есть."""
    token_id = str(token_id)
    meta = _cache.get(token_id)
    if meta and meta.complete:
        if time.time() - meta.fetched >= TTL_SEC:
            _start_fetch(token_id)   # обновим в фоне, а сейчас отдаём кэш — без задержки
        return meta
    put_hint(token_id, hint)
    task = _start_fetch(token_id)
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except asyncio.TimeoutError:
        log.debug("Gamma не ответила за %.1fs по токену %s", timeout, token_id[:12])
    except Exception as exc:  # noqa: BLE001
        log.debug("Не удалось получить рынок по токену %s: %s", token_id[:12], exc)
    return _cache.get(token_id) or put_hint(token_id, hint)


async def refresh_conditions(condition_ids: list[str]) -> dict[str, list[dict]]:
    """Свежие данные рынков (для проверки резолюции) — пачками по 20.
    Возвращает condition_id -> сырой market dict."""
    out: dict[str, dict] = {}
    ids = [c for c in dict.fromkeys(condition_ids) if c]
    for closed_flag in (None, "true"):
        # Второй проход — с closed=true для тех, кого не вернули: часть
        # выборок Gamma по умолчанию прячет закрытые рынки.
        todo = [c for c in ids if c not in out]
        for i in range(0, len(todo), 20):
            chunk = todo[i:i + 20]
            params = {"condition_ids": chunk, "include_tag": "true", "limit": len(chunk) * 2}
            if closed_flag:
                params["closed"] = closed_flag
            try:
                payload = await http.gamma_get("/markets", params, timeout=8.0)
            except Exception as exc:  # noqa: BLE001
                log.warning("Gamma: не удалось обновить рынки (%s)", exc)
                continue
            for m in _markets_list(payload):
                cid = m.get("conditionId")
                if cid:
                    out[cid] = m
                    for meta in metas_from_market(m):
                        _cache[meta.token_id] = meta
        if len(out) >= len(ids):
            break
    return out


def resolution_payouts(m: dict) -> dict[str, float] | None:
    """token_id -> выплата за акцию, если рынок окончательно разрешён.
    Пока результат не финальный (оспаривается и т.п.) — None."""
    if not m.get("closed"):
        return None
    status = str(m.get("umaResolutionStatus") or "").lower()
    if status and status not in ("resolved", "settled"):
        return None
    tokens = [str(t) for t in _seq(m.get("clobTokenIds"))]
    prices = [fnum(p, -1) for p in _seq(m.get("outcomePrices"))]
    if len(tokens) != len(prices) or not tokens:
        return None
    # Финальные цены — 1/0 (или 0.5/0.5 при «ничьей»). Промежуточные —
    # значит, рынок закрыт для торговли, но ещё не разрешён.
    if not all(p in (0.0, 0.5, 1.0) for p in prices):
        return None
    if abs(sum(prices) - 1.0) > 1e-6:
        return None
    return dict(zip(tokens, prices))


# --- Классификация «быстрых» рынков -------------------------------------------

_FAST_SLUG = re.compile(
    r"(updown-(5m|15m|30m|1h|4h)-|up-or-down-.*-\d{1,2}(am|pm)-et|-above-on-.*-\d{1,2}(am|pm)-et|"
    r"-(5|15)-?min|price-.*-\d{1,2}(am|pm)-et)")


def is_fast_slug(slug: str | None) -> bool:
    return bool(slug and _FAST_SLUG.search(slug.lower()))


def fast_reason(meta: TokenMeta, min_minutes_left: float, now: float | None = None) -> str | None:
    """Причина считать рынок «быстрым» (не успеем скопировать), либо None."""
    if is_fast_slug(meta.slug) or is_fast_slug(meta.event_slug):
        return "короткий крипто-рынок (5м/15м/час)"
    left = meta.minutes_left(now)
    if left is not None and min_minutes_left > 0 and left < min_minutes_left:
        return f"до конца рынка {max(left, 0):.0f} мин (< {min_minutes_left:g})"
    return None


def in_play(meta: TokenMeta, now: float | None = None) -> bool:
    now = now or time.time()
    return bool(meta.game_start_ts and now >= meta.game_start_ts and not meta.closed)
