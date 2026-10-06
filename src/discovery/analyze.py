"""
Метрики кошелька для отбора в копирование. Чистые функции над сырыми
ответами Data API v2 — тестируются без сети.

Главная идея: на рынке предсказаний безубыточный винрейт = цена входа.
Кто покупает по 0.90 и выигрывает 92% — почти не зарабатывает и рискует
всем на редком проигрыше; кто покупает по 0.40 и выигрывает 55% — сильный.
Поэтому основа оценки — не винрейт и не сумма PnL, а «edge»: насколько
реальный результат каждой ставки лучше цены, по которой она куплена, и
насколько это статистически значимо (z-score). Остальное — копируемость:
частота решений (бот или человек), время удержания, быстрые рынки,
маркет-мейкинг, зависимость от одной удачной ставки, проскальзывание
после их входа, стабильность по неделям.

Формат Data API v2 (проверено на живых ответах, октябрь 2026):
  * позиция: total_size — куплено акций за всё время, avg_price — средняя
    цена входа, entry_cost_usdc — стоимость ТЕКУЩЕГО остатка (у закрытой
    позиции 0!), realized_pnl / total_pnl, percent_realized_pnl =
    realized_pnl / (total_size × avg_price) × 100, first_entry_at,
    last_event_at, end_date, redeemable;
  * статус OPEN включает и уже разрешённые, но не погашенные позиции
    (redeemable=true); REDEEMABLE — разрешённые и не погашенные, в том
    числе проигранные (REDEEMABLE_LOST — их подмножество);
  * /v2/user-pnl и all_time_pnl в /v2/user-stats — накопленные значения:
    position_pnl (торговый, после комиссий), realized_pnl, unrealized_pnl,
    economic_pnl (= position_pnl + wallet_income), wallet_income (ребейты,
    награды, доходность, рефералка), fees_refunded, volume_usdc, trade_count.
"""
from __future__ import annotations

import math
import re
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone

from src.util import fnum

FAST_RE = re.compile(
    r"(updown-(5m|15m|30m|1h|4h)-|up-or-down-.*-\d{1,2}(am|pm)-et|-above-on-.*-\d{1,2}(am|pm)-et|"
    r"-(5|15)-?min|price-.*-\d{1,2}(am|pm)-et)")

CATEGORY_RULES = [
    ("crypto", re.compile(r"bitcoin|btc|ethereum|eth-|solana|sol-|xrp|crypto|doge|bnb|hype|updown|up-or-down|"
                          r"-above-|microstrategy|coinbase|altcoin|memecoin|pump")),
    ("sports", re.compile(r"nfl|nba|mlb|nhl|wnba|epl|premier-league|champions-league|la-liga|serie-a|bundesliga|"
                          r"ligue-1|mls|ufc|boxing|tennis|atp|wta|f1|formula-1|nascar|golf|pga|cricket|ipl|ncaa|"
                          r"-vs-|super-bowl|world-cup|euro-20|copa|olympic|match|game-\d|esports|cs2|dota|lol-|valorant|"
                          r"fifwc|fifa")),
    ("politics", re.compile(r"election|president|presidential|senate|house-|congress|governor|mayor|trump|biden|"
                            r"harris|vance|newsom|democrat|republican|gop|primary|nominee|parliament|prime-minister|"
                            r"vote|poll|cabinet|impeach|supreme-court|government-shutdown|lula|bolsonaro")),
    ("geopolitics", re.compile(r"war|ukraine|russia|israel|gaza|iran|china-|taiwan|nato|ceasefire|invasion|strike|"
                               r"missile|sanction|putin|zelensk|netanyahu|hamas|hezbollah|invade")),
    ("economics", re.compile(r"fed-|fomc|rate-cut|interest-rate|inflation|cpi|gdp|recession|unemployment|jobs-report|"
                             r"payroll|tariff|treasury|powell")),
    ("tech", re.compile(r"gpt|openai|anthropic|claude|gemini|grok|deepseek|mythos|llm|chatgpt|sora|kling|"
                        r"text-to-video|best-ai|ai-model|agi-|xai-|mistral|astra-model|astra-released")),
    ("finance", re.compile(r"stock|nasdaq|s-p-500|sp500|dow-|tesla|nvidia|apple|google|amazon|microsoft|meta-|"
                           r"ipo|earnings|market-cap|gold|oil|silver|wti")),
    ("culture", re.compile(r"oscar|grammy|emmy|movie|box-office|album|song|spotify|taylor-swift|celebrity|tiktok|"
                           r"youtube|mrbeast|twitter|tweet|elon|musk|mentions|say-|said")),
    ("weather", re.compile(r"temperature|weather|hurricane|snow|rain|heat|climate")),
]

# Исполнения одного токена и стороны ближе этого (сек) — одно решение
# человека: лимитку, которую «съели» 30 разных тейкеров, activity отдаёт
# 30 строками с разными транзакциями.
DECISION_GAP_SEC = 600
# Позиции дешевле — пыль (остатки, тестовые покупки), в статистику ставок не идут.
MIN_BET_USDC = 1.0
# Данные PnL у Polymarket иногда отстают; старше — помечаем.
PNL_STALE_DAYS = 3.0

_INCOME_FIELDS = ("maker_rebate", "taker_rebate", "reward_income", "yield_income", "referral_income")


# Спортивные и киберспортивные рынки Polymarket начинаются с кода лиги:
# nfl-…, lol-…, val-…, dota2-…, epl-…, lal-…, itf-…, cwbb-… и т.п.
SPORTS_PREFIX_RE = re.compile(
    r"^(nfl|nba|mlb|nhl|wnba|cfb|cbb|cwbb|ncaab?|epl|lal|bun|sea|fl1|ere|por|elc|ucl|uel|uecl|mls|bra|arg|"
    r"col|mex|tur|swe|nor|den|sco|bel|aut|sui|grc|jpn|kor|chn|aus|rus|ukr|pol|cze|atp|wta|itf|ufc|box|pga|"
    r"f1|nascar|ipl|cric|cs2|csgo|lol|val|dota2|codmw|cod|lec|lck|lpl|r6|ow|rl|fif|fifwc|kbo|npb|bk[a-z]*|"
    r"euroleague|ncaaf|afl|nrl|rugby|tennis|golf|darts|snooker|mma|pfl|bellator)\d*-")


def categorize(slug: str | None, event_slug: str | None = None) -> str:
    for part in (slug, event_slug):
        if part and SPORTS_PREFIX_RE.match(str(part).lower()):
            return "sports"
    text = f"{slug or ''} {event_slug or ''}".lower()
    for name, rx in CATEGORY_RULES:
        if rx.search(text):
            return name
    return "other"


def is_fast(slug: str | None, event_slug: str | None = None) -> bool:
    return bool(FAST_RE.search(f"{slug or ''} {event_slug or ''}".lower()))


def _ts(v) -> float:
    t = fnum(v)
    if t > 1e14:     # микросекунды
        return t / 1e6
    if t > 1e12:     # миллисекунды
        return t / 1e3
    return t


def _date_ts(v) -> float | None:
    """Эпоха в секундах из числа или ISO-строки ('2026-08-01',
    '2026-08-01T12:00:00Z'). '1970-01-01' и прочий мусор → None."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) or str(v).strip().replace(".", "", 1).isdigit():
        t = _ts(v)
        return t if t > 1e9 else None
    try:
        s = str(v).strip().replace("Z", "+00:00")
        if len(s) == 10:
            s += "T00:00:00+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        t = dt.timestamp()
        return t if t > 1e9 else None
    except (ValueError, TypeError):
        return None


def _truthy(v) -> bool:
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes")
    return bool(v)


def _median(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def _pct(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    k = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
    return s[k]


# ------------------------------------------------- профиль (user-stats) ----

def point_trading_pnl(p: dict | None) -> float | None:
    """Торговый PnL точки (накопленный): position_pnl = реализованный +
    нереализованный по рынкам, после комиссий. Ребейты, награды, рефералка
    и доходность сюда не входят — это не навык трейдера."""
    if not isinstance(p, dict):
        return None
    if p.get("position_pnl") is not None:
        return fnum(p.get("position_pnl"))
    if p.get("realized_pnl") is not None:
        return fnum(p.get("realized_pnl")) + fnum(p.get("unrealized_pnl"))
    for key in ("total_pnl", "cumulative_pnl", "pnl"):   # прежние форматы
        if p.get(key) is not None:
            return fnum(p.get(key))
    return None


def point_income(p: dict | None) -> float:
    """Доход не от сделок: ребейты мейкера/тейкера, награды за ликвидность,
    доходность, рефералка."""
    if not isinstance(p, dict):
        return 0.0
    if p.get("wallet_income") is not None:
        return fnum(p.get("wallet_income"))
    return sum(fnum(p.get(k)) for k in _INCOME_FIELDS)


def stats_summary(stats: dict | None) -> dict:
    """/v2/user-stats → плоские числа. all_time_pnl там — объект (как точка
    /v2/user-pnl), а не число; trades — число рынков, где торговал."""
    st = stats if isinstance(stats, dict) else {}
    atp = st.get("all_time_pnl")
    out = {"trading_pnl": None, "economic_pnl": None, "wallet_income": None, "fees_refunded": None,
           "trade_count": None, "volume_usdc": None, "stats_as_of": None,
           "markets_traded": st.get("trades"),
           "biggest_win": fnum(st.get("biggest_win")) if st.get("biggest_win") is not None else None,
           "join_ts": _date_ts(st.get("join_date"))}
    if isinstance(atp, dict):
        out["trading_pnl"] = point_trading_pnl(atp)
        if atp.get("economic_pnl") is not None:
            out["economic_pnl"] = fnum(atp.get("economic_pnl"))
        out["wallet_income"] = point_income(atp)
        out["fees_refunded"] = fnum(atp.get("fees_refunded"))
        out["trade_count"] = atp.get("trade_count")
        if atp.get("volume_usdc") is not None:
            out["volume_usdc"] = fnum(atp.get("volume_usdc"))
        out["stats_as_of"] = _ts(atp.get("timestamp")) or None
    elif atp is not None:
        out["trading_pnl"] = fnum(atp)
    return out


# ------------------------------------------------------- позиции (edge) ----

def position_cost(p: dict) -> float:
    """Сколько вложено в позицию за всё время.

    В v2 entry_cost_usdc — стоимость текущего остатка (у закрытой позиции
    0), поэтому основа — total_size (куплено акций всего) × avg_price. Для
    закрытых точнее realized_pnl / percent_realized_pnl: avg_price в ответе
    обрезан до 4 знаков и на дешёвых входах (0.0034) врёт на проценты."""
    avg = fnum(p.get("avg_price"))
    total = fnum(p.get("total_size"))
    base = total * avg if total > 0 and avg > 0 else 0.0
    rp = fnum(p.get("realized_pnl"))
    prp = fnum(p.get("percent_realized_pnl"))
    closed = p.get("current_size") is not None and fnum(p.get("current_size")) <= 0
    if closed and abs(rp) > 1e-9 and abs(prp) > 1e-9:
        alt = rp / (prp / 100.0)
        if alt > 0 and (base <= 0 or 0.5 <= alt / base <= 2.0):
            return alt
    if base > 0:
        return base
    for key in ("initial_value", "initialValue"):          # прежние форматы
        if fnum(p.get(key)) > 0:
            return fnum(p.get(key))
    bought = fnum(p.get("total_bought") or p.get("totalBought"))
    if bought > 0 and avg > 0:
        return bought * avg
    return fnum(p.get("entry_cost_usdc"))


def position_pnl(p: dict) -> float:
    if p.get("total_pnl") is not None:
        return fnum(p.get("total_pnl"))
    return fnum(p.get("realized_pnl")) + fnum(p.get("unrealized_pnl"))


def entry_ts(p: dict) -> float:
    return _ts(p.get("first_entry_at")) or _ts(p.get("last_event_at") or p.get("timestamp"))


def end_ts(v) -> float | None:
    """Конец рынка. end_date в позициях — дата без времени ('2026-10-05'):
    берём конец этих суток, иначе матч, который начался днём, «закончится»
    в полночь до своего начала и удержание выйдет отрицательным."""
    t = _date_ts(v)
    if t and isinstance(v, str) and len(v.strip()) == 10:
        t += 86400
    return t


def settle_ts(p: dict) -> float:
    """Когда позиция стала окончательной: продажа (last_event_at) или конец
    рынка (end_date) — что раньше. Погашение выигрыша бывает через недели
    после конца рынка, поэтому одно last_event_at завышает «свежесть»."""
    last = _ts(p.get("last_event_at") or p.get("timestamp"))
    end = end_ts(p.get("end_date"))
    cands = [t for t in (last, end) if t]
    return min(cands) if cands else 0.0


def resolved_positions(closed: list[dict], redeemable: list[dict]) -> list[dict]:
    """Закрытые + разрешённые, но не погашённые (иначе проигрыши, которые
    человек не стал «забирать», выпадают из статистики и она выглядит
    лучше, чем есть). Дубли по token_id убираем."""
    out: dict[str, dict] = {}
    for p in list(closed) + list(redeemable):
        key = str(p.get("token_id") or p.get("asset") or "") or str(id(p))
        if key not in out:
            out[key] = p
    return list(out.values())


def sample_window(closed: list[dict], redeemable: list[dict], caps: dict | None) -> float | None:
    """Начало окна, за которое выборка позиций полная.

    Закрытые и непогашенные приходят отсортированными по последнему
    событию и обрезаются лимитом. Если просто сложить «последние 500
    закрытых» (это в основном погашенные выигрыши за пару недель) и
    «последние 300 непогашенных» (это проигрыши, копящиеся годами), edge
    исказится. Берём только позиции, открытые после начала самого
    короткого из обрезанных списков: каждая такая позиция гарантированно
    попала в свой список."""
    caps = caps or {}
    starts = []
    for rows, cap in ((closed, caps.get("closed")), (redeemable, caps.get("redeemable"))):
        if cap and len(rows) >= cap:
            ts = [_ts(p.get("last_event_at")) for p in rows if _ts(p.get("last_event_at"))]
            if ts:
                starts.append(min(ts))
    return max(starts) if starts else None


def edge_metrics(resolved: list[dict], now: float | None = None, window_start: float | None = None) -> dict:
    now = now or time.time()
    bets = []
    for p in resolved:
        if window_start and entry_ts(p) < window_start:
            continue
        cost = position_cost(p)
        price = fnum(p.get("avg_price"))
        if cost < MIN_BET_USDC or not (0.0 < price < 1.0):
            continue
        pnl = position_pnl(p)
        y = min(1.0, max(0.0, price * (1.0 + pnl / cost)))
        bets.append({"cost": cost, "p": price, "pnl": pnl, "y": y, "ts": settle_ts(p), "t0": entry_ts(p),
                     "slug": p.get("slug"), "event_slug": p.get("event_slug")})
    n = len(bets)
    m: dict = {"n_resolved": n}
    if n == 0:
        return m
    starts = [b["t0"] for b in bets if b["t0"]]
    # Сколько дней покрывают разобранные ставки (от самого раннего входа).
    m["window_days"] = (now - min(starts)) / 86400 if starts else None
    cost_total = sum(b["cost"] for b in bets)
    pnl_total = sum(b["pnl"] for b in bets)
    wins = [b for b in bets if b["pnl"] > 0]
    gains = sorted((b["pnl"] for b in wins), reverse=True)
    losses = [b["pnl"] for b in bets if b["pnl"] < 0]
    var_bets = sum(b["p"] * (1 - b["p"]) for b in bets)
    var_usd = sum(b["cost"] ** 2 * (1 - b["p"]) / b["p"] for b in bets)
    m.update({
        "cost_resolved": cost_total,
        "pnl_resolved": pnl_total,
        "roi_resolved": pnl_total / cost_total if cost_total > 0 else None,
        "win_rate": len(wins) / n,
        "avg_entry": sum(b["p"] * b["cost"] for b in bets) / cost_total if cost_total else None,
        "edge_cents": 100.0 * sum(b["y"] - b["p"] for b in bets) / n,
        "z_bets": sum(b["y"] - b["p"] for b in bets) / math.sqrt(var_bets) if var_bets > 0 else None,
        "z_usd": pnl_total / math.sqrt(var_usd) if var_usd > 0 else None,
        "profit_factor": (sum(gains) / -sum(losses)) if losses else (math.inf if gains else None),
        "top1_share": gains[0] / sum(gains) if gains else None,
        "top3_share": sum(gains[:3]) / sum(gains) if gains else None,
        "pnl_ex_top3": pnl_total - sum(gains[:3]),
        "median_bet": _median([b["cost"] for b in bets]),
        "fav_share": sum(b["cost"] for b in bets if b["p"] >= 0.9) / cost_total if cost_total else None,
        "longshot_share": sum(b["cost"] for b in bets if b["p"] <= 0.1) / cost_total if cost_total else None,
        "fast_share_pos": sum(b["cost"] for b in bets if is_fast(b["slug"], b["event_slug"])) / cost_total
        if cost_total else None,
    })
    recent = [b for b in bets if b["ts"] and now - b["ts"] <= 30 * 86400]
    rc = sum(b["cost"] for b in recent)
    m["n_resolved_30d"] = len(recent)
    m["roi_resolved_30d"] = sum(b["pnl"] for b in recent) / rc if rc > 0 else None
    # Калибровка по корзинам цены: где у человека реальное преимущество.
    buckets = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 0.9), (0.9, 1.0)]
    parts = []
    for lo, hi in buckets:
        bb = [b for b in bets if lo <= b["p"] < hi]
        if bb:
            e = 100.0 * sum(b["y"] - b["p"] for b in bb) / len(bb)
            c = sum(b["cost"] for b in bb)
            roi = sum(b["pnl"] for b in bb) / c * 100 if c else 0.0
            parts.append(f"{int(lo * 100)}-{int(hi * 100)}:{e:+.1f}c/roi{roi:+.0f}%/n{len(bb)}")
    m["edge_by_price"] = " | ".join(parts)
    cats: dict[str, list] = defaultdict(lambda: [0.0, 0.0, 0])
    for b in bets:
        c = cats[categorize(b["slug"], b["event_slug"])]
        c[0] += b["cost"]
        c[1] += b["pnl"]
        c[2] += 1
    m["categories"] = " | ".join(
        f"{k}:{v[0] / cost_total * 100:.0f}% roi{(v[1] / v[0] * 100 if v[0] else 0):+.0f}% n{v[2]}"
        for k, v in sorted(cats.items(), key=lambda kv: -kv[1][0]))
    m["main_category"] = max(cats.items(), key=lambda kv: kv[1][0])[0] if cats else None
    return m


LIVE_SWING_CENTS = 0.10     # покупки одного исхода с разбросом цены ≥10¢ ...
LIVE_SWING_SEC = 3 * 3600   # ... за ≤3 часа — так двигается цена только по ходу матча
LIVE_LEAD_SEC = 2 * 3600    # купил выигравший исход меньше чем за 2 ч до погашения


def live_metrics(trades: list[dict], closed: list[dict]) -> dict:
    """Похоже ли, что человек ставит по ходу матча (лайв). Время начала
    матча в Data API нет, поэтому два косвенных признака по спортивным
    покупкам:
      * «качели» — один исход покупался по ценам с разбросом ≥10¢ в пределах
        3 часов: до матча линия так не ходит, а по ходу игры — постоянно;
      * «поздний вход» — выигравший исход куплен меньше чем за 2 часа до
        погашения (матч + резолюция дольше, значит вход уже в игре).
    Такие сделки копией не догнать: пока мы видим их ордер, цена уже другая,
    а фильтр «лайв» в копировщике их пропустит."""
    buys = []
    for t in trades or []:
        if str(t.get("side") or "").upper() != "BUY":
            continue
        if categorize(t.get("slug"), t.get("event_slug")) != "sports":
            continue
        usd = fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
        buys.append((str(t.get("token_id") or t.get("asset") or ""), _ts(t.get("timestamp")), fnum(t.get("price")), usd))
    all_usd = sum(fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
                  for t in trades or [] if str(t.get("side") or "").upper() == "BUY")
    sport_usd = sum(b[3] for b in buys)
    m: dict = {"sports_buy_share": sport_usd / all_usd if all_usd > 0 else None}
    if sport_usd <= 0:
        return m
    by_tok: dict[str, list] = defaultdict(list)
    for b in buys:
        by_tok[b[0]].append(b)
    swing_tokens = set()
    for tok, bb in by_tok.items():
        prices = [b[2] for b in bb if b[2] > 0]
        tss = [b[1] for b in bb if b[1]]
        if prices and tss and max(prices) - min(prices) >= LIVE_SWING_CENTS and max(tss) - min(tss) <= LIVE_SWING_SEC:
            swing_tokens.add(tok)
    swing_usd = sum(b[3] for b in buys if b[0] in swing_tokens)
    redeem_at = {str(p.get("token_id") or ""): _ts(p.get("last_event_at")) for p in closed or []
                 if fnum(p.get("current_price")) >= 0.99 and fnum(p.get("total_pnl")) > 0}
    win_buys = [b for b in buys if redeem_at.get(b[0])]
    win_usd = sum(b[3] for b in win_buys)
    late_usd = sum(b[3] for b in win_buys if 0 < redeem_at[b[0]] - b[1] < LIVE_LEAD_SEC)
    m["live_swing_share"] = swing_usd / sport_usd
    m["live_late_share"] = late_usd / win_usd if win_usd > 0 else None
    m["live_share"] = max(m["live_swing_share"], m["live_late_share"] or 0.0)
    return m


def hold_metrics(closed: list[dict]) -> dict:
    """Удержание по закрытым позициям: first_entry_at → продажа или конец
    рынка. Точнее, чем по сделкам: видны сотни позиций, а не 1000 строк."""
    holds, short_cost, total_cost = [], 0.0, 0.0
    for p in closed:
        t0 = _ts(p.get("first_entry_at"))
        t1 = settle_ts(p)
        if not t0 or not t1 or t1 < t0:
            continue
        h = (t1 - t0) / 3600.0
        c = max(position_cost(p), 0.0)
        holds.append(h)
        total_cost += c
        if h < 1.0:
            short_cost += c
    if len(holds) < 10:
        return {}
    return {"median_hold_h": _median(holds),
            "short_hold_share": short_cost / total_cost if total_cost > 0 else None,
            "hold_source": "positions"}


# ------------------------------------------------------ PnL по дням ----

def pnl_series_metrics(points: list[dict], now: float | None = None) -> dict:
    """Окна (7/30/90 дн.) считаются от последней точки: у части кошельков
    Polymarket обновляет PnL с опозданием на дни — иначе «30 дней» тихо
    превратились бы в «21 день»."""
    now = now or time.time()
    pts = []
    for p in points or []:
        ts = _ts(p.get("timestamp"))
        v = point_trading_pnl(p)
        if ts and v is not None:
            pts.append((ts, v, p))
    pts.sort(key=lambda x: x[0])
    m: dict = {"pnl_days": len(pts)}
    if not pts:
        return m
    last_ts, last_v, last_p = pts[-1]
    m["pnl_total"] = last_v
    m["pnl_as_of"] = last_ts
    m["pnl_lag_days"] = max(0.0, (now - last_ts) / 86400.0)
    m["first_pnl_ts"] = pts[0][0]
    m["rebates"] = fnum(last_p.get("maker_rebate")) + fnum(last_p.get("taker_rebate"))
    m["rewards"] = (fnum(last_p.get("reward_income")) + fnum(last_p.get("yield_income")) +
                    fnum(last_p.get("referral_income")))
    m["wallet_income"] = point_income(last_p)
    m["fees_refunded"] = fnum(last_p.get("fees_refunded"))
    m["fees_paid"] = fnum(last_p.get("fees_paid"))
    m["volume_usdc"] = fnum(last_p.get("volume_usdc"))
    m["lp_pnl"] = fnum(last_p.get("realized_lp_pnl"))
    if last_p.get("economic_pnl") is not None:
        m["economic_pnl_series"] = fnum(last_p.get("economic_pnl"))
    if len(pts) < 2:
        return m
    vals = [v for _, v, _ in pts]

    def value_at(age_days: float) -> float:
        target = last_ts - age_days * 86400
        prior = [v for ts, v, _ in pts if ts <= target]
        return prior[-1] if prior else vals[0]

    m["pnl_7d"] = last_v - value_at(7)
    m["pnl_30d"] = last_v - value_at(30)
    m["pnl_90d"] = last_v - value_at(90)
    peak = -math.inf
    max_dd = 0.0
    for v in vals:
        peak = max(peak, v)
        max_dd = max(max_dd, peak - v)
    m["max_dd"] = max_dd
    recent = [(ts, v) for ts, v, _ in pts if ts >= last_ts - 90 * 86400]
    if len(recent) >= 2:
        peak = -math.inf
        dd90 = 0.0
        for _, v in recent:
            peak = max(peak, v)
            dd90 = max(dd90, peak - v)
        m["max_dd_90d"] = dd90
        diffs = [b[1] - a[1] for a, b in zip(recent, recent[1:])]
        if len(diffs) >= 5:
            sd = statistics.pstdev(diffs)
            m["daily_sharpe"] = (statistics.mean(diffs) / sd) if sd > 0 else None
            gains = [d for d in diffs if d > 0]
            m["best_day_share"] = max(gains) / sum(gains) if gains else None
        weeks: dict[int, float] = defaultdict(float)
        for (ts_a, va), (ts_b, vb) in zip(recent, recent[1:]):
            weeks[int(ts_b // (7 * 86400))] += vb - va
        wk = [v for v in weeks.values() if abs(v) > 1e-9]
        m["weeks_90d"] = len(wk)
        m["pos_weeks_share"] = sum(1 for v in wk if v > 0) / len(wk) if wk else None
    return m


def income_share(trading_pnl: float | None, wallet_income: float | None, fees_refunded: float | None,
                 lp_pnl: float | None = None) -> float | None:
    """Доля дохода не от сделок (ребейты, награды, возвраты комиссий, LP).
    Возвраты комиссий уже сидят в position_pnl — вычитаем их, чтобы
    увидеть чистый результат сделок, который можно повторить копией."""
    if trading_pnl is None and wallet_income is None:
        return None
    refunds = max(fnum(fees_refunded), 0.0)
    other = max(fnum(wallet_income), 0.0) + refunds + max(fnum(lp_pnl), 0.0)
    skill = fnum(trading_pnl) - refunds
    return other / max(abs(skill) + other, 1.0)


# ------------------------------------------------------ сделки ----

def decisions(rows: list[dict]) -> list[dict]:
    """Склейка исполнений в решения: один токен и сторона, соседние
    исполнения не дальше DECISION_GAP_SEC. Сделка в обратную сторону по
    тому же токену решение обрывает — иначе скальпер «купил-продал-купил»
    выглядел бы одним спокойным входом."""
    fills = sorted(rows, key=lambda t: _ts(t.get("timestamp")))
    out: list[dict] = []
    open_by_key: dict[tuple, dict] = {}
    for t in fills:
        ts = _ts(t.get("timestamp"))
        side = str(t.get("side") or "").upper()
        token = str(t.get("token_id") or t.get("asset") or "")
        key = (token, side)
        open_by_key.pop((token, "SELL" if side == "BUY" else "BUY"), None)
        usdc = fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
        shares = fnum(t.get("size"))
        d = open_by_key.get(key)
        if d is None or ts - d["t1"] > DECISION_GAP_SEC:
            d = {"t0": ts, "t1": ts, "side": side, "token": key[0], "cond": str(t.get("condition_id") or ""),
                 "oi": t.get("outcome_index"), "usdc": 0.0, "shares": 0.0, "fills": 0,
                 "slug": t.get("slug"), "event_slug": t.get("event_slug")}
            out.append(d)
            open_by_key[key] = d
        d["t1"] = ts
        d["usdc"] += usdc
        d["shares"] += shares
        d["fills"] += 1
    return out


def trade_metrics(trades: list[dict], taker_trades: list[dict] | None, resolved: list[dict],
                  now: float | None = None, taker_cap: int | None = None) -> dict:
    now = now or time.time()
    rows = [t for t in trades or [] if str(t.get("type") or "TRADE").upper() == "TRADE"]
    m: dict = {"n_trades_sample": len(rows)}
    if not rows:
        return m
    ol = decisions(rows)
    ts_list = sorted(d["t0"] for d in ol if d["t0"])
    fill_ts = sorted(_ts(t.get("timestamp")) for t in rows if t.get("timestamp"))
    span_days = max((fill_ts[-1] - fill_ts[0]) / 86400, 1 / 24) if len(fill_ts) >= 2 else None
    days = defaultdict(int)
    for d in ol:
        if d["t0"]:
            days[int(d["t0"] // 86400)] += 1
    per_day = list(days.values())
    gaps = [b - a for a, b in zip(ts_list, ts_list[1:])]
    sizes = [d["usdc"] for d in ol if d["usdc"] > 0]
    buys = [d for d in ol if d["side"] == "BUY"]
    buy_usdc = sum(d["usdc"] for d in buys)
    total_usdc = sum(d["usdc"] for d in ol)
    m.update({
        "n_orders_sample": len(ol),
        "fills_per_order": len(rows) / len(ol) if ol else None,
        "sample_span_days": span_days,
        "orders_per_active_day": _median(per_day),
        "orders_per_day_p90": _pct(per_day, 0.9),
        "active_days_sample": len(per_day),
        "median_gap_min": (_median(gaps) / 60.0) if gaps else None,
        "burst_share": sum(1 for g in gaps if g < 10) / len(gaps) if gaps else None,
        "median_order_usdc": _median(sizes),
        "p90_order_usdc": _pct(sizes, 0.9),
        "buy_share": len(buys) / len(ol),
        "avg_buy_price": (buy_usdc / sum(d["shares"] for d in buys))
        if buys and sum(d["shares"] for d in buys) > 0 else None,
        "fast_share_trades": sum(d["usdc"] for d in ol if is_fast(d["slug"], d["event_slug"])) /
        max(total_usdc, 1e-9),
        "last_trade_age_h": (now - fill_ts[-1]) / 3600 if fill_ts else None,
        "markets_in_sample": len({d["cond"] for d in ol if d["cond"]}),
    })
    if buy_usdc > 0:
        by_event: dict[str, float] = defaultdict(float)
        for d in buys:
            by_event[d["event_slug"] or d["slug"] or d["cond"]] += d["usdc"]
        m["top_event_share"] = max(by_event.values()) / buy_usdc
        mids = [d for d in buys if d["shares"] > 0 and 0.2 <= d["usdc"] / d["shares"] <= 0.8]
        m["mid_price_share"] = sum(d["usdc"] for d in mids) / buy_usdc
        fav = [d for d in buys if d["shares"] > 0 and d["usdc"] / d["shares"] >= 0.9]
        m["fav_buy_share"] = sum(d["usdc"] for d in fav) / buy_usdc
    # Обе стороны одного рынка — признак маркет-мейкера/арбитража.
    sides_by_cond: dict[str, set] = defaultdict(set)
    for d in buys:
        if d["cond"] and d["oi"] is not None:
            sides_by_cond[d["cond"]].add(d["oi"])
    if sides_by_cond:
        m["both_sides_share"] = sum(1 for s in sides_by_cond.values() if len(s) > 1) / len(sides_by_cond)
    # Удержание по сделкам (запасной вариант, если в позициях нет first_entry_at):
    # первая покупка токена → первая продажа после неё (или конец позиции).
    first_buy: dict[str, float] = {}
    first_sell: dict[str, float] = {}
    for d in sorted(ol, key=lambda x: x["t0"]):
        if d["side"] == "BUY":
            first_buy.setdefault(d["token"], d["t0"])
        elif d["side"] == "SELL" and d["token"] in first_buy and d["token"] not in first_sell:
            first_sell[d["token"]] = d["t0"]
    end_by_token = {str(p.get("token_id")): settle_ts(p) for p in resolved or []}
    holds = []
    sold_early = 0
    for tok, t0 in first_buy.items():
        if tok in first_sell:
            holds.append(first_sell[tok] - t0)
            sold_early += 1
        elif end_by_token.get(tok):
            holds.append(max(0.0, end_by_token[tok] - t0))
    if holds:
        m["median_hold_h"] = _median(holds) / 3600.0
        m["short_hold_share"] = sum(1 for h in holds if h < 3600) / len(holds)
        m["hold_source"] = "trades"
    if first_buy:
        m["sell_before_end_share"] = sold_early / len(first_buy)
    # Доля тейкерских сделок — по деньгам, а не по строкам: одна лимитка,
    # исполненная 50 мелкими кусками, иначе выглядит как 50 «мейкерских сделок».
    if taker_trades is not None and fill_ts:
        lo = fill_ts[0]
        tk_ts = [_ts(t.get("timestamp")) for t in taker_trades if t.get("timestamp")]
        if taker_cap and len(taker_trades) >= taker_cap and tk_ts:
            lo = max(lo, min(tk_ts))
        tk_usd = sum(fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
                     for t in taker_trades if _ts(t.get("timestamp")) >= lo)
        all_usd = sum(fnum(t.get("usdc_size")) or fnum(t.get("size")) * fnum(t.get("price"))
                      for t in rows if _ts(t.get("timestamp")) >= lo)
        if all_usd > 0:
            m["taker_share"] = min(1.0, tk_usd / all_usd)
    return m


# ------------------------------------------------------ копируемость ----

def drift_metrics(samples: list[dict]) -> dict:
    """samples: [{'drift_3s': ¢, 'drift_60s': ¢}] — насколько цена ушла
    против нас через 3 и 60 секунд после их покупки."""
    d3 = [s["drift_3s"] for s in samples if s.get("drift_3s") is not None]
    d60 = [s["drift_60s"] for s in samples if s.get("drift_60s") is not None]
    return {"drift_samples": len(d3), "drift_3s_cents": _median(d3), "drift_60s_cents": _median(d60)}


# ------------------------------------------------------ итог ----

def _clip01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_wallet(m: dict) -> tuple[float, str, list[str]]:
    """Оценка 0-100, вердикт и причины (по-русски)."""
    reasons: list[str] = []
    hard: list[str] = []
    n = m.get("n_resolved") or 0
    if n < 20:
        hard.append(f"мало закрытых ставок ({n} < 20) — не отличить навык от удачи")
    opd = m.get("orders_per_active_day")
    p90 = m.get("orders_per_day_p90")
    burst = m.get("burst_share") or 0
    if (opd is not None and opd > 60) or (p90 is not None and p90 > 200) or burst > 0.5:
        hard.append(f"похоже на бота: ~{opd or 0:.0f} решений в активный день, "
                    f"{burst * 100:.0f}% подряд быстрее 10 с")
    fast = max(m.get("fast_share_trades") or 0, m.get("fast_share_pos") or 0)
    if fast > 0.5:
        hard.append(f"{fast * 100:.0f}% денег в быстрых крипто-рынках (5м/15м/час) — не успеть")
    if (m.get("both_sides_share") or 0) > 0.35:
        hard.append(f"маркет-мейкер/арбитраж: покупает обе стороны в {m['both_sides_share'] * 100:.0f}% рынков")
    inc = m.get("mm_income_share")
    if inc is not None and inc > 0.35:
        hard.append(f"живёт на ребейтах/наградах/возвратах комиссий ({inc * 100:.0f}% дохода) — копией не повторить")
    if (m.get("top1_share") or 0) > 0.5 or ((m.get("pnl_ex_top3") or 0) <= 0 and n >= 20):
        hard.append("прибыль держится на 1-3 удачных ставках")
    bws = m.get("biggest_win_share")
    if bws is not None and bws > 0.6:
        hard.append(f"лучшая ставка = {bws * 100:.0f}% всей прибыли")
    roi = m.get("roi_resolved")
    if roi is not None and roi <= 0:
        hard.append(f"в минусе по закрытым ставкам (ROI {roi * 100:.1f}%)")
    atp = m.get("all_time_pnl")
    if atp is not None and atp < 0:
        hard.append(f"за всё время в минусе ({atp:+,.0f}$)".replace(",", " "))
    if m.get("pnl_90d") is not None and m["pnl_90d"] < 0:
        hard.append(f"последние 90 дней в минусе ({m['pnl_90d']:+.0f}$)")
    age = m.get("last_trade_age_h")
    if age is not None and age > 14 * 24:
        hard.append(f"не торгует {age / 24:.0f} дн.")
    hold = m.get("median_hold_h")
    if hold is not None and hold < 0.5:
        hard.append(f"держит позиции ~{hold * 60:.0f} мин — скальпер, не успеть")
    short = m.get("short_hold_share")
    if m.get("hold_source") == "positions" and short is not None and short > 0.5:
        hard.append(f"{short * 100:.0f}% денег в позициях короче часа — не успеть")

    # --- баллы ---
    z = m.get("z_bets") or 0.0
    zu = m.get("z_usd") or 0.0
    s_skill = 30 * _clip01(min(z, zu + 1.0) / 4.0)
    s_roi = 15 * _clip01((roi or 0) / 0.30)
    pw = m.get("pos_weeks_share")
    s_weeks = 10 * _clip01(((pw if pw is not None else 0.5) - 0.45) / 0.35)
    dd = m.get("max_dd_90d")
    p90d = m.get("pnl_90d")
    if dd is not None and p90d and p90d > 0:
        s_dd = 10 * _clip01(1.0 - (dd / p90d - 0.2) / 0.8)
    else:
        s_dd = 3.0
    if hold is None:
        s_hold = 4.0
    else:
        s_hold = 10 * _clip01(math.log10(max(hold, 0.01) / 0.5) / math.log10(48 / 0.5))
    opd_v = opd if opd is not None else 5
    s_freq = 5.0 if 0.2 <= opd_v <= 25 else (2.0 if opd_v <= 60 else 0.0)
    d3 = m.get("drift_3s_cents")
    s_drift = 5 * _clip01(1.0 - ((d3 if d3 is not None else 1.5) - 0.5) / 3.0)
    top1 = m.get("top1_share")
    s_robust = 10 * (_clip01(1.0 - ((top1 if top1 is not None else 0.5) - 0.15) / 0.35)
                     if (m.get("pnl_ex_top3") or 0) > 0 else 0.0)
    s_recent = 5.0 if (age is not None and age <= 72) else (2.0 if age is not None and age <= 14 * 24 else 0.0)
    score = s_skill + s_roi + s_weeks + s_dd + s_hold + s_freq + s_drift + s_robust + s_recent
    fav = m.get("fav_share") or 0
    if fav > 0.6:
        score -= 10
        reasons.append(f"{fav * 100:.0f}% денег во входах ≥0.90 — мало апсайда, редкий проигрыш съедает всё")
    if (m.get("longshot_share") or 0) > 0.5:
        score -= 5
        reasons.append("в основном лотерейки ≤0.10 — результат очень шумный")
    tk = m.get("taker_share")
    if tk is not None and tk < 0.15 and (m.get("n_orders_sample") or 0) > 30:
        score -= 5
        reasons.append(f"входит в основном лимитками (по рынку лишь {tk * 100:.0f}% объёма) — "
                       "копия по рынку будет на спред хуже их цены")
    if d3 is not None and d3 > 2:
        reasons.append(f"после их входа цена за 3 с уходит на {d3:.1f}¢ — копия будет хуже")
    live = m.get("live_share")
    is_live = bool(live is not None and live > 0.4 and (m.get("sports_buy_share") or 0) >= 0.3)
    if is_live:
        score -= 10
        reasons.append(f"похоже, ставит по ходу матча (~{live * 100:.0f}% спортивных денег): копия опоздает, "
                       "а фильтр «лайв» такие сделки пропустит")
    if z >= 2 and zu >= 1.5:
        reasons.append(f"edge значим: {m.get('edge_cents', 0):+.1f}¢ на ставку, z={z:.1f}")
    elif n >= 20:
        reasons.append(f"edge {m.get('edge_cents', 0):+.1f}¢ на ставку, z={z:.1f} — пока не доказано")
    if hold is not None and hold >= 24:
        reasons.append(f"держит позиции ~{hold / 24:.1f} дн. — копировать легко")
    lag = m.get("pnl_lag_days")
    if lag is not None and lag > PNL_STALE_DAYS:
        reasons.append(f"PnL у Polymarket обновлён {lag:.0f} дн. назад — окна 7/30/90 дн. на эту дату")
    score = max(0.0, min(100.0, score))
    # «✅» только с достаточной историей и не для лайв-игроков: короткая
    # выборка или ставки по ходу матча — максимум «наблюдать».
    caps_green = []
    if n < 60:
        caps_green.append(f"для ✅ мало ставок ({n} < 60)")
    acc_days = m.get("account_age_days")
    span = m.get("window_days")
    if (acc_days is not None and acc_days < 60) or (span is not None and span < 30):
        caps_green.append(f"короткая история ({min(x for x in (acc_days, span) if x is not None):.0f} дн.)")
    if is_live:
        caps_green.append("лайв-ставки")
    edge_c = m.get("edge_cents")
    if (d3 is not None and (m.get("drift_samples") or 0) >= 3 and edge_c and edge_c > 0 and d3 >= 0.5 * edge_c):
        caps_green.append(f"цена за 3 с уходит на {d3:.1f}¢ — это больше половины их перевеса")
    if caps_green and not hard and score >= 65 and z >= 2 and zu >= 1.5:
        reasons.append("до ✅ не хватает: " + ", ".join(caps_green))
    if hard:
        verdict = "❌ не копировать"
        reasons = hard + reasons
    elif score >= 65 and z >= 2 and zu >= 1.5 and not caps_green:
        verdict = "✅ кандидат"
    elif score >= 45:
        verdict = "🟡 наблюдать (виртуально)"
    else:
        verdict = "⚪ слабый"
    return round(score, 1), verdict, reasons


def analyze_wallet(raw: dict, now: float | None = None) -> dict:
    """raw: stats, pnl_points, closed, redeemable, open, trades, taker_trades, drift, caps."""
    now = now or time.time()
    caps = raw.get("caps") or {}
    closed = raw.get("closed") or []
    redeem = raw.get("redeemable") or []
    window = sample_window(closed, redeem, caps)
    resolved = resolved_positions(closed, redeem)
    m: dict = {}
    m.update(edge_metrics(resolved, now, window))
    m["window_capped"] = window is not None
    m.update(pnl_series_metrics(raw.get("pnl_points") or [], now))
    m.update(trade_metrics(raw.get("trades") or [], raw.get("taker_trades"), resolved, now,
                           taker_cap=caps.get("taker")))
    m.update(hold_metrics(closed))
    m.update(live_metrics(raw.get("trades") or [], closed))
    m.update(drift_metrics(raw.get("drift") or []))
    ss = stats_summary(raw.get("stats"))
    m["all_time_pnl"] = ss["trading_pnl"] if ss["trading_pnl"] is not None else m.get("pnl_total")
    m["economic_pnl"] = ss["economic_pnl"] if ss["economic_pnl"] is not None else m.get("economic_pnl_series")
    wallet_income = ss["wallet_income"] if ss["wallet_income"] is not None else m.get("wallet_income")
    refunds = ss["fees_refunded"] if ss["fees_refunded"] is not None else m.get("fees_refunded")
    m["wallet_income"] = wallet_income
    m["fees_refunded"] = refunds
    m["mm_income_share"] = income_share(m["all_time_pnl"], wallet_income, refunds, m.get("lp_pnl"))
    m["biggest_win"] = ss["biggest_win"]
    atp = m["all_time_pnl"]
    m["biggest_win_share"] = (ss["biggest_win"] / atp) if (ss["biggest_win"] is not None and atp and atp > 0) else None
    m["trades_lifetime"] = ss["markets_traded"]
    m["trade_count_lifetime"] = ss["trade_count"]
    m["volume_lifetime"] = ss["volume_usdc"] if ss["volume_usdc"] is not None else m.get("volume_usdc")
    if m["volume_lifetime"] and atp is not None:
        m["roi_on_volume"] = atp / m["volume_lifetime"]
    if ss["join_ts"]:
        m["account_age_days"] = (now - ss["join_ts"]) / 86400
    # OPEN в v2 включает и разрешённые, но не погашенные позиции — их не
    # считаем «открытыми»: они уже в статистике ставок.
    open_pos = [p for p in raw.get("open") or [] if not _truthy(p.get("redeemable"))]
    m["open_positions"] = len(open_pos)
    m["open_value"] = sum(fnum(p.get("current_value")) for p in open_pos)
    m["open_cost"] = sum(fnum(p.get("entry_cost_usdc")) for p in open_pos)
    m["open_upnl"] = sum(fnum(p.get("unrealized_pnl")) for p in open_pos)
    score, verdict, reasons = score_wallet(m)
    m["score"] = score
    m["verdict"] = verdict
    m["reasons"] = "; ".join(reasons)
    return m
