"""
Лёгкое SQLite-хранилище: сигналы (для последующего бэктеста стратегии) и
сделки (для учёта PnL). Никакой внешней БД не нужно для старта.

Таблица signals пишет КАЖДЫЙ тик, вошёл бот или нет — это основной
датасет для анализа: раз в 15 минут рынок резолвится, и мы можем
подписать (label_signals_outcome) все тики этого рынка исходом,
получая полноценные признаки + метку для последующего подбора формулы.
"""
from __future__ import annotations
import os
import sqlite3
import time
from contextlib import contextmanager

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    current_price REAL,
    strike_price REAL,
    direction TEXT,
    entry_price REAL,
    safety_score REAL,
    minutes_left REAL,
    distance_atr REAL,
    should_enter INTEGER,
    reasons TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    condition_id TEXT,
    direction TEXT,
    entry_price REAL,
    size_usdc REAL,
    order_id TEXT,
    status TEXT,
    outcome TEXT,
    pnl_usdc REAL,
    dry_run INTEGER
);

CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS momentum_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    asset TEXT,
    timeframe TEXT,
    side TEXT,
    checkpoint_price REAL,
    minutes_left REAL,
    rsi REAL,
    macd_histogram REAL,
    macd_bullish INTEGER,
    atr REAL,
    atr_ratio_to_avg REAL,
    ema_fast REAL,
    ema_slow REAL,
    ema_fast_slope REAL,
    trend_up INTEGER,
    bb_width REAL,
    bb_percent_b REAL,
    volume_ratio REAL,
    book_imbalance REAL,
    final_outcome TEXT,
    side_won INTEGER
);

CREATE TABLE IF NOT EXISTS hedge_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    asset TEXT,
    timeframe TEXT,
    side TEXT,
    entry_price REAL,
    entry_shares REAL,
    entry_cost REAL,
    entry_token_id TEXT,
    hedge_price REAL,
    hedge_shares REAL,
    hedge_cost REAL,
    hedge_token_id TEXT,
    hedge_ts INTEGER,
    status TEXT,
    outcome TEXT,
    pnl_usdc REAL,
    dry_run INTEGER
);
"""

HEDGE_COLUMNS = [
    "id", "ts", "market_slug", "asset", "timeframe", "side", "entry_price", "entry_shares",
    "entry_cost", "entry_token_id", "hedge_price", "hedge_shares", "hedge_cost", "hedge_token_id",
    "hedge_ts", "status", "outcome", "pnl_usdc", "dry_run",
]


def create_hedge_position(market_slug: str, asset: str, timeframe: str, side: str,
                           entry_price: float, entry_shares: float, entry_cost: float,
                           entry_token_id: str, dry_run: bool) -> int:
    with _conn() as conn:
        cur = conn.execute(
            """INSERT INTO hedge_positions
               (ts, market_slug, asset, timeframe, side, entry_price, entry_shares, entry_cost,
                entry_token_id, status, dry_run)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'open_unhedged', ?)""",
            (int(time.time()), market_slug, asset, timeframe, side, entry_price, entry_shares,
             entry_cost, entry_token_id, int(dry_run)),
        )
        return cur.lastrowid


def get_open_hedge_position(market_slug: str, side: str):
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, entry_shares, entry_cost, status FROM hedge_positions "
            "WHERE market_slug = ? AND side = ? AND status IN ('open_unhedged', 'hedged')",
            (market_slug, side),
        )
        return cur.fetchone()


def mark_hedged(position_id: int, hedge_price: float, hedge_shares: float, hedge_cost: float,
                hedge_token_id: str) -> None:
    with _conn() as conn:
        conn.execute(
            """UPDATE hedge_positions SET status = 'hedged', hedge_price = ?, hedge_shares = ?,
               hedge_cost = ?, hedge_token_id = ?, hedge_ts = ? WHERE id = ?""",
            (hedge_price, hedge_shares, hedge_cost, hedge_token_id, int(time.time()), position_id),
        )


def settle_hedge_position(position_id: int, outcome: str, pnl_usdc: float) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE hedge_positions SET status = 'closed', outcome = ?, pnl_usdc = ? WHERE id = ?",
            (outcome, pnl_usdc, position_id),
        )


def get_unsettled_hedge_positions():
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, market_slug, side, entry_shares, entry_cost, hedge_shares, hedge_cost, status, dry_run "
            "FROM hedge_positions WHERE status IN ('open_unhedged', 'hedged')"
        )
        return cur.fetchall()


def get_hedge_positions_since(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        cols = ", ".join(HEDGE_COLUMNS)
        cur = conn.execute(f"SELECT {cols} FROM hedge_positions WHERE ts >= ? ORDER BY ts ASC", (since_ts,))
        return cur.fetchall()

# Колонки, добавленные уже после первого релиза — через ALTER TABLE, чтобы
# не терять историю на уже задеплоенных базах. (column_name, sql_type)
_SIGNALS_MIGRATIONS = [
    ("atr", "REAL"),
    ("atr_ratio_to_avg", "REAL"),
    ("ema_fast", "REAL"),
    ("ema_slow", "REAL"),
    ("ema_fast_slope", "REAL"),
    ("trend_up", "INTEGER"),
    ("time_score", "REAL"),
    ("distance_score", "REAL"),
    ("trend_score", "REAL"),
    ("vol_score", "REAL"),
    ("liq_score", "REAL"),
    ("ask_liquidity_usdc", "REAL"),
    ("up_best_ask", "REAL"),
    ("down_best_ask", "REAL"),
    ("book_source", "TEXT"),
    ("outcome", "TEXT"),
    ("asset", "TEXT"),
    ("timeframe", "TEXT"),
    ("macd_histogram", "REAL"),
    ("macd_bullish", "INTEGER"),
    # Какая проверка записала тик: "slow" — обычная раз в 3 с (REST-свечи),
    # "fast" — быстрая (цена Binance из потока, см. src/fast_signal.py; пишет
    # только тики с сигналом, не чаще раза в 3 с на рынок).
    ("tick_source", "TEXT"),
    # Источник цены и страйка, по которым считался сигнал: "chainlink" (как у
    # Polymarket при расчёте исхода) или "binance"; "none" — режим Chainlink, но
    # его цены/страйка нет, сигнал не считался. binance_price/binance_strike —
    # цена и страйк Binance на том же тике, чтобы сравнивать источники.
    ("price_source", "TEXT"),
    ("binance_price", "REAL"),
    ("binance_strike", "REAL"),
]

_TRADES_MIGRATIONS = [
    ("token_id", "TEXT"),
    ("source", "TEXT"),  # 'strategy' (наш сигнал) | 'copytrade' (скопировано с отслеживаемого кошелька)
    # С чем бот входил: ask на сигнале (entry_price — это потолок ордера),
    # score и диапазон входа монеты на тот момент. Диапазон и ставку теперь
    # можно задать каждой монете свои — без этих колонок по отчётам не понять,
    # какие сделки сделаны при каких настройках.
    ("signal_ask", "REAL"),
    ("safety_score", "REAL"),
    ("range_min", "REAL"),
    ("range_max", "REAL"),
    # Какая проверка нашла сигнал: "slow" (раз в 3 с) или "fast" (быстрая)
    ("entry_path", "TEXT"),
    # Бот двух стратегий (src/duo.py): "virt" — виртуальная сделка по каждому
    # сигналу, "A" — стратегия 1️⃣ «после проигрыша», "B" — 2️⃣ «пауза после
    # проигрыша». Пусто — обычный режим (сигнал = сделка) или копитрейдинг.
    ("strategy", "TEXT"),
    # Исход, который бот определил сам в момент конца рынка (Chainlink на конце
    # окна против страйка) — только у виртуальных сделок; outcome — официальный.
    ("early_outcome", "TEXT"),
]


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# True, если при этом запуске файла базы ещё не было (создана с нуля). При
# каждом деплое без постоянного тома на /app/data так и будет — тогда
# настройки из Telegram и статистика сбрасываются; main.py предупреждает.
DB_WAS_NEW = False


def init_db():
    global DB_WAS_NEW
    DB_WAS_NEW = not os.path.exists(settings.DB_PATH)
    with _conn() as conn:
        conn.executescript(_SCHEMA)
        existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
        for col, sql_type in _SIGNALS_MIGRATIONS:
            if col not in existing:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {sql_type}")
        existing_trades = {row[1] for row in conn.execute("PRAGMA table_info(trades)")}
        for col, sql_type in _TRADES_MIGRATIONS:
            if col not in existing_trades:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {sql_type}")


def log_signal(market_slug: str, current_price: float, strike_price: float, decision,
               indicators: dict | None = None, up_book=None, down_book=None,
               tick_source: str = "slow", price_source: str | None = None,
               binance_price: float | None = None, binance_strike: float | None = None) -> None:
    """
    indicators/up_book/down_book необязательны (обратная совместимость), но
    без них отчёт для анализа будет неполным — main.py всегда должен их
    передавать.
    """
    indicators = indicators or {}
    asset, timeframe_label = parse_market_slug(market_slug)
    with _conn() as conn:
        conn.execute(
            """INSERT INTO signals
               (ts, market_slug, current_price, strike_price, direction, entry_price,
                safety_score, minutes_left, distance_atr, should_enter, reasons,
                atr, atr_ratio_to_avg, ema_fast, ema_slow, ema_fast_slope, trend_up,
                time_score, distance_score, trend_score, vol_score, liq_score,
                ask_liquidity_usdc, up_best_ask, down_best_ask, book_source, asset, timeframe,
                macd_histogram, macd_bullish, tick_source, price_source, binance_price, binance_strike)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                int(time.time()), market_slug, current_price, strike_price, decision.direction,
                decision.entry_price, decision.safety_score, decision.minutes_left,
                decision.distance_atr, int(decision.should_enter), "; ".join(decision.reasons),
                indicators.get("atr"), indicators.get("atr_ratio_to_avg"),
                indicators.get("ema_fast"), indicators.get("ema_slow"), indicators.get("ema_fast_slope"),
                int(indicators.get("trend_up")) if indicators.get("trend_up") is not None else None,
                decision.time_score, decision.distance_score, decision.trend_score,
                decision.vol_score, decision.liq_score,
                (up_book.ask_liquidity_usdc if decision.direction == "UP" else down_book.ask_liquidity_usdc)
                if (up_book and down_book) else None,
                up_book.best_ask if up_book else None,
                down_book.best_ask if down_book else None,
                (up_book.source if decision.direction == "UP" else down_book.source)
                if (up_book and down_book) else None,
                asset, timeframe_label,
                indicators.get("macd_histogram"),
                int(indicators.get("macd_bullish")) if indicators.get("macd_bullish") is not None else None,
                tick_source, price_source, binance_price, binance_strike,
            ),
        )


def label_signals_outcome(market_slug: str, outcome: str) -> int:
    """Проставляет исход рынка всем ещё не подписанным тикам этого рынка.
    Возвращает число обновлённых строк."""
    with _conn() as conn:
        cur = conn.execute(
            "UPDATE signals SET outcome = ? WHERE market_slug = ? AND outcome IS NULL",
            (outcome, market_slug),
        )
        return cur.rowcount


def get_markets_needing_outcome(exclude_slugs: set[str] | None, limit: int = 50) -> list[str]:
    """Слаги рынков, у которых есть сигналы без проставленного исхода —
    кандидаты на то, чтобы спросить Gamma API, не зарезолвились ли они."""
    exclude_slugs = exclude_slugs or set()
    with _conn() as conn:
        cur = conn.execute(
            "SELECT DISTINCT market_slug FROM signals WHERE outcome IS NULL ORDER BY ts ASC LIMIT ?",
            (limit + len(exclude_slugs),),
        )
        rows = [row[0] for row in cur.fetchall() if row[0] not in exclude_slugs]
        return rows[:limit]


def log_trade(market_slug: str, condition_id: str, direction: str, entry_price: float,
              size_usdc: float, order_id: str, status: str, dry_run: bool, token_id: str = "",
              source: str = "strategy", signal_ask: float | None = None,
              safety_score: float | None = None, range_min: float | None = None,
              range_max: float | None = None, entry_path: str | None = None,
              strategy: str | None = None) -> int:
    with _conn() as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (ts, market_slug, condition_id, direction, entry_price, size_usdc,
                order_id, status, outcome, pnl_usdc, dry_run, token_id, source,
                signal_ask, safety_score, range_min, range_max, entry_path, strategy)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (int(time.time()), market_slug, condition_id, direction, entry_price,
             size_usdc, order_id, status, int(dry_run), token_id, source,
             signal_ask, safety_score, range_min, range_max, entry_path, strategy),
        )
        return cur.lastrowid


def settle_trade(trade_id: int, outcome: str, pnl_usdc: float) -> bool:
    """Закрывает сделку, только если она ещё открыта. Резолюция и стоп-лосс
    теперь живут в разных фоновых циклах (стоп — раз в ~2 с), поэтому
    повторное закрытие уже закрытой сделки молча игнорируем и сообщаем об
    этом через False — вызывающий код тогда не шлёт второе уведомление."""
    with _conn() as conn:
        cur = conn.execute(
            "UPDATE trades SET outcome = ?, pnl_usdc = ? WHERE id = ? AND outcome IS NULL",
            (outcome, pnl_usdc, trade_id),
        )
        return cur.rowcount > 0


def get_open_trade_for_market(market_slug: str, strategy: str | None = None):
    """Открытая сделка по рынку. strategy — только сделки этой стратегии
    (бот двух стратегий: по одному рынку бывает виртуальная + 1️⃣ + 2️⃣)."""
    q = ("SELECT id, condition_id, direction, entry_price, size_usdc FROM trades "
         "WHERE market_slug = ? AND outcome IS NULL")
    params: list = [market_slug]
    if strategy is not None:
        q += " AND strategy = ?"
        params.append(strategy)
    with _conn() as conn:
        return conn.execute(q, params).fetchone()


_INTERVAL_SEC = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}


def market_end_ts(slug: str) -> int | None:
    """'btc-updown-5m-1791306300' -> 1791306600 (начало окна + длина). None,
    если слаг другого формата."""
    try:
        _asset, label = parse_market_slug(slug)
        start = int(slug.rsplit("-", 1)[1])
    except (ValueError, IndexError, AttributeError):
        return None
    interval = _INTERVAL_SEC.get(label)
    return start + interval if interval else None


def count_open_trades(live_only: bool = False, running_only: bool = False, now: float | None = None) -> int:
    """Общее число сейчас открытых позиций по ВСЕМ активам/таймфреймам —
    используется для общего лимита позиций (см. executor.py).
    live_only=True — только реальные: виртуальные (DRY) сделки монет в
    режиме DRY не должны занимать место реальных.
    running_only=True — только позиции в рынках, которые ещё идут. Отчёты
    07.10 показали: рынок закончился, исход уже не меняется, а Gamma API
    отмечает его закрытым через несколько минут — и всё это время позиция
    занимала слот, а сигналы других монет пропускались."""
    with _conn() as conn:
        q = "SELECT market_slug FROM trades WHERE outcome IS NULL"
        if live_only:
            q += " AND dry_run = 0"
        slugs = [row[0] for row in conn.execute(q).fetchall()]
    if not running_only:
        return len(slugs)
    now = time.time() if now is None else now
    count = 0
    for slug in slugs:
        end = market_end_ts(slug)
        if end is None or end > now:   # формат неизвестен — считаем, как раньше
            count += 1
    return count


def parse_market_slug(slug: str) -> tuple[str, str]:
    """'sol-updown-5m-1789270200' -> ('sol', '5m'). Если формат неожиданный,
    возвращает ('unknown', 'unknown') вместо падения — отчёты не должны
    рушиться из-за одного странного слага."""
    try:
        asset, rest = slug.split("-updown-", 1)
        label = rest.split("-", 1)[0]
        return asset, label
    except (ValueError, AttributeError):
        return "unknown", "unknown"


def get_unsettled_trades(with_strategy: bool = False):
    """Открытые сделки: (id, slug, condition_id, direction, entry_price, size,
    dry_run, token_id) и с with_strategy=True ещё (strategy, early_outcome)."""
    extra = ", strategy, early_outcome" if with_strategy else ""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run, token_id"
            f"{extra} FROM trades WHERE outcome IS NULL",
        )
        return cur.fetchall()


def get_pnl_summary(since_ts: int = 0, live_only: bool = False, dry_only: bool = False,
                    strategy: str | None = None):
    q = ("SELECT COUNT(*), COALESCE(SUM(pnl_usdc), 0), "
         "SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) "
         "FROM trades WHERE outcome IS NOT NULL AND ts >= ?")
    params: list = [since_ts]
    if live_only or dry_only:
        q += " AND dry_run = ?"
        params.append(0 if live_only else 1)
    if strategy is not None:
        q += " AND strategy = ?"
        params.append(strategy)
    with _conn() as conn:
        count, total_pnl, wins = conn.execute(q, params).fetchone()
        return {"trades": count or 0, "pnl_usdc": total_pnl or 0.0, "wins": wins or 0}


def get_outcome_stats(live: bool | None = None, since_ts: int = 0, asset: str | None = None,
                      strategy: str | None = None) -> dict:
    """Факт против ожидания по цене входа (см. src/loss_stats.py).

    Только сделки, дошедшие до резолюции (outcome UP/DOWN); досрочно
    закрытые по стоп-лоссу (STOPPED) не считаются. expected_losses =
    сумма (1 − entry_price): столько проигрышей «заложено» в цены входа.
    live=True — только реальные сделки, False — только DRY RUN, None — все.
    """
    q = ("SELECT entry_price, pnl_usdc, direction, outcome FROM trades "
         "WHERE outcome IN ('UP', 'DOWN') AND ts >= ?")
    params: list = [since_ts]
    if live is not None:
        q += " AND dry_run = ?"
        params.append(0 if live else 1)
    if asset:
        q += " AND market_slug LIKE ?"
        params.append(f"{asset.lower()}-updown-%")
    if strategy is not None:
        q += " AND strategy = ?"
        params.append(strategy)
    with _conn() as conn:
        rows = conn.execute(q, params).fetchall()
    pnl_total = sum(pnl for _p, pnl, _d, _o in rows if pnl is not None)
    win_pnls = [pnl for _p, pnl, d, o in rows if d == o and pnl is not None]
    loss_pnls = [pnl for _p, pnl, d, o in rows if d != o and pnl is not None]
    return {
        "trades": len(rows),
        "losses": sum(1 for _p, _pnl, d, o in rows if d != o),
        "expected_losses": sum(1.0 - float(p) for p, *_ in rows if p is not None),
        "avg_win": sum(win_pnls) / len(win_pnls) if win_pnls else 0.0,
        "avg_loss": sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0,
        "pnl_usdc": pnl_total,
    }


def get_pnl_by_asset_mode(since_ts: int = 0) -> dict[tuple[str, bool], dict]:
    """PnL по монетам отдельно для реальных и виртуальных сделок:
    {(asset, dry_run): {"trades", "pnl_usdc", "wins"}} — чтобы в отчётах и
    статистике виртуальная прибыль DRY-монет не смешивалась с реальной."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT market_slug, pnl_usdc, dry_run FROM trades WHERE outcome IS NOT NULL AND ts >= ?",
            (since_ts,),
        ).fetchall()
    out: dict[tuple[str, bool], dict] = {}
    for slug, pnl, dry in rows:
        asset, _label = parse_market_slug(slug)
        b = out.setdefault((asset, bool(dry)), {"trades": 0, "pnl_usdc": 0.0, "wins": 0})
        b["trades"] += 1
        b["pnl_usdc"] += pnl or 0.0
        if (pnl or 0.0) > 0:
            b["wins"] += 1
    return out


def get_pnl_by_asset(since_ts: int = 0) -> dict[str, dict]:
    """PnL/винрейт по каждому активу отдельно (суммируя все таймфреймы этого
    актива) — для кнопки 'Статистика' в Telegram."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT market_slug, pnl_usdc FROM trades WHERE outcome IS NOT NULL AND ts >= ?",
            (since_ts,),
        )
        rows = cur.fetchall()

    by_asset: dict[str, dict] = {}
    for slug, pnl in rows:
        asset, _label = parse_market_slug(slug)
        bucket = by_asset.setdefault(asset, {"trades": 0, "pnl_usdc": 0.0, "wins": 0})
        bucket["trades"] += 1
        bucket["pnl_usdc"] += pnl or 0.0
        if (pnl or 0.0) > 0:
            bucket["wins"] += 1
    return by_asset


def get_pnl_by_timeframe(since_ts: int = 0) -> dict[str, dict]:
    """То же самое, но сгруппировано по таймфрейму вместо актива (в этом боте таймфрейм всегда 5m, но колонка общая с основным ботом)."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT market_slug, pnl_usdc FROM trades WHERE outcome IS NOT NULL AND ts >= ?",
            (since_ts,),
        )
        rows = cur.fetchall()

    by_tf: dict[str, dict] = {}
    for slug, pnl in rows:
        _asset, label = parse_market_slug(slug)
        bucket = by_tf.setdefault(label, {"trades": 0, "pnl_usdc": 0.0, "wins": 0})
        bucket["trades"] += 1
        bucket["pnl_usdc"] += pnl or 0.0
        if (pnl or 0.0) > 0:
            bucket["wins"] += 1
    return by_tf


# --- Экспорт для периодических отчётов (см. src/reporting.py) ---

SIGNALS_COLUMNS = [
    "id", "ts", "market_slug", "asset", "timeframe", "current_price", "strike_price", "direction", "entry_price",
    "safety_score", "minutes_left", "distance_atr", "should_enter", "reasons",
    "atr", "atr_ratio_to_avg", "ema_fast", "ema_slow", "ema_fast_slope", "trend_up",
    "time_score", "distance_score", "trend_score", "vol_score", "liq_score",
    "ask_liquidity_usdc", "up_best_ask", "down_best_ask", "book_source", "outcome",
    "macd_histogram", "macd_bullish", "tick_source", "price_source", "binance_price", "binance_strike",
]

TRADES_COLUMNS = [
    "id", "ts", "market_slug", "condition_id", "direction", "entry_price", "size_usdc",
    "order_id", "status", "outcome", "pnl_usdc", "dry_run", "token_id", "source",
    "signal_ask", "safety_score", "range_min", "range_max", "entry_path",
    "strategy", "early_outcome",
]


MOMENTUM_COLUMNS = [
    "id", "ts", "market_slug", "asset", "timeframe", "side", "checkpoint_price", "minutes_left",
    "rsi", "macd_histogram", "macd_bullish", "atr", "atr_ratio_to_avg", "ema_fast", "ema_slow",
    "ema_fast_slope", "trend_up", "bb_width", "bb_percent_b", "volume_ratio", "book_imbalance",
    "final_outcome", "side_won",
]


def log_momentum_checkpoint(market_slug: str, asset: str, timeframe: str, side: str,
                             checkpoint_price: float, minutes_left: float, indicators: dict,
                             book_imbalance: float | None) -> None:
    with _conn() as conn:
        conn.execute(
            """INSERT INTO momentum_checkpoints
               (ts, market_slug, asset, timeframe, side, checkpoint_price, minutes_left,
                rsi, macd_histogram, macd_bullish, atr, atr_ratio_to_avg, ema_fast, ema_slow,
                ema_fast_slope, trend_up, bb_width, bb_percent_b, volume_ratio, book_imbalance)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                int(time.time()), market_slug, asset, timeframe, side, checkpoint_price, minutes_left,
                indicators.get("rsi"), indicators.get("macd_histogram"),
                int(indicators.get("macd_bullish")) if indicators.get("macd_bullish") is not None else None,
                indicators.get("atr"), indicators.get("atr_ratio_to_avg"),
                indicators.get("ema_fast"), indicators.get("ema_slow"), indicators.get("ema_fast_slope"),
                int(indicators.get("trend_up")) if indicators.get("trend_up") is not None else None,
                indicators.get("bb_width"), indicators.get("bb_percent_b"), indicators.get("volume_ratio"),
                book_imbalance,
            ),
        )


def label_momentum_outcome(market_slug: str, outcome: str) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE momentum_checkpoints SET final_outcome = ?, side_won = (side = ?) WHERE market_slug = ? AND final_outcome IS NULL",
            (outcome, outcome, market_slug),
        )


def get_momentum_markets_needing_outcome(exclude_slugs: set[str] | None, limit: int = 50) -> list[str]:
    exclude_slugs = exclude_slugs or set()
    with _conn() as conn:
        cur = conn.execute(
            "SELECT DISTINCT market_slug FROM momentum_checkpoints WHERE final_outcome IS NULL ORDER BY ts ASC LIMIT ?",
            (limit + len(exclude_slugs),),
        )
        rows = [row[0] for row in cur.fetchall() if row[0] not in exclude_slugs]
        return rows[:limit]


def get_momentum_since(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        cols = ", ".join(MOMENTUM_COLUMNS)
        cur = conn.execute(f"SELECT {cols} FROM momentum_checkpoints WHERE ts >= ? ORDER BY ts ASC", (since_ts,))
        return cur.fetchall()


def get_signals_since(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        cols = ", ".join(SIGNALS_COLUMNS)
        cur = conn.execute(f"SELECT {cols} FROM signals WHERE ts >= ? ORDER BY ts ASC", (since_ts,))
        return cur.fetchall()


def last_trade() -> tuple[str, int] | None:
    """(market_slug, ts) последней сделки бота по своим сигналам — для
    «🔎 Почему нет сделок»."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT market_slug, ts FROM trades WHERE COALESCE(source, 'strategy') = 'strategy' "
            "ORDER BY ts DESC LIMIT 1"
        ).fetchone()
    return (row[0], int(row[1])) if row else None


def get_trades_since(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        cols = ", ".join(TRADES_COLUMNS)
        cur = conn.execute(f"SELECT {cols} FROM trades WHERE ts >= ? ORDER BY ts ASC", (since_ts,))
        return cur.fetchall()


# --- Настройки, управляемые из Telegram (переживают рестарт процесса) ---

def get_all_settings() -> dict[str, str]:
    with _conn() as conn:
        cur = conn.execute("SELECT key, value FROM bot_settings")
        return {k: v for k, v in cur.fetchall()}


def set_setting(key: str, value) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO bot_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )


# --- Бот двух стратегий (src/duo.py) ---

def set_early_outcome(trade_id: int, outcome: str) -> None:
    """Исход, который бот определил сам в момент конца рынка (до официальной
    резолюции) — у виртуальной сделки."""
    with _conn() as conn:
        conn.execute("UPDATE trades SET early_outcome = ? WHERE id = ?", (outcome, trade_id))


def virtual_trades_since(since_ts: int) -> list[tuple]:
    """Виртуальные сделки: (id, market_slug, direction, outcome, early_outcome)
    по порядку входа."""
    with _conn() as conn:
        return conn.execute(
            "SELECT id, market_slug, direction, outcome, early_outcome FROM trades "
            "WHERE strategy = 'virt' AND ts >= ? ORDER BY ts ASC, id ASC", (since_ts,),
        ).fetchall()


def strategy_entries_since(since_ts: int) -> list[tuple]:
    """(market_slug, strategy) входов стратегий 1️⃣ и 2️⃣ с момента since_ts."""
    with _conn() as conn:
        return conn.execute(
            "SELECT market_slug, strategy FROM trades WHERE strategy IN ('A', 'B') AND ts >= ?",
            (since_ts,),
        ).fetchall()


def get_strategy_summary(since_ts: int = 0) -> dict[tuple[str, bool], dict]:
    """Итоги по стратегиям: {(strategy, dry_run): {"trades", "losses",
    "expected_losses", "pnl_usdc"}} — только сделки с исходом UP/DOWN."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT strategy, dry_run, entry_price, direction, outcome, pnl_usdc FROM trades "
            "WHERE outcome IN ('UP', 'DOWN') AND strategy IS NOT NULL AND ts >= ?", (since_ts,),
        ).fetchall()
    out: dict[tuple[str, bool], dict] = {}
    for strat, dry, price, direction, outcome, pnl in rows:
        b = out.setdefault((strat, bool(dry)), {"trades": 0, "losses": 0, "expected_losses": 0.0, "pnl_usdc": 0.0})
        b["trades"] += 1
        b["losses"] += int(direction != outcome)
        b["expected_losses"] += 1.0 - float(price or 0)
        b["pnl_usdc"] += pnl or 0.0
    return out


def virtual_results_since(since_ts: int) -> list[tuple]:
    """Виртуальные сделки с исходом: (ts входа, market_slug, entry_price,
    выиграла ли, pnl_usdc) — официальный исход, иначе ранний."""
    with _conn() as conn:
        rows = conn.execute(
            "SELECT ts, market_slug, entry_price, direction, outcome, early_outcome, pnl_usdc FROM trades "
            "WHERE strategy = 'virt' AND ts >= ? ORDER BY ts ASC, id ASC", (since_ts,),
        ).fetchall()
    out = []
    for ts, slug, price, direction, outcome, early, pnl in rows:
        res = outcome if outcome in ("UP", "DOWN") else (early if early in ("UP", "DOWN") else None)
        if res is not None:
            out.append((int(ts), slug, float(price or 0), res == direction, pnl))
    return out
