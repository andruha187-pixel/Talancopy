"""
Поиск кошельков для копирования по всему Polymarket.

Этапы:
 1. Кандидаты: лидерборды (неделя/месяц/всё время × категории), крупные
    сделки последних часов, крупнейшие держатели топ-рынков, «перепись»
    активных кошельков из потока RTDS, победители недели/месяца.
 2. Предфильтр (/v2/user-stats): плюс по PnL, ≥30 сделок, аккаунт не новый.
 3. Глубокий разбор лучших N: закрытые и неразрешённые позиции, PnL по дням,
    последние 1000 сделок, доля тейкерских сделок → метрики и оценка.
 4. Для лучших — «проскальзывание при копировании»: как уходит цена через
    3 и 60 секунд после их покупок.
 5. Архив (zip с CSV) в Telegram + топ-10 с кнопками «копировать виртуально».
Пришли архив в чат — разберу глубже и посоветую, кого копировать.
"""
from __future__ import annotations

import asyncio
import csv
import heapq
import io
import json
import logging
import math
import os
import time
import zipfile

from config import settings
from src import db, notifier, state
from src.discovery import collect
from src.discovery.analyze import analyze_wallet, drift_metrics, score_wallet
from src.util import esc, fnum, money, short_addr

log = logging.getLogger("discovery")

MIN_PNL = 300.0
MIN_TRADES = 30
MIN_ACCOUNT_DAYS = 21

WALLET_COLUMNS = [
    "rank", "wallet", "name", "sources", "score", "verdict", "reasons",
    "all_time_pnl", "pnl_7d", "pnl_30d", "pnl_90d", "n_resolved", "roi_resolved", "win_rate", "avg_entry",
    "edge_cents", "z_bets", "z_usd", "profit_factor", "top1_share", "top3_share", "pnl_ex_top3", "median_bet",
    "fav_share", "longshot_share", "fast_share_pos", "fast_share_trades", "n_resolved_30d", "roi_resolved_30d",
    "edge_by_price", "categories", "main_category", "max_dd", "max_dd_90d", "daily_sharpe", "best_day_share",
    "pos_weeks_share", "weeks_90d", "mm_income_share", "rebates", "rewards", "fees_paid", "volume_usdc",
    "n_trades_sample", "n_orders_sample", "sample_span_days", "orders_per_active_day", "orders_per_day_p90",
    "active_days_sample", "median_gap_min", "burst_share", "median_order_usdc", "p90_order_usdc", "buy_share",
    "avg_buy_price", "last_trade_age_h", "markets_in_sample", "top_event_share", "mid_price_share",
    "both_sides_share", "median_hold_h", "short_hold_share", "sell_before_end_share", "taker_share",
    "drift_samples", "drift_3s_cents", "drift_60s_cents", "account_age_days", "trades_lifetime",
    "open_positions", "open_value", "profile_url",
]
CLOSED_COLS = ["wallet", "src", "token_id", "condition_id", "title", "slug", "event_slug", "outcome", "avg_price",
               "entry_cost_usdc", "realized_pnl", "unrealized_pnl", "total_pnl", "current_price", "last_event_at"]
TRADE_COLS = ["wallet", "timestamp", "side", "price", "size", "usdc_size", "token_id", "condition_id", "slug",
              "event_slug", "outcome", "outcome_index", "transaction_hash"]
OPEN_COLS = ["wallet", "token_id", "condition_id", "title", "slug", "event_slug", "outcome", "current_size",
             "avg_price", "entry_cost_usdc", "current_price", "current_value", "unrealized_pnl", "last_event_at"]
PNL_COLS = ["wallet", "timestamp", "total_pnl", "cumulative_pnl", "realized_total_pnl", "unrealized_total_pnl",
            "volume_usdc", "trade_count", "fees_paid", "rebates", "rewards", "realized_lp_pnl", "yield"]
# Сколько сырых строк по кошельку держим в памяти/кладём в архив.
CAP_TRADES, CAP_CLOSED, CAP_OPEN, CAP_PNL = 800, 600, 200, 500

README = """Архив поиска кошельков для копирования (Polymarket)

wallets.csv — по строке на разобранный кошелёк, отсортировано по score.
  score 0-100, verdict: ✅ кандидат / 🟡 наблюдать / ⚪ слабый / ❌ не копировать (+ reasons).
  edge_cents  — средний «перевес» ставки: (итог за акцию − цена входа), в центах. >0 = лучше рынка.
  z_bets      — значимость edge по ставкам (≥2 — навык вероятнее удачи); z_usd — то же в долларах.
  roi_resolved — PnL / вложено по закрытым + разрешённым позициям (включая непогашенные проигрыши).
  top1_share  — доля лучшей ставки во всей прибыли; pnl_ex_top3 — PnL без 3 лучших ставок.
  fav_share / longshot_share — доля денег во входах ≥0.90 / ≤0.10.
  fast_share_* — доля в быстрых крипто-рынках (5м/15м/час) — их не скопировать.
  orders_per_active_day, burst_share (сделки чаще 10 с) — признаки бота.
  median_hold_h — медианное удержание позиции, часы. taker_share — доля тейкерских сделок.
  both_sides_share — доля рынков, где покупал обе стороны (маркет-мейкинг/арбитраж).
  mm_income_share — доля ребейтов/наград в итоговом PnL.
  drift_3s/60s_cents — как уходит цена через 3/60 с после их покупки (сколько потеряет копирующий).
  pos_weeks_share — доля прибыльных недель за 90 дней; max_dd_90d — макс. просадка за 90 дней, $.
  edge_by_price — edge по корзинам цены входа; categories — доля денег и ROI по категориям.
closed_positions.csv, trades.csv, open_positions.csv, pnl_daily.csv, drift.csv — сырьё по лучшим кошелькам.
"""

progress: dict = {"running": False, "stage": "", "done": 0, "total": 0, "started": 0.0, "mode": ""}
_cancel = False
_progress_hook = None     # async fn(text)
_doc_sender = None        # async fn(path, caption)
_top_hook = None          # async fn(top_rows) — сообщение с кнопками


def set_hooks(progress_hook=None, doc_sender=None, top_hook=None) -> None:
    global _progress_hook, _doc_sender, _top_hook
    _progress_hook = progress_hook or _progress_hook
    _doc_sender = doc_sender or _doc_sender
    _top_hook = top_hook or _top_hook


def cancel() -> None:
    global _cancel
    _cancel = True


def status_text() -> str:
    if not progress["running"]:
        return "Поиск сейчас не идёт."
    el = time.time() - progress["started"]
    return (f"🔎 Поиск ({progress['mode']}): {progress['stage']} — {progress['done']}/{progress['total']}, "
            f"идёт {el / 60:.0f} мин")


async def _report_progress(force: bool = False) -> None:
    now = time.time()
    if _progress_hook and (force or now - progress.get("_last", 0) > 20):
        progress["_last"] = now
        try:
            await _progress_hook(status_text())
        except Exception:  # noqa: BLE001
            pass


# ------------------------------------------------------------ кандидаты ----

async def collect_candidates(full: bool, extra: list[str] | None = None) -> dict[str, dict]:
    cands: dict[str, dict] = {}

    def add(items: list[dict]) -> None:
        for it in items:
            w = it["wallet"]
            c = cands.setdefault(w, {"wallet": w, "name": it.get("name") or "", "sources": set(),
                                     "lb_pnl": None, "usdc": 0.0})
            c["sources"].add(it["src"])
            if it.get("name") and not c["name"]:
                c["name"] = it["name"]
            if it["src"].startswith("lb:") and it.get("pnl") is not None:
                c["lb_pnl"] = max(c["lb_pnl"] or -math.inf, it["pnl"])
            c["usdc"] += fnum(it.get("usdc"))

    periods = [("month", 300 if full else 200), ("all", 300 if full else 150), ("week", 150 if full else 100)]
    tasks = [collect.leaderboard(tp, "overall", rows) for tp, rows in periods]
    cats = collect.LEADERBOARD_CATEGORIES[1:]
    for cat in cats:
        tasks.append(collect.leaderboard("month", cat, 60 if full else 40))
        if full:
            tasks.append(collect.leaderboard("all", cat, 40))
    progress.update(stage="лидерборды", done=0, total=len(tasks))
    for coro in asyncio.as_completed(tasks):
        add(await coro)
        progress["done"] += 1
        await _report_progress()
        if _cancel:
            return cands
    progress.update(stage="крупные сделки и держатели", done=0, total=3)
    add(await collect.big_trades(2000, 1000 if full else 500))
    progress["done"] += 1
    cids = await collect.top_markets(30 if full else 15)
    add(await collect.holders(cids[: (40 if full else 20)]))
    progress["done"] += 1
    add(await collect.biggest_winners("month", 100))
    progress["done"] += 1
    try:
        add(await asyncio.to_thread(collect.census_candidates, 400 if full else 200))
    except Exception:  # noqa: BLE001
        pass
    for w in extra or []:
        add([{"wallet": w, "src": "manual"}])
    # Уже копируемых тоже разбираем — свежая оценка пригодится.
    add([{"wallet": w.address, "name": w.label, "src": "copied"} for w in state.wallets()])
    return cands


def _priority(c: dict, stats: dict | None) -> float:
    pnl = fnum((stats or {}).get("all_time_pnl"), c.get("lb_pnl") or 0.0)
    p = math.log10(max(pnl, 1.0))
    srcs = c["sources"]
    p += 0.6 * len(srcs)
    if any(s.startswith("lb:month") or s.startswith("lb:week") for s in srcs):
        p += 1.0
    if "manual" in srcs or "copied" in srcs:
        p += 100
    return p


async def prefilter(cands: dict[str, dict], keep: int) -> list[tuple[dict, dict | None]]:
    items = list(cands.values())
    # Ограничиваем число запросов: сначала самые «громкие» кандидаты.
    items.sort(key=lambda c: -_priority(c, None))
    items = items[: keep * 3]
    progress.update(stage="предфильтр", done=0, total=len(items))
    out: list[tuple[dict, dict | None]] = []
    sem = asyncio.Semaphore(4)
    now = time.time()

    async def one(c: dict):
        async with sem:
            if _cancel:
                return
            try:
                st = await collect.user_stats(c["wallet"])
            except Exception:  # noqa: BLE001
                st = None
            progress["done"] += 1
            await _report_progress()
            forced = "manual" in c["sources"] or "copied" in c["sources"]
            if not forced:
                pnl = fnum((st or {}).get("all_time_pnl"), c.get("lb_pnl") or 0.0)
                trades = fnum((st or {}).get("trades"), MIN_TRADES)
                if pnl < MIN_PNL or trades < MIN_TRADES:
                    return
                jd = (st or {}).get("join_date")
                if jd:
                    from src.markets import parse_iso
                    jts = parse_iso(jd) if isinstance(jd, str) else fnum(jd)
                    if jts and now - jts < MIN_ACCOUNT_DAYS * 86400:
                        return
            out.append((c, st))

    await asyncio.gather(*(one(c) for c in items))
    out.sort(key=lambda cs: -_priority(cs[0], cs[1]))
    return out[:keep]


# ------------------------------------------------------------ разбор ----

def _compact(rows: list[dict], cols: list[str], wallet: str, **extra) -> list[list]:
    out = []
    for r in rows:
        rec = []
        for c in cols:
            if c == "wallet":
                rec.append(wallet)
            elif c in extra:
                rec.append(extra[c])
            else:
                rec.append(r.get(c))
        out.append(rec)
    return out


def _slim(raw: dict, wallet: str) -> dict:
    """Сжимаем сырьё до строк архива (списки значений, а не словари) —
    иначе 60 кошельков × тысячи строк съедают сотни МБ памяти."""
    redeem = raw.get("redeemable") or []
    return {
        "closed": _compact((raw.get("closed") or [])[:CAP_CLOSED], CLOSED_COLS, wallet, src="closed")
        + _compact(redeem[:CAP_CLOSED // 2], CLOSED_COLS, wallet, src="redeemable"),
        "trades": _compact((raw.get("trades") or [])[:CAP_TRADES], TRADE_COLS, wallet),
        "open": _compact((raw.get("open") or [])[:CAP_OPEN], OPEN_COLS, wallet),
        "pnl": _compact((raw.get("pnl_points") or [])[-CAP_PNL:], PNL_COLS, wallet),
        # Для замера проскальзывания — последние покупки (полные поля).
        "buys": [t for t in (raw.get("trades") or []) if str(t.get("side") or "").upper() == "BUY"][:40],
        "drift": [],
    }


async def deep_analyze(selected: list[tuple[dict, dict | None]], detail_n: int) -> tuple[list[dict], dict]:
    results: list[dict] = []
    keep_raw: list[tuple[float, int, str]] = []     # min-heap (score, seq, wallet)
    raw_store: dict[str, dict] = {}
    seq = 0
    progress.update(stage="глубокий разбор", done=0, total=len(selected))
    sem = asyncio.Semaphore(3)

    async def one(c: dict, st: dict | None):
        nonlocal seq
        async with sem:
            if _cancel:
                return
            try:
                raw = await collect.fetch_wallet(c["wallet"], stats=st)
                m = analyze_wallet(raw)
            except Exception as exc:  # noqa: BLE001
                log.warning("Разбор %s: %s", c["wallet"][:10], exc)
                progress["done"] += 1
                return
            m.update({"wallet": c["wallet"], "name": c.get("name") or (raw["stats"] or {}).get("name") or "",
                      "sources": ",".join(sorted(c["sources"])),
                      "profile_url": f"https://polymarket.com/profile/{c['wallet']}"})
            results.append(m)
            seq += 1
            item = (m["score"], seq, c["wallet"])
            if len(keep_raw) < detail_n:
                heapq.heappush(keep_raw, item)
                raw_store[c["wallet"]] = _slim(raw, c["wallet"])
            elif item > keep_raw[0]:
                _, _, evicted = heapq.heapreplace(keep_raw, item)
                raw_store.pop(evicted, None)
                raw_store[c["wallet"]] = _slim(raw, c["wallet"])
            del raw
            progress["done"] += 1
            await _report_progress()

    await asyncio.gather(*(one(c, st) for c, st in selected))
    return results, raw_store


async def add_drift(results: list[dict], raw_store: dict, n: int) -> None:
    pool = [m for m in sorted(results, key=lambda m: -m["score"])
            if not m["verdict"].startswith("❌") and m["wallet"] in raw_store][:n]
    progress.update(stage="проскальзывание при копировании", done=0, total=len(pool))
    for m in pool:
        if _cancel:
            return
        slim = raw_store[m["wallet"]]
        try:
            slim["drift"] = await collect.drift_samples(slim["buys"])
            m.update(drift_metrics(slim["drift"]))
            score, verdict, reasons = score_wallet(m)
            m.update({"score": score, "verdict": verdict, "reasons": "; ".join(reasons)})
        except Exception as exc:  # noqa: BLE001
            log.debug("drift %s: %s", m["wallet"][:10], exc)
        progress["done"] += 1
        await _report_progress()


# ------------------------------------------------------------ архив ----

def _fmt(v):
    if isinstance(v, float):
        if math.isinf(v):
            return "inf"
        return round(v, 5)
    return v


def build_archive(results: list[dict], raw_store: dict, meta: dict) -> str:
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M", time.gmtime())
    path = os.path.join(settings.REPORTS_DIR, f"wallets_{stamp}.zip")
    ranked = sorted(results, key=lambda m: (-m["score"], m["wallet"]))
    for i, m in enumerate(ranked, 1):
        m["rank"] = i

    def csv_text(header: list[str], rows: list[list]) -> str:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(header)
        w.writerows(rows)
        return buf.getvalue()

    wallet_rows = [[_fmt(m.get(c)) for c in WALLET_COLUMNS] for m in ranked]
    closed_rows, trade_rows, open_rows, pnl_rows, drift_rows = [], [], [], [], []
    for m in ranked:
        slim = raw_store.get(m["wallet"])
        if not slim:
            continue
        w = m["wallet"]
        closed_rows += slim["closed"]
        trade_rows += slim["trades"]
        open_rows += slim["open"]
        pnl_rows += slim["pnl"]
        for d in slim.get("drift") or []:
            drift_rows.append([w, d.get("ts"), d.get("token"), d.get("price"), d.get("drift_3s"),
                               d.get("drift_60s"), d.get("slug")])
    lines = [f"Поиск кошельков: {meta.get('mode')} | {time.strftime('%Y-%m-%d %H:%M', time.gmtime())} UTC",
             f"Кандидатов: {meta.get('n_candidates')} | разобрано глубоко: {len(results)} | "
             f"время: {meta.get('minutes', 0):.0f} мин", ""]
    for m in ranked[:25]:
        lines.append(f"#{m['rank']} {m['wallet']} {m.get('name') or ''}")
        lines.append(f"   {m['verdict']} score {m['score']} | PnL всего {fnum(m.get('all_time_pnl')):,.0f}$, "
                     f"30д {fnum(m.get('pnl_30d')):+,.0f}$ | ставок {m.get('n_resolved')} "
                     f"ROI {fnum(m.get('roi_resolved')) * 100:+.1f}% edge {fnum(m.get('edge_cents')):+.1f}¢ "
                     f"z={fnum(m.get('z_bets')):.1f} | удержание {fnum(m.get('median_hold_h')):.1f}ч | "
                     f"ордеров/день {fnum(m.get('orders_per_active_day')):.1f}")
        lines.append(f"   {m.get('reasons')}")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("wallets.csv", csv_text(WALLET_COLUMNS, wallet_rows))
        z.writestr("closed_positions.csv", csv_text(CLOSED_COLS, closed_rows))
        z.writestr("trades.csv", csv_text(TRADE_COLS, trade_rows))
        z.writestr("open_positions.csv", csv_text(OPEN_COLS, open_rows))
        z.writestr("pnl_daily.csv", csv_text(PNL_COLS, pnl_rows))
        z.writestr("drift.csv", csv_text(["wallet", "ts", "token_id", "their_price", "drift_3s_cents",
                                          "drift_60s_cents", "slug"], drift_rows))
        z.writestr("summary.txt", "\n".join(lines))
        z.writestr("README.txt", README)
    return path


def _top_payload(results: list[dict], n: int = 10) -> list[dict]:
    ranked = sorted(results, key=lambda m: -m["score"])
    out = []
    for m in ranked[:n]:
        out.append({k: _fmt(m.get(k)) for k in ("wallet", "name", "score", "verdict", "reasons", "all_time_pnl",
                                                "pnl_30d", "n_resolved", "roi_resolved", "edge_cents", "z_bets",
                                                "median_hold_h", "orders_per_active_day", "drift_3s_cents",
                                                "main_category")})
    return out


# ------------------------------------------------------------ запуск ----

async def run(full: bool = False, extra: list[str] | None = None) -> str | None:
    global _cancel
    if progress["running"]:
        return None
    _cancel = False
    started = time.time()
    progress.update(running=True, started=started, mode="полный" if full else "быстрый", stage="старт",
                    done=0, total=0)
    try:
        await _report_progress(force=True)
        cands = await collect_candidates(full, extra)
        n_deep = settings.DISCOVERY_FULL_WALLETS if full else settings.DISCOVERY_QUICK_WALLETS
        selected = await prefilter(cands, n_deep)
        if _cancel:
            notifier.notify("⏹ Поиск кошельков остановлен.")
            return None
        detail_n = max(settings.DISCOVERY_ARCHIVE_DETAIL, settings.DISCOVERY_DRIFT_WALLETS)
        results, raw_store = await deep_analyze(selected, detail_n)
        await add_drift(results, raw_store, settings.DISCOVERY_DRIFT_WALLETS)
        if not results:
            notifier.notify("🔎 Поиск завершён, но ни один кошелёк не прошёл предфильтр (или Data API не отвечает).")
            return None
        minutes = (time.time() - started) / 60
        meta = {"mode": progress["mode"], "n_candidates": len(cands), "minutes": minutes}
        path = await asyncio.to_thread(build_archive, results, raw_store, meta)
        top = _top_payload(results)
        db.execute_sync(
            "INSERT INTO discovery_runs (started, finished, mode, n_candidates, n_analyzed, archive_path, top_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (int(started), int(time.time()), progress["mode"], len(cands), len(results), path, json.dumps(top)))
        state.set("last_discovery_ts", time.time())
        good = sum(1 for m in results if m["verdict"].startswith("✅"))
        watch = sum(1 for m in results if m["verdict"].startswith("🟡"))
        caption = (f"🔎 Поиск кошельков ({progress['mode']}) за {minutes:.0f} мин\n"
                   f"Кандидатов: {len(cands)}, разобрано: {len(results)}\n"
                   f"✅ кандидатов: {good}, 🟡 наблюдать: {watch}\n"
                   "Пришли этот архив мне в чат — разберу и скажу, кого копировать.")
        if _doc_sender:
            await _doc_sender(path, caption)
        if _top_hook:
            await _top_hook(top)
        return path
    except Exception as exc:  # noqa: BLE001
        log.exception("Поиск кошельков упал")
        notifier.notify(f"⚠️ Поиск кошельков прервался: {esc(str(exc)[:300])}")
        return None
    finally:
        progress["running"] = False
        _cancel = False
        await _report_progress(force=True)


async def analyze_one(address: str) -> tuple[dict, str] | None:
    """Разбор одного кошелька по запросу: метрики + маленький архив."""
    raw = await collect.fetch_wallet(address, with_drift=True)
    m = analyze_wallet(raw)
    m.update({"wallet": address, "name": (raw.get("stats") or {}).get("name") or "", "sources": "manual",
              "profile_url": f"https://polymarket.com/profile/{address}"})
    slim = _slim(raw, address)
    slim["drift"] = raw.get("drift") or []
    path = await asyncio.to_thread(build_archive, [m], {address: slim}, {"mode": "один кошелёк",
                                                                         "n_candidates": 1, "minutes": 0})
    return m, path


def wallet_card(m: dict) -> str:
    def pct(v):
        return "—" if v is None else f"{fnum(v) * 100:.0f}%"

    def num(v, fmt="{:.1f}"):
        return "—" if v is None else fmt.format(fnum(v))

    return (
        f"<b>{esc(m.get('name') or short_addr(m['wallet']))}</b> <code>{m['wallet']}</code>\n"
        f"{m['verdict']} · score {m['score']}\n"
        f"PnL: всего {money(fnum(m.get('all_time_pnl')))}, 30д {money(fnum(m.get('pnl_30d')), True)}, "
        f"90д {money(fnum(m.get('pnl_90d')), True)}\n"
        f"Ставок закрыто: {m.get('n_resolved') or 0}, ROI {pct(m.get('roi_resolved'))}, "
        f"винрейт {pct(m.get('win_rate'))}, средний вход {num(m.get('avg_entry'), '{:.2f}')}\n"
        f"Edge: {num(m.get('edge_cents'), '{:+.1f}')}¢ на ставку, z={num(m.get('z_bets'))} "
        f"(z$={num(m.get('z_usd'))})\n"
        f"Лучшая ставка = {pct(m.get('top1_share'))} прибыли · прибыльных недель {pct(m.get('pos_weeks_share'))}\n"
        f"Ордеров в день: {num(m.get('orders_per_active_day'))} · удержание {num(m.get('median_hold_h'))} ч · "
        f"тейкер {pct(m.get('taker_share'))}\n"
        f"Быстрые рынки: {pct(max(fnum(m.get('fast_share_trades')), fnum(m.get('fast_share_pos'))))} · "
        f"категории: {esc(m.get('categories') or '—')}\n"
        f"Цена после их входа: 3с {num(m.get('drift_3s_cents'), '{:+.1f}')}¢, "
        f"60с {num(m.get('drift_60s_cents'), '{:+.1f}')}¢\n"
        f"<i>{esc(m.get('reasons') or '')}</i>"
    )


async def schedule_loop() -> None:
    await asyncio.sleep(120)
    while True:
        hours = fnum(state.get("discovery_auto_hours"))
        if hours > 0 and not progress["running"]:
            last = fnum(state.get("last_discovery_ts"))
            if time.time() - last >= hours * 3600:
                notifier.notify("⏰ Плановый поиск кошельков запущен.")
                await run(full=False)
        await asyncio.sleep(300)
