"""
📚 Серии UP/DOWN: что было после k одинаковых исходов подряд и чем кончилась бы
ставка на разворот с удвоением после проигрыша — по свечам Binance за
SERIES_HISTORY_DAYS дней, окна 5m и 15m. Только расчёт, ничего не покупает.

Исход окна — как у Polymarket: UP, если цена в конце не ниже, чем в начале;
берём свечу Binance того же окна (закрытие ≥ открытия). С официальным исходом
(Chainlink) это совпадает примерно в 87% окон 5m (архив 18.09–16.10):
расходятся окна, где цена почти не сдвинулась. На вопрос «зависит ли следующее
окно от серии» этого хватает.

Ставка — по ask на старте окна (SERIES_ENTRY_PRICE; по архиву в среднем
0.515), комиссия тейкера — ставка × FEE_RATE × (1 − цена). План (k, m): после
k одинаковых исходов ставим на разворот; проиграли — следующая ставка ×2, не
больше m ставок; проигранная серия ждёт, пока серия исходов прервётся.
"""
from __future__ import annotations
import asyncio
import csv
import io
import logging
import os
import time
import zipfile
from array import array

from config import settings
from src import binance_feed

log = logging.getLogger("history_study")

INTERVALS = {"5m": 300, "15m": 900}
KMAX = 8                       # k=8 — это «8 и больше»
SIM_K = (2, 3, 4, 5, 6, 7)
SIM_M = (1, 2, 3, 4, 5)
REQUEST_PAUSE_SEC = 0.15       # между запросами свечей (лимиты Binance)

_state = {"running": False, "started": 0.0, "finished": 0.0, "error": "", "progress": ""}
_tasks: set = set()            # ссылки на фоновые задачи, чтобы их не собрал сборщик мусора


def running() -> bool:
    return bool(_state["running"])


def entry_price() -> float:
    return float(settings.SERIES_ENTRY_PRICE)


def breakeven(price: float | None = None) -> float:
    """Доля выигрышей, при которой ставка по цене price с комиссией в ноль."""
    p = entry_price() if price is None else price
    return p + float(settings.FEE_RATE) * p * (1 - p)


async def fetch_returns(asset: str, interval: str, days: int) -> tuple[array, array]:
    """Время открытия окон (с) и доходность закрытие/открытие − 1."""
    symbol = binance_feed.symbol_for(asset)
    step = INTERVALS[interval]
    now = int(time.time())
    start_ms = (now - days * 86400) // step * step * 1000
    end_ms = now // step * step * 1000              # текущее, ещё не закрытое окно не берём
    times, rets = array("q"), array("d")
    while start_ms < end_ms:
        df = await binance_feed.get_klines(symbol, limit=1000, interval=interval, start_time_ms=start_ms)
        if df is None or df.empty:
            break
        last = None
        for ot, o, c in zip(df["open_time"], df["open"], df["close"]):
            ot = int(ot)
            if ot >= end_ms:
                break
            if o > 0:
                times.append(ot // 1000)
                rets.append(float(c) / float(o) - 1)
            last = ot
        if last is None or last + step * 1000 <= start_ms:
            break
        start_ms = last + step * 1000
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return times, rets


def runs(times, rets, step: int) -> list[tuple[int, int, bool, int]]:
    """Серии одинаковых исходов: (направление ±1, длина, прервана разворотом, время последнего окна).
    Разрыв в данных обрывает серию без разворота."""
    out = []
    cur, length, prev, last_t = None, 0, None, 0
    for t, r in zip(times, rets):
        d = 1 if r >= 0 else -1
        contiguous = prev is not None and t - prev == step
        if contiguous and d == cur:
            length += 1
        else:
            if cur is not None:
                out.append((cur, length, contiguous, last_t))
            cur, length = d, 1
        prev = last_t = t
    if cur is not None:
        out.append((cur, length, False, last_t))
    return out


def streak_table(rs, kmax: int = KMAX, t_from: int = 0, t_to: int = 1 << 62) -> tuple[list[int], list[int]]:
    """n[k], rev[k]: сколько раз после k одинаковых подряд было следующее окно и сколько из них — разворот."""
    n, rev = [0] * (kmax + 1), [0] * (kmax + 1)
    for _d, length, closed, t_end in rs:
        if not (t_from <= t_end < t_to):
            continue
        for k in range(1, length + 1):
            kk = min(k, kmax)
            if k < length:
                n[kk] += 1
            elif closed:
                n[kk] += 1
                rev[kk] += 1
    return n, rev


def simulate(rs, k: int, m: int, price: float | None = None) -> dict:
    """План (k, m) по сериям в порядке времени; ставки 1, 2, 4… (в единицах базовой ставки)."""
    price = entry_price() if price is None else price
    fee = float(settings.FEE_RATE) * (1 - price)            # на $1 ставки
    win_g, loss_g = 1 / price - 1 - fee, -1 - fee
    pnl = peak = mdd = staked = worst = 0.0
    series = wins = losses = bets = 0
    for _d, length, closed, _t in rs:
        if length < k:
            continue
        sp = 0.0
        for j in range(m):
            stake = 2.0 ** j
            if length > k + j:                 # серия продолжилась — ставка проиграла
                g = stake * loss_g
                if j == m - 1:
                    losses += 1
                    series += 1
            elif closed:                       # серия кончилась — ставка выиграла
                g = stake * win_g
                wins += 1
                series += 1
            else:                              # данные оборвались — исход ставки неизвестен
                break
            bets += 1
            staked += stake
            pnl += g
            sp += g
            peak = max(peak, pnl)
            mdd = max(mdd, peak - pnl)
            if length <= k + j or j == m - 1:
                worst = min(worst, sp)
                break
    return {"k": k, "m": m, "series": series, "wins": wins, "losses": losses, "bets": bets, "staked": staked,
            "pnl": pnl, "pct": pnl / staked * 100 if staked else 0.0, "mdd": mdd, "worst": worst}


def _rate(n, r) -> str:
    return f"{r / n * 100:.1f}%" if n else "—"


def _num(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def _ktag(k: int) -> str:
    return f"{k}+" if k == KMAX else str(k)


async def run_study(days: int | None = None, assets=None, intervals=("5m", "15m"), progress=None) -> dict:
    days = int(days or settings.SERIES_HISTORY_DAYS)
    assets = list(assets or settings.ASSETS)
    res = {"days": days, "assets": assets, "price": entry_price(), "intervals": {}, "raw": {}}
    for iv in intervals:
        step = INTERVALS[iv]
        per = {}
        for a in assets:
            if progress:
                progress(f"{iv} {a.upper()}")
            try:
                times, rets = await fetch_returns(a, iv, days)
            except Exception as exc:  # noqa: BLE001 — монеты нет / сеть: идём дальше
                log.warning("История %s %s: %s", a, iv, exc)
                continue
            if len(times) < 100:
                continue
            rs = runs(times, rets, step)
            mid = times[len(times) // 2]
            sims = {}
            for k in SIM_K:
                for m in SIM_M:
                    sims[(k, m)] = simulate(rs, k, m)
                    await asyncio.sleep(0)                 # не держим цикл бота
            per[a] = {"windows": len(times), "up": sum(1 for r in rets if r >= 0) / len(rets),
                      "from": times[0], "to": times[-1], "table": streak_table(rs),
                      "half1": streak_table(rs, t_to=mid), "half2": streak_table(rs, t_from=mid), "sims": sims}
            res["raw"][(iv, a)] = (times, rets)
        res["intervals"][iv] = per
    return res


def _pooled(per: dict, key: str) -> tuple[list[int], list[int]]:
    n, rev = [0] * (KMAX + 1), [0] * (KMAX + 1)
    for v in per.values():
        for k in range(1, KMAX + 1):
            n[k] += v[key][0][k]
            rev[k] += v[key][1][k]
    return n, rev


def summary_text(res: dict) -> str:
    be = breakeven(res["price"])
    lines = [f"📚 Серии UP/DOWN по свечам Binance за {res['days']} дн.",
             f"Ставка на разворот по ask {res['price']:.3f} с комиссией окупается, если разворот чаще "
             f"{be * 100:.1f}%. Исход по Binance совпадает с официальным примерно в 87% окон 5m."]
    for iv, per in res["intervals"].items():
        if not per:
            lines += ["", f"{iv}: данных нет"]
            continue
        total = sum(v["windows"] for v in per.values())
        up = sum(v["up"] * v["windows"] for v in per.values()) / total
        t0 = min(v["from"] for v in per.values())
        t1 = max(v["to"] for v in per.values())
        lines += ["", f"{iv} — {', '.join(a.upper() for a in per)}; окон {_num(total)} "
                  f"({time.strftime('%d.%m.%y', time.gmtime(t0))}–{time.strftime('%d.%m.%y', time.gmtime(t1))}), "
                  f"UP {up * 100:.1f}%"]
        n, rev = _pooled(per, "table")
        lines.append("Разворот после k одинаковых подряд, все монеты: " + " · ".join(
            f"k={_ktag(k)} {_rate(n[k], rev[k])}" for k in range(1, KMAX + 1)))
        nn, rr = sum(n[3:]), sum(rev[3:])
        n1, r1 = _pooled(per, "half1")
        n2, r2 = _pooled(per, "half2")
        lines.append(f"k≥3: {_rate(nn, rr)} из {_num(nn)} (1-я половина {_rate(sum(n1[3:]), sum(r1[3:]))}, "
                     f"2-я {_rate(sum(n2[3:]), sum(r2[3:]))})")
        if "btc" in per:
            bn, br = per["btc"]["table"]
            lines.append("BTC: " + " · ".join(f"k={_ktag(k)} {_rate(bn[k], br[k])}" for k in range(1, KMAX + 1)))
        plans = {}
        for v in per.values():
            for key, s in v["sims"].items():
                p = plans.setdefault(key, {"pnl": 0.0, "staked": 0.0})
                p["pnl"] += s["pnl"]
                p["staked"] += s["staked"]
        rated = sorted(((p["pnl"] / p["staked"] * 100 if p["staked"] else 0.0, key) for key, p in plans.items()),
                       reverse=True)
        plus = sum(1 for pct, _ in rated if pct > 0)
        best, worst = rated[0], rated[-1]
        lines.append(f"Удвоение (k — серия до входа, m — ставок в серии): в плюсе {plus} из {len(rated)} планов; "
                     f"лучший k={best[1][0]}, m={best[1][1]}: {best[0]:+.1f}% оборота; "
                     f"худший k={worst[1][0]}, m={worst[1][1]}: {worst[0]:+.1f}%")
    return "\n".join(lines)


def write_zip(res: dict, path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["interval", "asset", "part", "k", "n", "reversals", "rate"])
        for iv, per in res["intervals"].items():
            for a, v in per.items():
                for part in ("table", "half1", "half2"):
                    n, rev = v[part]
                    for k in range(1, KMAX + 1):
                        w.writerow([iv, a, {"table": "all", "half1": "first_half", "half2": "second_half"}[part],
                                    _ktag(k), n[k], rev[k], round(rev[k] / n[k], 4) if n[k] else ""])
        zf.writestr("streaks.csv", buf.getvalue())
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["interval", "asset", "k", "m", "series", "wins", "losses", "bets", "staked_units",
                    "pnl_units", "pnl_pct", "max_drawdown_units", "worst_series_units", "entry_price"])
        for iv, per in res["intervals"].items():
            for a, v in per.items():
                for (k, m), s in v["sims"].items():
                    w.writerow([iv, a, k, m, s["series"], s["wins"], s["losses"], s["bets"], round(s["staked"], 2),
                                round(s["pnl"], 2), round(s["pct"], 2), round(s["mdd"], 2), round(s["worst"], 2),
                                res["price"]])
        zf.writestr("plans.csv", buf.getvalue())
        for (iv, a), (times, rets) in res["raw"].items():
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["open_time", "ret_bps"])
            for t, r in zip(times, rets):
                w.writerow([t, round(r * 1e4, 2)])
            zf.writestr(f"candles_{iv}_{a}.csv", buf.getvalue())
    return path


async def run_and_send(days: int | None = None) -> None:
    """Кнопка 📚: посчитать и прислать итог и файл. Повторный запуск во время расчёта — игнор."""
    from src import telegram_notify
    if _state["running"]:
        return
    _state.update(running=True, started=time.time(), error="", progress="")
    path = None
    try:
        res = await run_study(days, progress=lambda s: _state.update(progress=s))
        text = summary_text(res)
        path = write_zip(res, os.path.join(settings.REPORTS_DIR,
                                           time.strftime("series_%Y%m%d-%H%M.zip", time.gmtime())))
        await telegram_notify.notify(text[:4000])
        await telegram_notify.send_document(path, "📚 Серии UP/DOWN: streaks.csv — развороты по k, plans.csv — "
                                                  "планы с удвоением, candles_*.csv — доходности окон")
    except Exception as exc:  # noqa: BLE001
        _state["error"] = f"{type(exc).__name__}: {exc}"[:300]
        log.warning("История серий не посчиталась: %s", exc)
        try:
            await telegram_notify.notify(f"⚠️ 📚 История серий не посчиталась: {_state['error']}")
        except Exception:  # noqa: BLE001
            pass
    finally:
        _state.update(running=False, finished=time.time())
        if path:
            try:
                os.remove(path)
            except OSError:
                pass


def start(days: int | None = None) -> bool:
    """Запустить расчёт в фоне. False — уже идёт."""
    if _state["running"]:
        return False
    task = asyncio.create_task(run_and_send(days))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return True


def status_text() -> str:
    if _state["running"]:
        return (f"⏳ Считаю историю серий ({_state['progress'] or 'старт'}), начал "
                f"{int(time.time() - _state['started'])} с назад — пришлю итог и файл.")
    return ""
