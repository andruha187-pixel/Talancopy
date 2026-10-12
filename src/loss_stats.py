"""
Честная оценка проигрышей: факт против того, что уже «заложено» в цены входа.

Цена на Polymarket — это и есть оценка рынком вероятности выигрыша. Проверено
на отчётах 18.09–06.10 (2491 рынок BTC 5m Binance-бота + 235 Chainlink-бота):
сторона, купленная по 0.90–0.95 за 1–3.5 минуты до конца, выигрывает почти
ровно с той вероятностью, которую показывает цена (расхождение ~0.5 п.п.).
Значит, при входе по 0.95 примерно 1 сделка из 20 проиграет «нормально», и
это не ошибка бота, а цена стратегии.

Отсюда простой и честный тест: сумма (1 − цена входа) по всем сделкам —
сколько проигрышей ожидать при честной цене. Если фактических заметно меньше,
отбор сигналов (safety score) даёт преимущество; если больше — преимущества
нет, и стратегию пора останавливать. Для оценки «случайность или нет» —
распределение Пуассона (проигрыши редкие и независимые).

Модуль без зависимостей от Telegram/БД — чистые функции, их легко тестировать.
"""
from __future__ import annotations
import math

MIN_TRADES_FOR_VERDICT = 20


def plural(n: int, one: str, few: str, many: str) -> str:
    """Русское согласование: 1 сделка, 3 сделки, 25 сделок."""
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def _trades(n: int) -> str:
    return f"{n} {plural(n, 'сделка', 'сделки', 'сделок')}"


def plural_trades(n: int) -> str:
    """«1 сделка», «3 сделки», «25 сделок»."""
    return _trades(n)


def _trades_gen(n: int) -> str:
    """«из N сделок» — родительный падеж: из 1 сделки, из 5 сделок."""
    return f"{n} {'сделки' if n % 10 == 1 and n % 100 != 11 else 'сделок'}"


def poisson_cdf(k: int, lam: float) -> float:
    """P(X <= k) для X ~ Poisson(lam)."""
    if k < 0:
        return 0.0
    if lam <= 0:
        return 1.0
    term = math.exp(-lam)
    total = term
    for i in range(1, k + 1):
        term *= lam / i
        total += term
    return min(1.0, total)


def verdict(trades: int, losses: int, expected: float) -> str:
    if trades < MIN_TRADES_FOR_VERDICT or expected <= 0:
        return f"мало сделок для вывода (нужно хотя бы {MIN_TRADES_FOR_VERDICT})"
    if losses <= expected:
        p = poisson_cdf(losses, expected)
        if p < 0.05:
            return f"проигрышей заметно меньше, чем заложено в цены: отбор сигналов работает (статистически значимо, p≈{p:.2f})"
        return f"проигрышей не больше, чем заложено в цены, но преимущество пока не доказано (может быть случайностью, p≈{p:.2f})"
    p = 1.0 - poisson_cdf(losses - 1, expected)
    if p < 0.05:
        return (f"проигрышей заметно больше, чем заложено в цены: преимущества нет, стратегию стоит "
                f"остановить и пересмотреть (статистически значимо, p≈{p:.2f})")
    return f"проигрышей не меньше, чем заложено в цены: преимущества не видно (p≈{p:.2f})"


def summary_lines(label: str, st: dict) -> list[str]:
    """Строки для 📊 Статистики. st — результат storage.get_outcome_stats()."""
    n = st.get("trades", 0)
    if not n:
        return [f"{label}: пока нет закрытых сделок"]
    lines = [
        f"{label}: {_trades(n)}, проигрышей {st['losses']}, "
        f"по ценам входа ожидалось ≈{st['expected_losses']:.1f}",
        f"  → {verdict(n, st['losses'], st['expected_losses'])}",
    ]
    if st.get("avg_win", 0) > 0 and st.get("avg_loss", 0) < 0:
        ratio = -st["avg_loss"] / st["avg_win"]
        lines.append(
            f"  средний выигрыш {st['avg_win']:+.2f}, средний проигрыш {st['avg_loss']:+.2f} "
            f"→ 1 проигрыш ≈ {ratio:.0f} {plural(round(ratio), 'выигрыш', 'выигрыша', 'выигрышей')}"
        )
    return lines


def short_line(label: str, st: dict) -> str:
    """Одна строка для подписи 4-часового отчёта."""
    n = st.get("trades", 0)
    if not n:
        return f"{label}: закрытых сделок пока нет"
    exp = st["expected_losses"]
    losses = st["losses"]
    mark = ""
    if n >= MIN_TRADES_FOR_VERDICT and exp > 0:
        if poisson_cdf(losses, exp) < 0.05:
            mark = " → заметно реже, чем заложено в цены"
        elif losses > 0 and 1.0 - poisson_cdf(losses - 1, exp) < 0.05:
            mark = " → заметно чаще, чем заложено в цены"
        else:
            mark = " → в пределах заложенного в цены"
    return f"{label}: {_trades(n)}, проигрышей {losses} (по ценам входа ожидалось ≈{exp:.1f}){mark}"


def loss_context(st: dict, entry_price: float, label: str | None = None) -> str:
    """Приписка к уведомлению о проигрыше: нормальный это проигрыш или нет.
    label — монета (и режим), если статистика посчитана по одной монете."""
    risk = max(0.0, 1.0 - float(entry_price)) * 100
    where = f" по {label}" if label else ""
    return (
        f"Цена входа {entry_price:.2f}: рынок закладывал ≈{risk:.0f}% шанс проигрыша. "
        f"Это {st['losses']}-й проигрыш из {_trades_gen(st['trades'])}{where} "
        f"(по ценам входа ожидалось ≈{st['expected_losses']:.1f})."
    )
