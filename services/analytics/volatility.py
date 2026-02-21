from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import List, Optional

from core.ram_window import get_global


def _collect_prices(pair: str, window_seconds: int) -> List[float]:
    rw = get_global()
    if not rw:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    prices = []
    with rw.lock:
        dq = rw.pair_index.get(pair) or []
        for snap in reversed(dq):
            if snap.timestamp < cutoff:
                break
            for ad in snap.ads:
                prices.append(ad.price)
    return prices


def handle_volatility(args: List[str], pair: str = 'USDT-COP') -> str:
    # parse window
    token = (" ".join(args)).strip() if args else ''
    if token in ('5m', '5'):
        seconds = 5 * 60
    elif token in ('15m', '15'):
        seconds = 15 * 60
    elif token in ('1h', '60') or token == '':
        seconds = 60 * 60
    elif token == 'shock':
        # perform shock detection: compare last two snapshots
        rw = get_global()
        if not rw:
            return "⚠️ RAM no inicializada"
        with rw.lock:
            dq = rw.pair_index.get(pair) or []
            if len(dq) < 2:
                return "⚠️ No hay suficientes snapshots para shock detection"
            last = dq[-1]
            prev = None
            # find previous snapshot at least 30s earlier
            for s in reversed(dq[:-1]):
                if (last.timestamp - s.timestamp).total_seconds() >= 15:
                    prev = s
                    break
            if not prev:
                prev = dq[-2]

        # compute avg prices
        def avg_price(snap):
            ps = [ad.price for ad in snap.ads]
            return mean(ps) if ps else 0

        a_last = avg_price(last)
        a_prev = avg_price(prev)
        if a_prev == 0:
            return "⚠️ Datos previos insuficientes"
        pct = (a_last - a_prev) / a_prev * 100
        sev = 'LOW'
        if abs(pct) >= 2.0:
            sev = 'HIGH'
        elif abs(pct) >= 1.0:
            sev = 'MEDIUM'
        return f"⚡ Shock detected: {pct:.2f}% in {(last.timestamp - prev.timestamp).total_seconds():.0f}s — Severity: {sev}"
    else:
        # default interpreted as minutes if numeric
        try:
            if token.endswith('m'):
                minutes = int(token[:-1])
                seconds = minutes * 60
            else:
                seconds = int(token) * 60
        except Exception:
            seconds = 60 * 60

    prices = _collect_prices(pair, seconds)
    if not prices:
        return "⚠️ No hay precios en la ventana solicitada"
    avg_p = mean(prices)
    std = pstdev(prices) if len(prices) > 1 else 0
    coef_var = (std / avg_p) if avg_p else 0
    pmax = max(prices)
    pmin = min(prices)

    # classification by coefficient of variation
    if coef_var < 0.005:
        cls = 'LOW'
    elif coef_var < 0.02:
        cls = 'MEDIUM'
    else:
        cls = 'HIGH'

    return (
        f"📈 Volatilidad ({seconds//60}m):\n"
        f"Std dev: {std:.4f}\n"
        f"Coef var: {coef_var:.4f}\n"
        f"Rango: {pmin:.4f} - {pmax:.4f}\n"
        f"Clasificación: {cls}"
    )
