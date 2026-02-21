from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import List, Dict, Optional

from core.ram_window import get_global


def _cutoff(seconds: int = 3600):
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def handle_merchant(args: List[str], pair: str = 'USDT-COP') -> str:
    rw = get_global()
    if not rw:
        return "⚠️ RAM no inicializada"

    token = (" ".join(args)).strip() if args else 'top'

    # Top merchants by visible volume last 1 hour
    if token == 'top':
        cutoff = _cutoff(3600)
        volumes: Dict[str, float] = {}
        with rw.lock:
            for m, dq in rw.merchant_index.items():
                vol = 0.0
                for ts, ad in dq:
                    if ts < cutoff:
                        continue
                    vol += ad.quantity
                if vol > 0:
                    volumes[m] = volumes.get(m, 0.0) + vol

        items = sorted(volumes.items(), key=lambda x: x[1], reverse=True)[:10]
        if not items:
            return "⚠️ No hay merchants en la última hora."
        lines = ["🏷️ Top merchants por volumen (1h):"]
        for m, v in items:
            lines.append(f"• {m}: {v:.2f}")
        return "\n".join(lines)

    if token == 'bots':
        # suspicious merchants: high ad freq + low per-ad volume + repeated prices
        cutoff = _cutoff(3600)
        suspects = []
        with rw.lock:
            for m, dq in rw.merchant_index.items():
                count = 0
                vol = 0.0
                prices = []
                for ts, ad in dq:
                    if ts < cutoff:
                        continue
                    count += 1
                    vol += ad.quantity
                    prices.append(ad.price)
                if count >= 20 and (vol / max(1, count)) < 1.0:
                    uniq = len(set(prices))
                    if uniq < max(1, int(count * 0.6)):
                        suspects.append((m, count, vol))
        if not suspects:
            return "✅ No se detectaron merchants sospechosos (bots)"
        lines = ["🤖 Merchants sospechosos:"]
        for m, c, v in sorted(suspects, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"• {m}: ads={c} vol={v:.2f}")
        return "\n".join(lines)

    if token == 'grandes':
        cutoff = _cutoff(3600)
        bigs = []
        with rw.lock:
            for m, dq in rw.merchant_index.items():
                vol = 0.0
                count = 0
                for ts, ad in dq:
                    if ts < cutoff:
                        continue
                    vol += ad.quantity
                    count += 1
                if vol >= 100 and count >= 5:
                    bigs.append((m, vol, count))
        if not bigs:
            return "⚠️ No se encontraron merchants grandes"
        lines = ["🏦 Merchants grandes:"]
        for m, v, c in sorted(bigs, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"• {m}: vol={v:.2f} ads={c}")
        return "\n".join(lines)

    # specific merchant name
    name = token
    cutoff = _cutoff(3600)
    count = 0
    vol = 0.0
    prices = []
    positions = []
    with rw.lock:
        dq = rw.merchant_index.get(name, [])
        for ts, ad in dq:
            if ts < cutoff:
                continue
            count += 1
            vol += ad.quantity
            prices.append(ad.price)
            # compute position in latest snapshot if available
        # compute avg spread positioning across latest snapshot
        latest = None
        pdq = rw.pair_index.get(pair) or []
        if pdq:
            latest = pdq[-1]
        if latest:
            buys = [a for a in latest.ads if a.side == 'buy']
            sells = [a for a in latest.ads if a.side == 'sell']
            buys_sorted = sorted(buys, key=lambda a: a.price, reverse=True)
            sells_sorted = sorted(sells, key=lambda a: a.price)
            # find positions
            for i in range(min(len(buys_sorted), len(sells_sorted))):
                if buys_sorted[i].merchant == name or sells_sorted[i].merchant == name:
                    positions.append(i + 1)

    if count == 0:
        return f"⚠️ Merchant {name} sin actividad en la última hora"

    avg_price = mean(prices) if prices else 0
    price_std = pstdev(prices) if len(prices) > 1 else 0
    stability = 'ESTABLE' if price_std / (avg_price or 1) < 0.01 else ('MEDIA' if price_std / (avg_price or 1) < 0.03 else 'ALTA')
    activity_score = min(100, int((count / 20) * 50 + min(50, vol / 2)))

    lines = [f"👤 Merchant: {name}", f"Ads (1h): {count}", f"Vol visible: {vol:.2f}", f"Posiciones ejemplo: {positions[:5]}", f"Price std%: {(price_std / (avg_price or 1) * 100):.2f}%", f"Stabilidad precios: {stability}", f"Actividad (heur): {activity_score}/100"]
    return "\n".join(lines)
