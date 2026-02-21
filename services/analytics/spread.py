from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import Tuple, List, Optional

from core.ram_window import get_global
from core import db as core_db
from types import SimpleNamespace


def _get_latest_snapshot(pair: str):
    rw = get_global()
    if not rw:
        return None
    with rw.lock:
        dq = rw.pair_index.get(pair) or []
        if not dq:
            return None
        return dq[-1]


def _ordered_lists(snapshot):
    buys = [ad for ad in snapshot.ads if ad.side == 'buy']
    sells = [ad for ad in snapshot.ads if ad.side == 'sell']
    # buy: highest first; sell: lowest first
    buys_sorted = sorted(buys, key=lambda a: a.price, reverse=True)
    sells_sorted = sorted(sells, key=lambda a: a.price)
    return buys_sorted, sells_sorted


def _spread_from_pair(buy, sell) -> Optional[float]:
    try:
        return ((sell.price - buy.price) / buy.price) * 100
    except Exception:
        return None


def handle_spread(args: List[str], pair: str = 'USDT-COP') -> str:
    """Process /spread command arguments and return formatted message."""
    snap = _get_latest_snapshot(pair)
    if not snap:
        # attempt DB fallback: last stored snapshot summary
        last = core_db.get_latest_snapshot_for_pair(pair)
        if last:
            raw = last.get('raw', {})
            rows = raw.get('rows_fetched') or raw.get('rows') or raw.get('rows_count')
            avg_simple = raw.get('avg_price_simple') or raw.get('avg_price')
            spread_pct = raw.get('spread_pct')
            top1 = raw.get('top1_price')
            lines = [f"🗄️ No hay snapshot en RAM — mostrando última snapshot guardada ({last.get('timestamp_utc')})"]
            if rows is not None:
                lines.append(f"• Rows fetched: {rows}")
            if avg_simple is not None:
                try:
                    lines.append(f"• Avg price: {float(avg_simple):.4f}")
                except Exception:
                    lines.append(f"• Avg price: {avg_simple}")
            if spread_pct is not None:
                try:
                    lines.append(f"• Spread: {float(spread_pct):.2f}%")
                except Exception:
                    lines.append(f"• Spread: {spread_pct}")
            if top1 is not None:
                lines.append(f"• Top1 price: {top1}")
            lines.append("\nPara obtener datos en tiempo real, inicia el worker de ingest y espera a que RAM se llene.")
            return "\n".join(lines)
        return "⚠️ No hay snapshot disponible en RAM para {pair}. Inicia el worker para poblar RAM.".format(pair=pair)

    buys, sells = _ordered_lists(snap)
    if not buys or not sells:
        return "⚠️ Datos insuficientes en el snapshot." 

    token = (" ".join(args)).strip() if args else "top5"

    # Position-based
    if token.isdigit():
        idx = int(token) - 1
        if idx < 0 or idx >= len(buys) or idx >= len(sells):
            return f"⚠️ Posición {token} fuera de rango (hay {min(len(buys), len(sells))} posiciones)."
        sp = _spread_from_pair(buys[idx], sells[idx])
        vol = buys[idx].quantity + sells[idx].quantity
        return f"📈 Spread posición {token}: {sp:.2f}%\nVol visible: {vol:.2f}"

    if token.startswith("top") and token[3:].isdigit():
        n = int(token[3:])
        pairs = min(n, len(buys), len(sells))
        spreads = []
        vols = []
        for i in range(pairs):
            s = _spread_from_pair(buys[i], sells[i])
            if s is not None:
                spreads.append(s)
                vols.append(buys[i].quantity + sells[i].quantity)
        if not spreads:
            return "⚠️ No hay spreads calculables." 
        return f"📊 Top{n} — Spread medio: {mean(spreads):.2f}%\nVol medio: {mean(vols):.2f}"

    # Range like 10-20
    if '-' in token and token.replace('%', '').replace('>', '').count('-') == 1 and not token.endswith('%'):
        parts = token.split('-')
        try:
            a = int(parts[0]) - 1
            b = int(parts[1]) - 1
        except Exception:
            return "⚠️ Formato de rango inválido. Usa 10-20"
        a = max(0, a)
        b = min(b, min(len(buys), len(sells)) - 1)
        if a > b:
            return "⚠️ Rango inválido."
        spreads = []
        vols = []
        for i in range(a, b + 1):
            s = _spread_from_pair(buys[i], sells[i])
            if s is not None:
                spreads.append(s)
                vols.append(buys[i].quantity + sells[i].quantity)
        if not spreads:
            return "⚠️ No hay datos en ese rango." 
        return f"📊 Pos {a+1}-{b+1} — Spread medio: {mean(spreads):.2f}%\nVol medio: {mean(vols):.2f}"

    # Percentage-based 1% or 1-2%
    if token.endswith('%'):
        tok = token[:-1]
        if '-' in tok:
            pmin, pmax = tok.split('-')
            try:
                pmin = float(pmin)
                pmax = float(pmax)
            except Exception:
                return "⚠️ Formato % inválido"
            positions = []
            for i in range(min(len(buys), len(sells))):
                s = _spread_from_pair(buys[i], sells[i])
                if s is not None and pmin <= abs(s) <= pmax:
                    positions.append(i + 1)
            if not positions:
                return f"⚠️ Ninguna posición entre {pmin}% y {pmax}%"
            return f"✅ Posiciones en rango {pmin}%–{pmax}%: {positions[0]} ... {positions[-1]} (total {len(positions)})"
        else:
            try:
                p = float(tok)
            except Exception:
                return "⚠️ Formato % inválido"
            matches = []
            for i in range(min(len(buys), len(sells))):
                s = _spread_from_pair(buys[i], sells[i])
                if s is not None and abs(s) >= p:
                    matches.append(i + 1)
            if not matches:
                return f"⚠️ Ninguna posición >= {p}%"
            return f"✅ Posiciones >= {p}%: {matches[:10]} (total {len(matches)})"

    # Dynamic rotation >1.2
    if token.startswith('>'):
        try:
            thresh = float(token[1:])
        except Exception:
            return "⚠️ Formato inválido para operador '>'"
        # find first pos where absolute spread >= thresh
        first = None
        last = None
        spreads_all = []
        vols_all = []
        for i in range(min(len(buys), len(sells))):
            s = _spread_from_pair(buys[i], sells[i])
            if s is None:
                continue
            spreads_all.append(abs(s))
            vol = buys[i].quantity + sells[i].quantity
            vols_all.append(vol)
            if first is None and abs(s) >= thresh:
                first = i + 1
        if first is None:
            return f"⚠️ No se encontró posición con spread >= {thresh}%"
        upper = thresh + 0.3
        # find final position where spread <= upper starting from first
        final = first
        for i in range(first - 1, min(len(buys), len(sells))):
            s = _spread_from_pair(buys[i], sells[i])
            if s is None:
                continue
            if abs(s) <= upper:
                final = i + 1
        # analyze last 1 hour
        rw = get_global()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        spreads_history = []
        vols_history = []
        with rw.lock:
            dq = rw.pair_index.get(pair, [])
            for snap in reversed(dq):
                if snap.timestamp < cutoff:
                    break
                b_s, s_s = _ordered_lists(snap)
                # compute avg spread across pos range for this snapshot
                vals = []
                vvols = []
                for idx in range(first - 1, final):
                    if idx < len(b_s) and idx < len(s_s):
                        sp = _spread_from_pair(b_s[idx], s_s[idx])
                        if sp is not None:
                            vals.append(abs(sp))
                            vvols.append(b_s[idx].quantity + s_s[idx].quantity)
                if vals:
                    spreads_history.append(mean(vals))
                    vols_history.append(mean(vvols) if vvols else 0)

        avg_spread = mean(spreads_all) if spreads_all else 0
        avg_vol = mean(vols_all) if vols_all else 0
        hist_spread_avg = mean(spreads_history) if spreads_history else 0
        # variation
        var = 0
        try:
            var = pstdev(spreads_history) if len(spreads_history) > 1 else 0
        except Exception:
            var = 0
        # rotation indicator
        # rules: HIGH if var/avg_spread > 0.02 and avg_vol > 5
        stability = 'LOW'
        if avg_spread > 0 and var / (avg_spread or 1) > 0.02 and avg_vol > 5:
            stability = 'HIGH'
        elif avg_vol > 2 or var / (avg_spread or 1) > 0.01:
            stability = 'MEDIUM'

        return (
            f"🔎 Rotación {first}-{final}\n"
            f"Spread actual medio: {avg_spread:.2f}% (hist avg: {hist_spread_avg:.2f}%)\n"
            f"Vol visible medio (pos range): {avg_vol:.2f}\n"
            f"Variación spread (std): {var:.3f}\n"
            f"Rotation: {stability}"
        )

    return "⚠️ Comando no entendido. Ejemplos: /spread 10, /spread 10-20, /spread 1%, /spread >1.2"
