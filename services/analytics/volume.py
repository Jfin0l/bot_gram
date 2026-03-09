from typing import List
from statistics import mean
from core.ram_window import get_global
from core.processor import format_vol, ai_meta
from datetime import datetime


def handle_volume(args: List[str], pair: str = 'USDT-COP') -> str:
    """Análisis de volumen y rotación (Smart Money)."""
    rw = get_global()
    if not rw:
        return "⚠️ RAM no inicializada. Inicia el worker."

    with rw.lock:
        dq = rw.pair_index.get(pair)
        if not dq:
            return f"⚠️ No hay datos para {pair}"

        snap = dq[-1]
        buys = [a for a in snap.ads if a.side == 'buy']
        sells = [a for a in snap.ads if a.side == 'sell']

        vol_buy = sum(a.quantity for a in buys)
        vol_sell = sum(a.quantity for a in sells)

    ratio = vol_buy / vol_sell if vol_sell > 0 else 0
    if ratio > 1.5:
        trend = " acumulación (Presión de Compra 📈)"
    elif ratio < 0.6:
        trend = " liquidación (Presión de Venta 📉)"
    else:
        trend = " equilibrio de mercado ⚖️"
    # Dominancia por Merchant (Market Share)
    merchant_vols = {}
    total_market_vol = vol_buy + vol_sell
    
    for ad in (buys + sells):
        m = ad.merchant
        merchant_vols[m] = merchant_vols.get(m, 0) + ad.quantity
    
    sorted_merchants = sorted(merchant_vols.items(), key=lambda x: x[1], reverse=True)

    lines = [
        f"📊 <b>ANÁLISIS DE VOLUMEN</b> ({pair})",
        "",
        f"💰 <b>Liquidez Expuesta:</b>",
        f"• Compra: <b>{format_vol(vol_buy)} USDT</b>",
        f"• Venta: <b>{format_vol(vol_sell)} USDT</b>",
        f"• Total: <b>{format_vol(total_market_vol)} USDT</b>",
        "",
        f"🔄 <b>Ratio B/S: {ratio:.2f}</b>",
        f"Sentimiento: Mercado en{trend}",
        "",
        "👑 <b>Dominancia de Mercado (Top 5):</b>"
    ]

    for i, (m, v) in enumerate(sorted_merchants[:5], 1):
        share = (v / total_market_vol * 100) if total_market_vol > 0 else 0
        lines.append(
            f"{i}. <code>@{m[:10]:<10}</code> <b>{format_vol(v):>8}</b> ({share:>4.1f}%)"
        )

    lines.append("\n🎯 <b>Ads con Mayor Liquidez:</b>")
    all_ads_sorted = sorted(buys + sells, key=lambda a: a.quantity, reverse=True)
    for i, ad in enumerate(all_ads_sorted[:3], 1):
        side_label = "BUY" if ad.side == 'buy' else "SELL"
        lines.append(
            f"• <code>@{ad.merchant[:10]:<10}</code> <b>{format_vol(ad.quantity):>8}</b> ({side_label})")

    meta = {
        "type": "volume_analysis",
        "vol_buy": vol_buy,
        "vol_sell": vol_sell,
        "ratio": ratio,
        "top_share": (sorted_merchants[0][1]/total_market_vol) if sorted_merchants and total_market_vol > 0 else 0
    }

    return "\n".join(lines) + ai_meta(meta)
