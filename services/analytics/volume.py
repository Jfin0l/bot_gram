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

    lines = [
        f"📊 <b>ANÁLISIS DE VOLUMEN</b> ({pair})",
        "",
        f"💰 <b>Liquidez Expuesta:</b>",
        f"• Compra: <b>{format_vol(vol_buy)} USDT</b>",
        f"• Venta: <b>{format_vol(vol_sell)} USDT</b>",
        f"• Total: <b>{format_vol(vol_buy + vol_sell)} USDT</b>",
        "",
        f"🔄 <b>Ratio B/S: {ratio:.2f}</b>",
        f"Sentimiento: Mercado en{trend}",
        "",
        "🎯 <b>Top Liquidez (Individual):</b>"
    ]

    # Mostrar top 3 anuncios por volumen
    all_ads = sorted(buys + sells, key=lambda a: a.quantity, reverse=True)
    for i, ad in enumerate(all_ads[:3], 1):
        side_label = "Compra" if ad.side == 'buy' else "Venta"
        lines.append(
            f"{i}. <code>@{ad.merchant[:10]}</code>: <b>{format_vol(ad.quantity)}</b> ({side_label})")

    meta = {
        "type": "volume_analysis",
        "vol_buy": vol_buy,
        "vol_sell": vol_sell,
        "ratio": ratio
    }

    return "\n".join(lines) + ai_meta(meta)
