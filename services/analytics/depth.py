from typing import List
from statistics import mean
from core.ram_window import get_global
from core.processor import format_num, format_vol, ai_meta


def handle_depth(args: List[str], pair: str = 'USDT-COP') -> str:
    """Análisis de profundidad de mercado y deslizamiento (Slippage)."""
    rw = get_global()
    if not rw:
        return "⚠️ RAM no inicializada. Inicia el worker."

    with rw.lock:
        dq = rw.pair_index.get(pair)
        if not dq:
            return f"⚠️ No hay datos para {pair}"

        snap = dq[-1]
        buys = sorted([a for a in snap.ads if a.side == 'buy'],
                      key=lambda a: a.price, reverse=True)
        sells = sorted([a for a in snap.ads if a.side ==
                       'sell'], key=lambda a: a.price)

    def calculate_slippage(ads, target_usdt):
        total_vol = 0
        total_cost = 0
        if not ads:
            return None, 0

        for ad in ads:
            if total_vol >= target_usdt:
                break
            qty = min(ad.quantity, target_usdt - total_vol)
            total_vol += qty
            total_cost += qty * ad.price

        if total_vol < target_usdt:
            return None, total_vol
        avg_price = total_cost / total_vol
        top1_price = ads[0].price
        slippage = abs((avg_price / top1_price - 1) * 100)
        return avg_price, slippage

    amounts = [1000, 5000, 10000, 50000]

    currency = pair.split('-')[1] if '-' in pair else 'COP'
    lines = [
        f"🌊 <b>PROFUNDIDAD DE MERCADO</b> ({pair})",
        "",
        "<b>Simulación de Venta (Liquidando USDT):</b>",
        "<code>Monto     Precio Eff   Slippage</code>",
        "<code>------   -----------   --------</code>"
    ]

    for amt in amounts:
        avg_p, slip = calculate_slippage(sells, amt)
        if avg_p:
            lines.append(
                f"<code>{amt:>5}$   {format_num(avg_p, 0):>11}   {slip:>7.2f}%</code>")
        else:
            lines.append(
                f"<code>{amt:>5}$   Sin liquidez (Max: {slip:,.0f} USDT)</code>")

    lines.append("")
    lines.append("<b>Simulación de Compra (Obteniendo USDT):</b>")
    lines.append("<code>Monto     Precio Eff   Slippage</code>")
    lines.append("<code>------   -----------   --------</code>")

    for amt in amounts:
        avg_p, slip = calculate_slippage(buys, amt)
        if avg_p:
            lines.append(
                f"<code>{amt:>5}$   {format_num(avg_p, 0):>11}   {slip:>7.2f}%</code>")
        else:
            lines.append(
                f"<code>{amt:>5}$   Sin liquidez (Max: {slip:,.0f} USDT)</code>")

    lines.append(
        "\n💡 <i>El Slippage mide el costo extra de ejecutar una orden grande.</i>")

    meta = {
        "type": "depth_analysis",
        "pair": pair,
        "amounts": amounts
    }

    return "\n".join(lines) + ai_meta(meta)
