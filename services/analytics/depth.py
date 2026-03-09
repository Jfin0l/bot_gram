from typing import List, Tuple
from statistics import mean
from core.ram_window import get_global
from core.processor import format_num, format_vol, ai_meta
from core import app_config


def handle_depth(args: List[str], pair: str = 'USDT-COP') -> str:
    """Análisis de profundidad de mercado, deslizamiento y muros de liquidez."""
    rw = get_global()
    if not rw:
        return "⚠️ RAM no inicializada. Inicia el worker."

    token = args[0].lower() if args else ""
    bank_filter = token if token and token != 'muro' else None

    with rw.lock:
        dq = rw.pair_index.get(pair)
        if not dq:
            return f"⚠️ No hay datos para {pair}"

        snap = dq[-1]
        
        # Aplicar filtro de banco si existe
        if bank_filter:
            filtered_ads = [a for a in snap.ads if bank_filter in (a.payment_method or '').lower()]
            if not filtered_ads:
                return f"⚠️ No hay anuncios activos para el banco: <b>{bank_filter.upper()}</b> en {pair}"
            target_ads = filtered_ads
        else:
            target_ads = snap.ads

        # BUY: Ascendente (quien menos paga a quien más paga)
        buys = sorted([a for a in target_ads if a.side == 'buy'],
                      key=lambda a: a.price)
        # SELL: Descendente (quien más cobra a quien menos cobra)
        sells = sorted([a for a in target_ads if a.side == 'sell'],
                       key=lambda a: a.price, reverse=True)

    def find_walls(ads, side):
        cfg = app_config.DETECTORS.get('depth', {})
        min_wall = cfg.get('wall_threshold_usdt', 25000.0)
        multiplier = cfg.get('wall_multiplier', 3.0)
        
        if not ads: return []
        
        avg_vol_top10 = mean([a.quantity for a in ads[:10]]) if len(ads) >= 5 else 1000
        walls = []
        for i, ad in enumerate(ads[:50]): # Buscamos en el top 50
            if ad.quantity >= min_wall or ad.quantity >= (avg_vol_top10 * multiplier):
                walls.append((i+1, ad))
        return walls

    # Caso específico de solo muros
    if token == 'muro':
        buy_walls = find_walls(buys, 'buy')
        sell_walls = find_walls(sells, 'sell')
        
        lines = [f"🧱 <b>MUROS DE LIQUIDEZ DETECTADOS</b> ({pair})", ""]
        
        if not buy_walls and not sell_walls:
            return f"✅ No se detectan muros de liquidez significativos en {pair} (Top 50)."

        if buy_walls:
            lines.append("🟢 <b>Muros en Compra (Soportes):</b>")
            for pos, ad in buy_walls:
                lines.append(f"• Pos #{pos}: <b>{format_vol(ad.quantity)} USDT</b> a {format_num(ad.price)}")
            lines.append("")

        if sell_walls:
            lines.append("🔴 <b>Muros en Venta (Resistencias):</b>")
            for pos, ad in sell_walls:
                lines.append(f"• Pos #{pos}: <b>{format_vol(ad.quantity)} USDT</b> a {format_num(ad.price)}")
        
        return "\n".join(lines)

    # Caso estándar con Slippage y Muros resumidos
    def calculate_slippage(ads, target_usdt):
        total_vol = 0
        total_cost = 0
        if not ads:
            return None, 0

        # Para el slippage necesitamos el orden inverso (los mejores precios primero para el usuario)
        # Usuario compra -> ve SELL ads (el mas barato primero)
        # Usuario vende -> ve BUY ads (el que mas paga primero)
        effective_ads = sorted(ads, key=lambda a: a.price, reverse=(ads[0].side == 'buy'))
        
        for ad in effective_ads:
            if total_vol >= target_usdt:
                break
            qty = min(ad.quantity, target_usdt - total_vol)
            total_vol += qty
            total_cost += qty * ad.price

        if total_vol < target_usdt:
            return None, total_vol
        avg_price = total_cost / total_vol
        top1_price = effective_ads[0].price
        slippage = abs((avg_price / top1_price - 1) * 100)
        return avg_price, slippage

    amounts = [1000, 5000, 10000, 50000]
    lines = [
        f"🌊 <b>PROFUNDIDAD Y SLIPPAGE</b> ({pair})",
        "",
        "<b>Liquidando USDT (Venta):</b>",
        "<code>Monto     Precio Eff   Slippage</code>"
    ]

    for amt in amounts:
        avg_p, slip = calculate_slippage(buys, amt)
        if avg_p:
            lines.append(f"<code>{amt:>5}$   {format_num(avg_p, 0):>11}   {slip:>7.2f}%</code>")
        else:
            lines.append(f"<code>{amt:>5}$   Sin liq. (Max: {slip:,.0f})</code>")

    lines.append("\n<b>Obteniendo USDT (Compra):</b>")
    lines.append("<code>Monto     Precio Eff   Slippage</code>")

    for amt in amounts:
        avg_p, slip = calculate_slippage(sells, amt)
        if avg_p:
            lines.append(f"<code>{amt:>5}$   {format_num(avg_p, 0):>11}   {slip:>7.2f}%</code>")
        else:
            lines.append(f"<code>{amt:>5}$   Sin liq. (Max: {slip:,.0f})</code>")

    # Resumen de muros
    buy_walls = find_walls(buys, 'buy')
    sell_walls = find_walls(sells, 'sell')
    if buy_walls or sell_walls:
        lines.append("\n🧱 <b>Muros detectados:</b>")
        if buy_walls: lines.append(f"• {len(buy_walls)} Soportes (BUY)")
        if sell_walls: lines.append(f"• {len(sell_walls)} Resistencias (SELL)")
        lines.append("<i>Usa <code>/depth muro</code> para ver detalles.</i>")

    return "\n".join(lines) + ai_meta({"type": "depth_analysis", "pair": pair})
