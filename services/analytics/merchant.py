from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from core.ram_window import get_global


def _cutoff(seconds: int = 3600):
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def _search_merchants(query: str, limit: int = 10) -> List[str]:
    """Busca merchants por nombre parcial (case insensitive)."""
    rw = get_global()
    if not rw:
        return []
    
    query = query.lower().lstrip('@')
    matches = []
    
    with rw.lock:
        for merchant in rw.merchant_index.keys():
            if query in merchant.lower():
                matches.append(merchant)
    
    return sorted(matches)[:limit]


def _top_merchants(pair: str, side: Optional[str] = None, limit: int = 10) -> List[Tuple]:
    rw = get_global()
    if not rw:
        return []
    
    cutoff = _cutoff(3600)
    merchant_stats = defaultdict(lambda: {'vol_usdt': 0.0, 'sum_price': 0.0, 'count': 0, 'side': None})
    
    with rw.lock:
        for merchant, ads_list in rw.merchant_index.items():
            for ts, ad in ads_list:
                if ts < cutoff:
                    continue
                
                if side and ad.side != side:
                    continue
                
                # CORRECCIÓN: quantity está en moneda local, dividir por precio para obtener USDT
                usdt_volume = ad.quantity / ad.price if ad.price > 0 else 0
                
                stats = merchant_stats[merchant]
                stats['vol_usdt'] += usdt_volume
                stats['sum_price'] += ad.price
                stats['count'] += 1
                stats['side'] = ad.side
    
    results = []
    for merchant, stats in merchant_stats.items():
        if stats['count'] > 0:
            avg_price = stats['sum_price'] / stats['count']
            results.append((
                merchant,
                stats['vol_usdt'],  # ← AHORA EN USDT REAL
                avg_price,
                stats['side'],
                stats['count']
            ))
    
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:limit]


def _format_merchant_list(merchants: List[Tuple], title: str, side_desc: str = "") -> str:
    """
    Formatea una lista de merchants para mostrar en Telegram.
    
    Args:f"• **Anuncios publicados (1h):** {count}"  # ← MÁS CLARO
        merchants: Lista de tuplas (merchant, volumen, precio_promedio, lado, conteo)
        title: Título de la sección
        side_desc: Descripción del lado (ej. "COMPRADORES")
    
    Returns:
        String formateado con emojis y columnas alineadas
    """
    if not merchants:
        return f"⚠️ No hay datos de merchants en la última hora."
    
    lines = [f"🏆 **{title}**"]
    
    if side_desc:
        lines.append(f"📊 {side_desc}\n")
    
    for idx, (m, vol, avg_price, side, count) in enumerate(merchants, 1):
        # Determinar emoji según el rango
        if idx == 1:
            rank_emoji = "🥇"
        elif idx == 2:
            rank_emoji = "🥈"
        elif idx == 3:
            rank_emoji = "🥉"
        else:
            rank_emoji = f"{idx}."
        
        # Formatear línea
        lines.append(
            f"{rank_emoji} `{m:<20}` "
            f"Vol: {vol:>8.2f} USDT  "
            f"Precio: {avg_price:>7.2f}  "
            f"Ops: {count:>3}"
        )
    
    # Añadir leyenda
    lines.append("\n💡 *Volumen total visible en la última hora*")
    
    return "\n".join(lines)


def handle_merchant(args: List[str], pair: str = 'USDT-COP') -> str:
    """
    Procesa comandos /merchant.
    
    Formatos soportados:
    - /merchant          : Top 10 global (buy + sell)
    - /merchant top      : Top 10 global (alias)
    - /merchant buy      : Top 10 compradores
    - /merchant sell     : Top 10 vendedores
    - /merchant bots     : Detecta posibles bots
    - /merchant grandes  : Merchants con alto volumen
    - /merchant <nombre> : Perfil del merchant específico
    """
    token = (" ".join(args)).strip().lower() if args else 'top'

    # Definir comandos válidos SIN parámetros
    comandos_base = ['top', 'buy', 'sell', 'bots', 'grandes']

    # Verificar si es un comando válido
    if token not in comandos_base and not token.startswith('@') and not token.startswith('search ') and token != '':
        return (
        f"⚠️ Comando `/merchant {token}` no reconocido.\n\n"
        f"**Comandos disponibles:**\n"
        f"• `/merchant` - Top 10 global\n"
        f"• `/merchant buy` - Top compradores\n"
        f"• `/merchant sell` - Top vendedores\n"
        f"• `/merchant bots` - Detectar bots\n"
        f"• `/merchant grandes` - Volumen destacado\n"
        f"• `/merchant search <texto>` - Buscar merchants\n"
        f"• `/merchant @usuario` - Perfil de merchant"
        )
    
    
    # ===========================================
    # CASO 1: Top global (todos los merchants)
    # ===========================================
    if token in ['top', '']:
        merchants = _top_merchants(pair, side=None, limit=10)
        return _format_merchant_list(
            merchants,
            "TOP MERCHANTS GLOBAL",
            "Compra + Venta"
        )
    
    # ===========================================
    # CASO 2: Top compradores
    # ===========================================
    if token == 'buy':
        merchants = _top_merchants(pair, side='buy', limit=10)
        return _format_merchant_list(
            merchants,
            "TOP COMPRADORES",
            "Quienes mejor pagan"
        )
    
    # ===========================================
    # CASO 3: Top vendedores
    # ===========================================
    if token == 'sell':
        merchants = _top_merchants(pair, side='sell', limit=10)
        return _format_merchant_list(
            merchants,
            "TOP VENDEDORES",
            "Quienes mejor venden"
        )
    
    # ===========================================
    # CASO 4: Detección de bots (CORREGIDO)
    # ===========================================
    if token == 'bots':
        rw = get_global()  # ← DEFINIDA AQUÍ
        if not rw:
            return "⚠️ RAM no inicializada"
            
        cutoff = _cutoff(3600)
        suspects = []
        
        with rw.lock:
            for m, dq in rw.merchant_index.items():
                count = 0
                vol = 0.0
                prices = []
                sides = set()
                
                for ts, ad in dq:
                    if ts < cutoff:
                        continue
                    count += 1
                    vol += ad.quantity  # ← EN USDT
                    prices.append(ad.price)
                    sides.add(ad.side)
                
                # Criterios para detectar bots:
                # 1. Alta frecuencia (>20 ads/hora)
                # 2. Bajo volumen por ad (<1.0 USDT)
                # 3. Poca variedad de precios
                # 4. Opera en ambos lados (compra y venta)
                if count >= 20:
                    avg_vol_per_ad = vol / max(1, count)
                    price_variety = len(set(prices)) / max(1, count)
                    operates_both_sides = len(sides) == 2
                    
                    if (avg_vol_per_ad < 1.0 and price_variety < 0.3) or operates_both_sides:
                        suspects.append((m, count, vol, avg_vol_per_ad))
        
        if not suspects:
            return "✅ No se detectaron merchants con comportamiento de bot"
        
        lines = ["🤖 **POSIBLES BOTS DETECTADOS**", ""]
        for m, c, v, avg in sorted(suspects, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"• `{m}`: {c} ads | {v:.1f} USDT total | {avg:.2f} USDT/ad")
        
        lines.append("\n💡 *Bots típicamente: alta frecuencia + bajo volumen por ad*")
        return "\n".join(lines)
    
    # ===========================================
    # CASO 5: Merchants grandes (CORREGIDO)
    # ===========================================
    if token == 'grandes':
        rw = get_global()  # ← DEFINIDA AQUÍ
        if not rw:
            return "⚠️ RAM no inicializada"
            
        cutoff = _cutoff(3600)
        bigs = []
        
        with rw.lock:
            for m, dq in rw.merchant_index.items():
                vol = 0.0
                count = 0
                for ts, ad in dq:
                    if ts < cutoff:
                        continue
                    vol += ad.quantity  # ← EN USDT
                    count += 1
                
                # Criterio: volumen >= 1000 USDT o ads >= 10
                if vol >= 1000 or count >= 10:
                    bigs.append((m, vol, count))
        
        if not bigs:
            return "⚠️ No se encontraron merchants grandes en la última hora"
        
        lines = ["🏦 **MERCHANTS DESTACADOS**", ""]
        for m, v, c in sorted(bigs, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"• `{m}`: Vol: {v:>8.2f} USDT | Ads: {c:>3}")
        
        return "\n".join(lines)
    
    
    # ===========================================
    # CASO 6: Perfil de merchant específico (CORREGIDO)
    # ===========================================
    # Si llegamos aquí, se trata de un nombre de merchant
    if token.startswith('@'):
        name = token[1:].strip()
    rw = get_global()  # ← DEFINIDA AQUÍ
    if not rw:
        return "⚠️ RAM no inicializada"
        
    cutoff = _cutoff(3600)
    count = 0
    vol = 0.0
    prices = []
    sides = []
    
    with rw.lock:
        dq = rw.merchant_index.get(name, [])
        for ts, ad in dq:
            if ts < cutoff:
                continue
            count += 1
            vol += ad.quantity  # ← EN USDT
            prices.append(ad.price)
            sides.append(ad.side)
    
    if count == 0:
        return f"⚠️ Merchant `{name}` sin actividad en la última hora"
    
    # Calcular métricas
    avg_price = mean(prices) if prices else 0
    price_std = pstdev(prices) if len(prices) > 1 else 0
    price_variation = (price_std / (avg_price or 1)) * 100
    
    # Determinar estabilidad de precios
    if price_variation < 1.0:
        stability = "🔵 MUY ESTABLE"
    elif price_variation < 3.0:
        stability = "🟢 ESTABLE"
    elif price_variation < 6.0:
        stability = "🟡 VARIABLE"
    else:
        stability = "🔴 MUY VARIABLE"
    
    # Calcular score de actividad (0-100)
    activity_score = min(100, int((count / 30) * 50 + min(50, vol / 20)))
    
    # Determinar si opera en ambos lados
    unique_sides = set(sides)
    if len(unique_sides) == 2:
        role = "🔄 Compra y Vende"
    elif 'buy' in unique_sides:
        role = "⬆️ Solo Compra"
    else:
        role = "⬇️ Solo Vende"
    
    # Construir mensaje
    lines = [
        f"👤 **PERFIL DE MERCHANT**",
        f"📛 Nombre: `{name}`",
        f"🎭 Rol: {role}",
        "",
        f"📊 **Actividad (última hora)**",
        f"• **Anuncios publicados (1h):** {count}",
        f"• Volumen total: {vol:.2f} USDT",  
        f"• Volumen promedio por ad: {(vol/count):.2f} USDT",
        "",
        f"💰 **Precios**",
        f"• Precio promedio: {avg_price:.2f} COP" if 'COP' in pair else f"• Precio promedio: {avg_price:.2f} VES",
        f"• Variación: {price_variation:.2f}%",
        f"• Estabilidad: {stability}",
        "",
        f"📈 **Score de actividad: {activity_score}/100**"
    ]
    
    # Buscar posición actual (si está activo ahora)
    with rw.lock:
        latest_pair = rw.pair_index.get(pair)
        if latest_pair and latest_pair:
            latest_snap = latest_pair[-1]
            buys = [a for a in latest_snap.ads if a.side == 'buy']
            sells = [a for a in latest_snap.ads if a.side == 'sell']
            
            # Buscar en compradores
            for i, ad in enumerate(sorted(buys, key=lambda a: a.price, reverse=True), 1):
                if ad.merchant == name:
                    lines.append(f"• Posición actual (compra): #{i}")
                    break
            
            # Buscar en vendedores
            for i, ad in enumerate(sorted(sells, key=lambda a: a.price), 1):
                if ad.merchant == name:
                    lines.append(f"• Posición actual (venta): #{i}")
                    break
    
        return "\n".join(lines)

    #==================================================================
    #Caso 7: busqueda por nombre parcial
    #==================================================================

    if token.startswith('search '):
        query = token[7:].strip()
        if len(query) < 3:
            return "⚠️ Escribe al menos 3 caracteres para buscar."
    
        matches = _search_merchants(query)
    
        if not matches:
            return f"⚠️ No hay merchants que contengan '{query}'"
    
        lines = [f"🔍 **Merchants que contienen '{query}'**", ""]
        for m in matches:
            lines.append(f"• `{m}`")
    
        lines.append("\n💡 Usa `/merchant @nombre` para ver perfil completo")

    return "\n".join(lines)




'''
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
'''