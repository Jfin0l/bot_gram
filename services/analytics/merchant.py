from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import sqlite3

from core.ram_window import get_global
from core.db import DB_PATH


def _cutoff(seconds: int = 3600):
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


def _fetch_merchant_stats(merchant_name: str, pair: str) -> Optional[dict]:
    """Obtiene estadísticas completas de un merchant."""
    clean_name = merchant_name.lstrip('@')

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Estadísticas de 24h
    c.execute("""
        SELECT 
            SUM(volume_usdt) as vol_24h,
            AVG(avg_price) as avg_price,
            SUM(ad_count) as total_ops,
            COUNT(DISTINCT hour) as horas_activas,
            AVG(avg_price) as precio_promedio,
            SUM(CASE WHEN side='buy' THEN volume_usdt ELSE 0 END) as vol_compra,
            SUM(CASE WHEN side='sell' THEN volume_usdt ELSE 0 END) as vol_venta,
            AVG(CASE WHEN side='buy' THEN avg_price ELSE NULL END) as precio_compra,
            AVG(CASE WHEN side='sell' THEN avg_price ELSE NULL END) as precio_venta
        FROM merchant_stats
        WHERE merchant = ? AND pair = ? 
        AND date = ?  -- Solo hoy
    """, (clean_name, pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

    row_24h = c.fetchone()

    # Estadísticas de 7 días
    c.execute("""
        SELECT 
            SUM(volume_usdt) as vol_7d,
            AVG(avg_price) as avg_price_7d,
            COUNT(DISTINCT date) as dias_activos
        FROM merchant_stats
        WHERE merchant = ? AND pair = ?
        AND date >= date('now', '-7 days')
    """, (clean_name, pair))

    row_7d = c.fetchone()

    # Calcular spread promedio (diferencia entre buy y sell)
    c.execute("""
        SELECT 
            AVG(CASE WHEN side='buy' THEN avg_price ELSE NULL END) as buy_avg,
            AVG(CASE WHEN side='sell' THEN avg_price ELSE NULL END) as sell_avg
        FROM merchant_stats
        WHERE merchant = ? AND pair = ?
        AND date = ?
    """, (clean_name, pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

    row_spread = c.fetchone()

    conn.close()

    if not row_24h or not row_24h[0]:
        return None

    # Calcular spread
    buy_price = row_spread[0] if row_spread and row_spread[0] else 0
    sell_price = row_spread[1] if row_spread and row_spread[1] else 0
    spread_pct = ((buy_price - sell_price) / sell_price *
                  100) if sell_price > 0 else 0

    # Determinar confiabilidad
    ops_24h = row_24h[2] or 0
    horas_activas = row_24h[3] or 0
    vol_24h = row_24h[0] or 0

    if ops_24h > 50 and horas_activas > 8 and vol_24h > 10000:
        confiabilidad = "🟢 ALTA"
    elif ops_24h > 20 and horas_activas > 4 and vol_24h > 5000:
        confiabilidad = "🟡 MEDIA"
    else:
        confiabilidad = "🔴 BAJA"

    # Determinar horario pico (requiere consulta adicional)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT hour, SUM(volume_usdt) as vol
        FROM merchant_stats
        WHERE merchant = ? AND pair = ? AND date = ?
        GROUP BY hour
        ORDER BY vol DESC
        LIMIT 1
    """, (clean_name, pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

    hora_pico = c.fetchone()
    conn.close()

    hora_pico_str = f"{hora_pico[0]}:00" if hora_pico else "N/D"

    return {
        'merchant': clean_name,
        'vol_24h': row_24h[0] or 0,
        'vol_7d': row_7d[0] if row_7d and row_7d[0] else 0,
        'avg_price': row_24h[1] or 0,
        'ops_24h': row_24h[2] or 0,
        'horas_activas': row_24h[3] or 0,
        'vol_compra': row_24h[5] or 0,
        'vol_venta': row_24h[6] or 0,
        'precio_compra': row_24h[7] or 0,
        'precio_venta': row_24h[8] or 0,
        'spread_pct': spread_pct,
        'confiabilidad': confiabilidad,
        'hora_pico': hora_pico_str,
        'dias_activos': row_7d[2] if row_7d and row_7d[2] else 0
    }


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
    merchant_stats = defaultdict(
        lambda: {'vol_usdt': 0.0, 'sum_price': 0.0, 'count': 0, 'side': None})

    with rw.lock:
        for merchant, ads_list in rw.merchant_index.items():
            for ts, ad in ads_list:
                if ts < cutoff:
                    continue

                if side and ad.side != side:
                    continue

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
                stats['vol_usdt'],
                avg_price,
                stats['side'],
                stats['count']
            ))

    results.sort(key=lambda x: x[1], reverse=True)
    return results[:limit]


def _format_merchant_list(merchants: List[Tuple], title: str, side_desc: str = "") -> str:
    """Formatea una lista de merchants para mostrar en Telegram."""
    if not merchants:
        return "⚠️ No hay datos de merchants en la ultima hora."

    lines = [f"🏆 **{title}**"]

    if side_desc:
        lines.append(f"📊 {side_desc}\n")

    for idx, (m, vol, avg_price, side, count) in enumerate(merchants, 1):
        if idx == 1:
            rank_emoji = "🥇"
        elif idx == 2:
            rank_emoji = "🥈"
        elif idx == 3:
            rank_emoji = "🥉"
        else:
            rank_emoji = f"{idx}."

        lines.append(
            f"{rank_emoji} `{m:<20}` "
            f"Vol: {vol:>8.2f} USDT  "
            f"Precio: {avg_price:>7.2f}  "
            f"Ops: {count:>3}"
        )

    lines.append("\n💡 *Volumen total visible en la ultima hora*")

    return "\n".join(lines)


def _build_merchant_profile(name: str, pair: str) -> str:
    """Construye el perfil detallado de un merchant especifico."""
    rw = get_global()
    if not rw:
        return "⚠️ RAM no inicializada"

    cutoff = _cutoff(3600)
    count = 0
    vol = 0.0
    prices = []
    sides = []

    with rw.lock:
        # Case-insensitive lookup: find the actual key matching the name
        actual_name = None
        for key in rw.merchant_index:
            if key.lower() == name.lower():
                actual_name = key
                break
        if actual_name:
            name = actual_name  # use the correctly-cased name for display
        dq = rw.merchant_index.get(name, [])
        for ts, ad in dq:
            if ts < cutoff:
                continue
            count += 1
            vol += ad.quantity / ad.price if ad.price > 0 else 0
            prices.append(ad.price)
            sides.append(ad.side)

    if count == 0:
        return f"⚠️ Merchant `{name}` sin actividad en la ultima hora"

    avg_price = mean(prices) if prices else 0
    price_std = pstdev(prices) if len(prices) > 1 else 0
    price_variation = (price_std / (avg_price or 1)) * 100

    if price_variation < 1.0:
        stability = "🔵 MUY ESTABLE"
    elif price_variation < 3.0:
        stability = "🟢 ESTABLE"
    elif price_variation < 6.0:
        stability = "🟡 VARIABLE"
    else:
        stability = "🔴 MUY VARIABLE"

    activity_score = min(100, int((count / 30) * 50 + min(50, vol / 20)))

    unique_sides = set(sides)
    if len(unique_sides) == 2:
        role = "🔄 Compra y Vende"
    elif 'buy' in unique_sides:
        role = "⬆️ Solo Compra"
    else:
        role = "⬇️ Solo Vende"

    currency = "COP" if "COP" in pair else "VES"
    lines = [
        "👤 **PERFIL DE MERCHANT**",
        f"📛 Nombre: `{name}`",
        f"🎭 Rol: {role}",
        "",
        "📊 **Actividad (ultima hora)**",
        f"• **Anuncios publicados (1h):** {count}",
        f"• Volumen total: {vol:.2f} USDT",
        f"• Volumen promedio por ad: {(vol / count):.2f} USDT",
        "",
        "💰 **Precios**",
        f"• Precio promedio: {avg_price:.2f} {currency}",
        f"• Variacion: {price_variation:.2f}%",
        f"• Estabilidad: {stability}",
        "",
        f"📈 **Score de actividad: {activity_score}/100**",
    ]

    with rw.lock:
        latest_pair = rw.pair_index.get(pair)
        if latest_pair:
            latest_snap = latest_pair[-1]
            buys = [a for a in latest_snap.ads if a.side == 'buy']
            sells = [a for a in latest_snap.ads if a.side == 'sell']

            for i, ad in enumerate(sorted(buys, key=lambda a: a.price, reverse=True), 1):
                if ad.merchant == name:
                    lines.append(f"• Posicion actual (compra): #{i}")
                    break

            for i, ad in enumerate(sorted(sells, key=lambda a: a.price), 1):
                if ad.merchant == name:
                    lines.append(f"• Posicion actual (venta): #{i}")
                    break

    return "\n".join(lines)


def handle_merchant(args: List[str], pair: str = 'USDT-COP') -> str:
    """
    Procesa comandos /merchant.

    Formatos soportados:
    - /merchant              : Top 10 global (buy + sell)
    - /merchant top          : Top 10 global (alias)
    - /merchant buy          : Top 10 compradores
    - /merchant sell         : Top 10 vendedores
    - /merchant bots         : Detecta posibles bots
    - /merchant grandes      : Merchants con alto volumen
    - /merchant estables     : Merchants con spread consistente
    - /merchant rapidos      : Merchants con alta frecuencia de operaciones
    - /merchant search <txt> : Buscar merchants por nombre parcial
    - /merchant @<nombre>    : Perfil del merchant especifico
    """
    raw_token = (" ".join(args)).strip() if args else 'top'
    token = raw_token.lower()

    # ===========================================
    # CASO 1: Top global
    # ===========================================
    if token in ('top', ''):
        merchants = _top_merchants(pair, side=None, limit=10)
        return _format_merchant_list(merchants, "TOP MERCHANTS GLOBAL", "Compra + Venta")

    # ===========================================
    # CASO 2: Top compradores
    # ===========================================
    if token == 'buy':
        merchants = _top_merchants(pair, side='buy', limit=10)
        return _format_merchant_list(merchants, "TOP COMPRADORES", "Quienes mejor pagan")

    # ===========================================
    # CASO 3: Top vendedores
    # ===========================================
    if token == 'sell':
        merchants = _top_merchants(pair, side='sell', limit=10)
        return _format_merchant_list(merchants, "TOP VENDEDORES", "Quienes mejor venden")

    # ===========================================
    # CASO 4: Deteccion de bots
    # ===========================================
    if token == 'bots':
        rw = get_global()
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
                    vol += ad.quantity / ad.price if ad.price > 0 else 0
                    prices.append(ad.price)
                    sides.add(ad.side)

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
            lines.append(
                f"• `{m}`: {c} ads | {v:.1f} USDT total | {avg:.2f} USDT/ad")

        lines.append(
            "\n💡 *Bots tipicamente: alta frecuencia + bajo volumen por ad*")
        return "\n".join(lines)

    # ===========================================
    # CASO 5: Merchants grandes
    # ===========================================
    if token == 'grandes':
        rw = get_global()
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
                    vol += ad.quantity / ad.price if ad.price > 0 else 0
                    count += 1

                if vol >= 100 or count >= 10:
                    bigs.append((m, vol, count))

        if not bigs:
            return "⚠️ No se encontraron merchants grandes en la ultima hora"

        lines = ["🏦 **MERCHANTS DESTACADOS**", ""]
        for m, v, c in sorted(bigs, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(f"• `{m}`: Vol: {v:>8.2f} USDT | Ads: {c:>3}")

        return "\n".join(lines)

    # ===========================================
    # CASO 6: Busqueda por nombre parcial
    # ===========================================
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

    # ===========================================
    # CASO 8: Merchants estables
    # ===========================================
    if token == 'estables':
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            SELECT 
                merchant,
                AVG(avg_price) as precio_promedio,
                COUNT(DISTINCT hour) as horas_activas,
                SUM(volume_usdt) as volumen_total,
                AVG(CASE WHEN side='buy' THEN avg_price ELSE NULL END) as buy_avg,
                AVG(CASE WHEN side='sell' THEN avg_price ELSE NULL END) as sell_avg,
                COUNT(*) as muestras
            FROM merchant_stats
            WHERE pair = ? AND date = ?
            GROUP BY merchant
            HAVING muestras >= 4
            ORDER BY (buy_avg - sell_avg) DESC
            LIMIT 10
        """, (pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

        rows = c.fetchall()
        conn.close()

        if not rows:
            return "⚠️ No hay suficientes datos de merchants estables hoy."

        lines = ["📊 **MERCHANTS ESTABLES (hoy)**", ""]
        lines.append("` #  Merchant      Spread   Volumen  `")
        lines.append("`---  ----------  -------  ---------`")

        for idx, row in enumerate(rows, 1):
            merchant = row[0][:10]
            spread = ((row[4] or 0) - (row[5] or 0)) / (row[5] or 1) * 100
            volumen = row[3] or 0

            lines.append(
                f"`{idx:2d}  @{merchant:<10}  {spread:>5.2f}%  {volumen:>8.0f}`"
            )

        lines.append("")
        lines.append("💡 *Estables = spread consistente durante el día*")

        return "\n".join(lines)

    # ===========================================
    # CASO 9: Merchants rapidos
    # ===========================================
    if token == 'rapidos':
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            SELECT 
                merchant,
                SUM(ad_count) as total_ops,
                SUM(volume_usdt) as volumen_total,
                COUNT(DISTINCT hour) as horas_activas,
                AVG(ad_count) as ops_por_hora
            FROM merchant_stats
            WHERE pair = ? AND date = ?
            GROUP BY merchant
            HAVING horas_activas >= 3
            ORDER BY ops_por_hora DESC
            LIMIT 10
        """, (pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

        rows = c.fetchall()
        conn.close()

        if not rows:
            return "⚠️ No hay suficientes datos de merchants rápidos hoy."

        lines = ["⚡ **MERCHANTS RÁPIDOS (hoy)**", ""]
        lines.append("` #  Merchant      Ops/h  Vol/hora `")
        lines.append("`---  ----------  -----  ---------`")

        for idx, row in enumerate(rows, 1):
            merchant = row[0][:10]
            ops_por_hora = row[4] or 0
            volumen_hora = (row[2] or 0) / (row[3] or 1)

            lines.append(
                f"`{idx:2d}  @{merchant:<10}  {ops_por_hora:>4.0f}   {volumen_hora:>8.0f}`"
            )

        lines.append("")
        lines.append("💡 *Rápidos = alta frecuencia de operaciones por hora*")

        return "\n".join(lines)

    # ===========================================
    # CASO 7: Perfil de merchant especifico (@nombre o nombre directo)
    # ===========================================
    if token.startswith('@'):
        name = raw_token[1:].strip()
    else:
        name = raw_token.strip()

    if not name:
        return (
            "⚠️ Comando no reconocido.\n\n"
            "**Comandos disponibles:**\n"
            "• `/merchant` - Top 10 global\n"
            "• `/merchant buy` - Top compradores\n"
            "• `/merchant sell` - Top vendedores\n"
            "• `/merchant bots` - Detectar bots\n"
            "• `/merchant grandes` - Volumen destacado\n"
            "• `/merchant estables` - Spread consistente\n"
            "• `/merchant rapidos` - Alta frecuencia de operaciones\n"
            "• `/merchant search <texto>` - Buscar merchants\n"
            "• `/merchant @usuario` - Perfil de merchant"
        )

    stats = _fetch_merchant_stats(name, pair)

    if not stats:
        # Fallback a la version de RAM si es muy reciente y no hay stats en BD hoy
        ram_profile = _build_merchant_profile(name, pair)
        if "sin actividad" not in ram_profile.lower() and "⚠️ ram" not in ram_profile.lower():
            return ram_profile
        return f"⚠️ Merchant `{name}` sin actividad en las últimas 24h.\n💡 Los datos se actualizan periódicamenta."

    # Formatear números
    vol_24h_str = f"{stats['vol_24h']:,.0f}".replace(',', '.')
    vol_7d_str = f"{stats['vol_7d']:,.0f}".replace(',', '.')

    # Determinar emoji según volumen
    if stats['vol_24h'] > 20000:
        vol_emoji = "🐋"  # Ballena
    elif stats['vol_24h'] > 5000:
        vol_emoji = "🐬"  # Delfín
    else:
        vol_emoji = "🐟"  # Pez

    lines = [
        f"👤 **PERFIL: @{stats['merchant']}** ({pair})",
        f"• {vol_emoji} Volumen 24h: **{vol_24h_str} USD**",
        f"• 📅 Volumen 7d: **{vol_7d_str} USD** ({stats['dias_activos']} días activos)",
        f"• 📊 Spread promedio: **{stats['spread_pct']:.2f}%**",
        f"• 🔄 Operaciones 24h: **{stats['ops_24h']}**",
        f"• ⏰ Horario pico: **{stats['hora_pico']}**",
        f"• 🛡️ Confiabilidad: **{stats['confiabilidad']}**",
        ""
    ]

    # Mostrar precios si están disponibles
    if stats['precio_compra'] and stats['precio_compra'] > 0:
        lines.append(
            f"• 💰 Precio compra típico: **{stats['precio_compra']:.2f}**")
    if stats['precio_venta'] and stats['precio_venta'] > 0:
        lines.append(
            f"• 💸 Precio venta típico: **{stats['precio_venta']:.2f}**")

    # Añadir interpretación
    lines.append("")
    if stats['confiabilidad'] == "🟢 ALTA":
        lines.append(
            "✅ **Merchant confiable** - Ideal para operaciones recurrentes")
    elif stats['confiabilidad'] == "🟡 MEDIA":
        lines.append(
            "⚠️ **Merchant moderado** - Verificar disponibilidad actual")
    else:
        lines.append("🔍 **Merchant nuevo o esporádico** - Operar con cautela")

    return "\n".join(lines)
