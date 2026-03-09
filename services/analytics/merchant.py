from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import sqlite3

from core.ram_window import get_global
from core.db import DB_PATH
from core.processor import format_num, format_vol, ai_meta
from core.detectors.merchant_intel import calculate_automation_score


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

                usdt_volume = ad.quantity

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

    lines = [f"🏆 <b>{title}</b>"]

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
            f"{rank_emoji} <code>@{m:<15}</code> "
            f"Vol: {format_vol(vol):>8} USDT  "
            f"Pr: {format_num(avg_price, 2):>7}  "
            f"Ops: {count:>3}"
        )

    lines.append("\n💡 <i>Volumen total visible en la ultima hora</i>")

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
        dq = rw.merchant_index.get(name, [])
        for ts, ad in dq:
            if ts < cutoff:
                continue
            count += 1
            vol += ad.quantity
            prices.append(ad.price)
            sides.append(ad.side)

    if count == 0:
        return f"⚠️ Merchant `{name}` sin actividad en la ultima hora"

    # 1. Obtener Score de Automatización desde DB (Inteligencia de Comerciantes)
    # Buscamos por nombre para obtener el ID primero
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT merchant_id FROM merchant_registry WHERE nickname = ? LIMIT 1", (name,))
    row = cur.fetchone()
    conn.close()
    
    m_id = row[0] if row else None
    intel = calculate_automation_score(m_id) if m_id else {'score': 0, 'classification': 'N/D', 'metrics': {}}
    
    # 2. Cálculos base
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
    currency = "COP" if "COP" in pair else "VES"

    # 3. Score visual
    score = intel.get('score', 0)
    classification = intel.get('classification', 'HUMANO')
    
    if classification == "BOT/ALGORITMO":
        intel_emoji = "🤖"
    elif classification == "ACTIVO":
        intel_emoji = "⚡"
    else:
        intel_emoji = "👤"

    lines = [
        f"{intel_emoji} <b>INTELIGENCIA DE MERCHANT</b>",
        f"• Nombre: <code>{name}</code>",
        f"• ID: <code>{m_id or 'N/A'}</code>",
        f"• Clasificación: <b>{classification}</b>",
        f"• Automation Score: <b>{score}/100</b>",
        "",
        "📊 <b>Flujo Reciente (24h)</b>",
        f"• Frecuencia: {intel['metrics'].get('changes_24h', 0)} cambios",
        f"• Persistencia Top 3: {intel['metrics'].get('persistence_top3_pct', 0)}%",
        "",
        "📊 <b>Actividad (última hora)</b>",
        f"• Anuncios publicados (1h): {count}",
        f"• Volumen total: <b>{format_vol(vol)} USDT</b>",
        "",
        "💰 <b>Precios</b>",
        f"• Precio promedio: <b>{format_num(avg_price)} {currency}</b>",
        f"• Estabilidad: {stability}",
    ]

    meta = {
        "type": "merchant_profile",
        "merchant": name,
        "volume_1h": vol,
        "ad_count_1h": count,
        "avg_price": avg_price,
        "activity_score": activity_score
    }

    with rw.lock:
        latest_pair = rw.pair_index.get(pair)
        if latest_pair:
            latest_snap = latest_pair[-1]
            buys = [a for a in latest_snap.ads if a.side == 'buy']
            sells = [a for a in latest_snap.ads if a.side == 'sell']

            for i, ad in enumerate(sorted(buys, key=lambda a: a.price, reverse=True), 1):
                if ad.merchant == name:
                    lines.append(f"• Posición actual (compra): <b>#{i}</b>")
                    break

            for i, ad in enumerate(sorted(sells, key=lambda a: a.price), 1):
                if ad.merchant == name:
                    lines.append(f"• Posición actual (venta): <b>#{i}</b>")
                    break

    return "\n".join(lines) + ai_meta(meta)


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
    token = (" ".join(args)).strip().lower() if args else 'top'

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
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        
        # Obtener los top bots segun score y actividad reciente
        cur.execute(
            """
            SELECT nickname, automation_score, classification 
            FROM merchant_registry 
            WHERE automation_score > 60 OR classification = 'BOT/ALGORITMO'
            ORDER BY automation_score DESC 
            LIMIT 10
            """
        )
        suspects = cur.fetchall()
        conn.close()

        if not suspects:
            return "✅ No se han detectado merchants con comportamiento algorítmico significativo en el historial."

        lines = ["🤖 <b>ALGORITMOS Y BOTS DETECTADOS</b>", ""]
        lines.append("<code> #  Merchant      Score   Tipo </code>")
        lines.append("<code>---  ----------  ------  ---------</code>")
        
        for i, s in enumerate(suspects, 1):
            name = (s['nickname'] or 'N/D')[:10]
            score = s['automation_score']
            cl = s['classification']
            lines.append(f"<code>{i:2d}  @{name:<10}  {score:>5.1f}   {cl}</code>")

        lines.append("\n💡 <i>Detección basada en frecuencia de cambios, persistencia en Top y velocidad de relist (7 días).</i>")
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
                    vol += ad.quantity
                    count += 1

                if vol >= 1000 or count >= 10:
                    bigs.append((m, vol, count))

        if not bigs:
            return "⚠️ No se encontraron merchants grandes en la última hora."

        lines = ["🏦 <b>MERCHANTS DESTACADOS</b>", ""]
        for m, v, c in sorted(bigs, key=lambda x: x[1], reverse=True)[:10]:
            lines.append(
                f"• <code>@{m:<12}</code>: Vol: <b>{format_vol(v):>8}</b> USDT | Ads: <b>{c:>3}</b>")

        meta = {
            "type": "merchant_bigs",
            "count": len(bigs),
            "top_merchants": [b[0] for b in sorted(bigs, key=lambda x: x[1], reverse=True)[:5]]
        }
        return "\n".join(lines) + ai_meta(meta)

    # ===========================================
    # CASO 6: Búsqueda por nombre parcial
    # ===========================================
    if token.startswith('search '):
        query = token[7:].strip()
        if len(query) < 3:
            return "⚠️ Escribe al menos 3 caracteres para buscar."

        matches = _search_merchants(query)

        if not matches:
            return f"⚠️ No hay merchants que contengan '{query}'"

        lines = [f"🔍 <b>Merchants que contienen '{query}'</b>", ""]
        for m in matches:
            lines.append(f"• <code>@{m}</code>")

        lines.append(
            f"\n💡 Usa <code>/merchant @nombre</code> para ver perfil completo")
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
            ORDER BY ABS(COALESCE(buy_avg,0) - COALESCE(sell_avg,0)) ASC
            LIMIT 10
        """, (pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

        rows = c.fetchall()
        conn.close()

        if not rows:
            return f"⚠️ No hay suficientes datos de merchants estables para {pair} hoy."

        lines = [f"📊 <b>MERCHANTS ESTABLES ({pair})</b>", ""]
        lines.append("<code> #  Merchant      Spread   Volumen </code>")
        lines.append("<code>---  ----------  -------  ---------</code>")

        for idx, row in enumerate(rows, 1):
            merchant = row[0][:10]
            spread = abs(((row[4] or 0) - (row[5] or 0)) / (row[5] or 1) * 100)
            volumen = row[3] or 0

            lines.append(
                f"<code>{idx:2d}  @{merchant:<10}  {spread:>5.2f}%  {format_vol(volumen):>9}</code>"
            )

        lines.append("\n💡 <i>Estables = menor spread promedio hoy</i>")

        meta = {
            "type": "merchant_estables",
            "pair": pair,
            "count": len(rows),
            "best_spread": abs(((rows[0][4] or 0) - (rows[0][5] or 0)) / (rows[0][5] or 1) * 100) if rows else 0
        }
        return "\n".join(lines) + ai_meta(meta)

    # ===========================================
    # CASO 9: Merchants rápidos
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
                AVG(ad_count / CAST(NULLIF(ad_count, 0) AS REAL)) as dummy
            FROM merchant_stats
            WHERE pair = ? AND date = ?
            GROUP BY merchant
            HAVING horas_activas >= 3
            ORDER BY (SUM(ad_count) / COUNT(DISTINCT hour)) DESC
            LIMIT 10
        """, (pair, datetime.now(timezone.utc).strftime("%Y-%m-%d")))

        rows = c.fetchall()
        conn.close()

        if not rows:
            return f"⚠️ No hay suficientes datos de merchants rápidos para {pair} hoy."

        lines = [f"⚡ <b>MERCHANTS RÁPIDOS ({pair})</b>", ""]
        lines.append("<code> #  Merchant      Ops/h   Vol/hora </code>")
        lines.append("<code>---  ----------  -----   ---------</code>")

        for idx, row in enumerate(rows, 1):
            merchant = row[0][:10]
            total_ops = row[1] or 0
            horas = row[3] or 1
            ops_por_hora = total_ops / horas
            volumen_hora = (row[2] or 0) / horas

            lines.append(
                f"<code>{idx:2d}  @{merchant:<10}  {ops_por_hora:>5.1f}   {format_vol(volumen_hora):>9}</code>"
            )

        lines.append(
            "\n💡 <i>Rápidos = mayor frecuencia de operaciones/hora hoy</i>")

        meta = {
            "type": "merchant_rapidos",
            "pair": pair,
            "count": len(rows)
        }
        return "\n".join(lines) + ai_meta(meta)

    # Si el token es puramente numérico o muy corto y no es comando, ignorar
    if not token or token == 'top':
        # Proceder al top global por defecto
        pass
    else:
        # Si llegamos aquí, es un perfil específico (@usuario o nombre) o un comando
        valid_tokens = ('top', 'buy', 'sell', 'bots', 'grandes',
                        'estables', 'rapidos', 'search')

        is_command = False
        if token in valid_tokens or token.startswith('search'):
            is_command = True

        if is_command:
            # Si era un comando pero no devolvió nada arriba, es que hubo un fallo lógico
            # o el usuario escribió /merchant search sin nada más.
            if token.startswith('search') and len(token) <= 7:
                return "⚠️ Uso: <code>/merchant search &lt;texto&gt;</code>"
            # No retornamos aquí, dejamos que intente buscar perfil si no es comando EXACTO
        else:
            # ES UN PERFIL
            if token.startswith('@'):
                name = token[1:].strip()
            else:
                name = token.strip()

            if not name:
                return (
                    "⚠️ <b>Perfil no especificado.</b>\n\n"
                    "<b>Opciones:</b>\n"
                    "• <code>/merchant @usuario</code> - Ver perfil\n"
                    "• <code>/merchant search &lt;nombre&gt;</code> - Buscar\n"
                    "• <code>/merchant</code> - Top global"
                )

    stats = _fetch_merchant_stats(name, pair)

    if not stats:
        # Fallback a la versión de RAM
        ram_profile = _build_merchant_profile(name, pair)
        # Si el perfil de RAM dice "sin actividad", entonces el merchant realmente no existe o está inactivo
        if "sin actividad" in ram_profile.lower():
            return f"⚠️ <b>Merchant no encontrado:</b> <code>@{name}</code>\n<i>No se detectó actividad en las últimas 24h en {pair}.</i>"
        return ram_profile

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
        f"👤 <b>PERFIL: @{stats['merchant']}</b> ({pair})",
        f"• {vol_emoji} Volumen 24h: <b>{vol_24h_str} USD</b>",
        f"• 📅 Volumen 7d: <b>{vol_7d_str} USD</b> ({stats['dias_activos']} días activos)",
        f"• 📊 Spread promedio: <b>{stats['spread_pct']:.2f}%</b>",
        f"• 🔄 Operaciones 24h: <b>{stats['ops_24h']}</b>",
        f"• ⏰ Horario pico: <b>{stats['hora_pico']}</b>",
        f"• 🛡️ Confiabilidad: <b>{stats['confiabilidad']}</b>",
        ""
    ]

    # Mostrar precios si están disponibles
    if stats['precio_compra'] and stats['precio_compra'] > 0:
        lines.append(
            f"• 💰 Precio compra típico: <b>{format_num(stats['precio_compra'], 2)}</b>")
    if stats['precio_venta'] and stats['precio_venta'] > 0:
        lines.append(
            f"• 💸 Precio venta típico: <b>{format_num(stats['precio_venta'], 2)}</b>")

    # Añadir interpretación
    lines.append("")
    if stats['confiabilidad'] == "🟢 ALTA":
        lines.append(
            "✅ <b>Merchant confiable</b> - Ideal para operaciones recurrentes")
    elif stats['confiabilidad'] == "🟡 MEDIA":
        lines.append(
            "⚠️ <b>Merchant moderado</b> - Verificar disponibilidad actual")
    else:
        lines.append(
            "🔍 <b>Merchant nuevo o esporádico</b> - Operar con cautela")

    meta = {
        "type": "merchant_profile_stats",
        "merchant": stats['merchant'],
        "vol_24h": stats['vol_24h'],
        "spread_pct": stats['spread_pct'],
        "confiabilidad": stats['confiabilidad']
    }

    return "\n".join(lines) + ai_meta(meta)
