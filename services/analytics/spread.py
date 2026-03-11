from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import Tuple, List, Optional

from core.ram_window import get_global
from core import db as core_db
from core.processor import format_num, format_vol, ai_meta
from types import SimpleNamespace
from core.app_config import DEFAULT_TZ
import pytz


def _get_latest_snapshot(pair: str):
    """Obtiene el snapshot más reciente de RAM."""
    rw = get_global()
    if not rw:
        return None
    with rw.lock:
        dq = rw.pair_index.get(pair) or []
        if not dq:
            return None
        return dq[-1]


def _ordered_lists(snapshot):
    """Ordena compradores y vendedores según el libro de órdenes.
    
    - side='buy' (Binance BUY): Mercaderes VENDIENDO (tú compras). Mejor: Menor precio.
    - side='sell' (Binance SELL): Mercaderes COMPRANDO (tú vendes). Mejor: Mayor precio.
    """
    m_selling = [ad for ad in snapshot.ads if ad.side == 'buy']
    m_buying = [ad for ad in snapshot.ads if ad.side == 'sell']

    # Costo (tú compras): Del más barato al más caro
    cost_list = sorted(m_selling, key=lambda a: a.price)
    
    # Venta (tú vendes): Del que más te paga al que menos
    revenue_list = sorted(m_buying, key=lambda a: a.price, reverse=True)

    return cost_list, revenue_list


def _spread_from_pair(cost_ad, revenue_ad) -> Optional[float]:
    """Calcula spread % entre costo y venta.
    Fórmula: ((Sellers_Price - Buyers_Price) / Buyers_Price) * 100
    """
    try:
        # Profit = (Precio de Venta - Precio de Compra) / Precio de Compra
        return ((revenue_ad.price - cost_ad.price) / cost_ad.price) * 100
    except Exception:
        return None


def _format_spread_result(title: str, spreads: List[float], vols: List[float],
                          start_pos: int = None, end_pos: int = None) -> str:
    """Formatea resultados de spread de manera legible."""
    if not spreads:
        return "⚠️ No hay datos suficientes para calcular spreads."

    avg_spread = mean(spreads)
    avg_vol = mean(vols) if vols else 0
    min_spread = min(spreads)
    max_spread = max(spreads)

    # Determinar rango para el mensaje
    if start_pos is not None and end_pos is not None:
        if start_pos == end_pos:
            range_text = f"Posición {start_pos}"
        else:
            range_text = f"Posiciones {start_pos}–{end_pos}"
    else:
        range_text = title

    # Construir mensaje con explicación
    lines = [
        f"📊 <b>{range_text}</b>",
        f"• Spread promedio: <b>{avg_spread:.2f}%</b>",
        f"• Rango de spreads: {min_spread:.2f}% – {max_spread:.2f}%",
        f"• Volumen visible promedio: <b>{format_vol(avg_vol)} USDT</b>",
        ""
    ]

    # Añadir interpretación
    if avg_spread > 2.0:
        lines.append(
            "✅ <b>Spread AMPLIO</b> - Excelente oportunidad de arbitraje")
    elif avg_spread > 1.0:
        lines.append(
            "⚖️ <b>Spread MODERADO</b> - Evaluar liquidez antes de operar")
    elif avg_spread > 0.5:
        lines.append(
            "⚠️ <b>Spread ESTRECHO</b> - Mercado competitivo / Baja rentabilidad")
    else:
        lines.append(
            "🔴 <b>Fuga de Capital</b> - Inversión de precios detectada")

    if avg_vol > 15000:
        lines.append("💰 <b>Liquidez ALTA</b> - Ejecución inmediata probable")
    elif avg_vol > 5000:
        lines.append("💵 <b>Liquidez MEDIA</b> - Tiempo de ejecución moderado")
    else:
        lines.append(
            "🔄 <b>Liquidez BAJA</b> - Riesgo de demora en el intercambio")

    meta = {
        "type": "spread_analysis",
        "avg_spread": avg_spread,
        "avg_vol": avg_vol,
        "indicator": "AMPLIO" if avg_spread > 2 else "MODERADO" if avg_spread > 1 else "ESTRECHO"
    }
    return "\n".join(lines) + ai_meta(meta)


def _format_heat_map(pair: str, metrics: List[dict], period_name: str) -> str:
    """Genera una visualización de mapa de calor basada en bloques temporales."""
    if not metrics:
        return f"⚠️ No hay suficientes datos históricos para generar el reporte {period_name}."

    lines = [f"📊 <b>MAPA DE SPREAD {period_name.upper()}</b> ({pair})", ""]

    # Agrupar por bloques (ej. 1h)
    blocks = {}
    local_tz = pytz.timezone(DEFAULT_TZ)
    
    for m in metrics:
        # El timestamp viene en ISO UTC desde la DB
        utc_dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
        if utc_dt.tzinfo is None:
            utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
            
        local_dt = utc_dt.astimezone(local_tz)
        
        if period_name == 'dia':
            label = local_dt.strftime("%H:00")
        else:  # semana
            label = local_dt.strftime("%a %d")
        
        if label not in blocks:
            blocks[label] = []
        blocks[label].append(m['value'])

    for label, vals in blocks.items():
        avg = mean(vals)
        if avg > 2.0:
            emoji = "🔥"
        elif avg > 1.2:
            emoji = "🟩"
        elif avg > 0.8:
            emoji = "🟦"
        else:
            emoji = "⬜️"

        lines.append(f"• {label}: {emoji} <b>{avg:.2f}%</b>")

    lines.append("\n💡 <i>Promedio del Top 50 de anuncios por bloque.</i>")
    return "\n".join(lines)


def handle_spread(args: List[str], pair: str = 'USDT-COP') -> str:
    """
    Procesa comandos /spread.

    Formatos soportados:
    - /spread          : Promedio primeras 5 posiciones
    - /spread N        : Spread en posición específica (ej. /spread 10)
    - /spread N-M      : Promedio en rango de posiciones (ej. /spread 25-35)
    - /spread X%       : Busca posición más cercana a X% (ej. /spread 1%)
    - /spread X-Y%     : Lista posiciones en rango de % (ej. /spread 1-2%)
    - /spread >X       : Análisis de viabilidad (ej. /spread >0.7)
    """
    # Obtener snapshot
    snap = _get_latest_snapshot(pair)
    if not snap:
        # Fallback a BD
        last = core_db.get_latest_snapshot_for_pair(pair)
        if last:
            raw = last.get('raw', {})
            return (
                f"🗄️ **Datos de respaldo** (sin RAM activa)\n"
                f"• Último snapshot: {last.get('timestamp_utc')}\n"
                f"• Usa `/spread` nuevamente cuando el worker esté activo"
            )
        return f"⚠️ No hay datos disponibles para {pair}. Inicia el worker."

    # Obtener listas ordenadas
    costs, revenues = _ordered_lists(snap)
    if not costs or not revenues:
        return "⚠️ Datos insuficientes en el snapshot actual."

    max_positions = min(len(costs), len(revenues))
    token = (" ".join(args)).strip().lower() if args else ""

    # ===========================================
    # NUEVOS CASOS: dia / semana (Heatmap)
    # ===========================================
    if token in ('dia', 'semana'):
        hours = 24 if token == 'dia' else 168
        # Intentar con la nueva tabla detallada
        metrics = core_db.fetch_spread_analysis(pair, hours=hours)
        
        # Fallback a métricas genéricas si la nueva tabla está vacía (instalaciones nuevas)
        if not metrics:
            metrics = core_db.fetch_metrics_history(
                pair, 'avg_spread_top50', since_hours=hours)
        
        return _format_heat_map(pair, metrics, token)

    # ===========================================
    # NUEVO CASO: Filtro por Banco / Método Pago
    # ===========================================
    if token and any(c.isalpha() for c in token) and token not in ('buy', 'sell'):
        filtered_costs = [a for a in costs if token in (a.payment_method or '').lower()]
        filtered_revenues = [a for a in revenues if token in (a.payment_method or '').lower()]

        if not filtered_costs or not filtered_revenues:
            return f"⚠️ No hay suficientes anuncios activos con el método: <b>{token}</b> en {pair}"

        spreads = []
        vols = []
        for i in range(min(5, len(filtered_costs), len(filtered_revenues))):
            sp = _spread_from_pair(filtered_costs[i], filtered_revenues[i])
            if sp is not None:
                spreads.append(sp)
                vols.append(filtered_costs[i].quantity + filtered_revenues[i].quantity)

        return _format_spread_result(f"Método: {token.upper()}", spreads, vols)

    # ===========================================
    # CASO 1: Sin argumentos → primeras 5 posiciones
    # ===========================================
    if not token:
        n = min(5, max_positions)
        spreads = []
        vols = []
        for i in range(n):
            sp = _spread_from_pair(costs[i], revenues[i])
            if sp is not None:
                spreads.append(sp)
                vols.append(costs[i].quantity + revenues[i].quantity)

        return _format_spread_result("Primeras 5 posiciones", spreads, vols, 1, n)

    # ===========================================
    # CASO 2: Número específico (ej. /spread 10)
    # ===========================================
    if token.isdigit():
        idx = int(token) - 1
        if idx < 0 or idx >= max_positions:
            return f"⚠️ Posición {token} fuera de rango (máx: {max_positions})"

        sp = _spread_from_pair(costs[idx], revenues[idx])
        if sp is None:
            return f"⚠️ No se pudo calcular spread para posición {token}"

        vol = costs[idx].quantity + revenues[idx].quantity

        # Mensaje específico para una posición
        return (
            f"📌 <b>Posición #{token}</b>\n"
            f"• Spread: <b>{sp:.2f}%</b>\n"
            f"• Volumen visible: <b>{format_vol(vol)} USDT</b>\n"
            f"• Precio comp (Merchant Vende): {format_num(costs[idx].price)}\n"
            f"• Precio vent (Merchant Compra): {format_num(revenues[idx].price)}\n"
            f"\n💡 <i>Un spread positivo indica oportunidad de arbitraje</i>"
        ) + ai_meta({"type": "spread_position", "pos": token, "spread": sp, "vol": vol})

    # ===========================================
    # CASO 3: Rango numérico (ej. /spread 10-20)
    # ===========================================
    if '-' in token and token.replace('-', '').isdigit():
        parts = token.split('-')
        if len(parts) != 2:
            return "⚠️ Formato inválido. Usa: /spread 10-20"

        try:
            start = int(parts[0])
            end = int(parts[1])
        except ValueError:
            return "⚠️ Los valores deben ser números enteros"

        # Validar rango
        if start < 1 or end > max_positions or start > end:
            return f"⚠️ Rango inválido. Valores válidos: 1-{max_positions}"

        # Convertir a índices base 0
        start_idx = start - 1
        end_idx = end - 1

        spreads = []
        vols = []
        for i in range(start_idx, end_idx + 1):
            sp = _spread_from_pair(buys[i], sells[i])
            if sp is not None:
                spreads.append(sp)
                vols.append(buys[i].quantity + sells[i].quantity)

        if not spreads:
            return f"⚠️ No se pudieron calcular spreads en el rango {start}-{end}"

        return _format_spread_result(f"Rango {start}-{end}", spreads, vols, start, end)

    # ===========================================
    # CASO 4: Búsqueda por porcentaje (ej. /spread 1.25%)
    # ===========================================
    if token.endswith('%'):
        # Extraer el valor numérico (quitando el %)
        percent_str = token[:-1].strip()

        # Manejar rangos de porcentaje (ej. 1-2%)
        if '-' in percent_str:
            parts = percent_str.split('-')
            if len(parts) != 2:
                return "⚠️ Formato inválido. Usa: /spread 1-2%"

            try:
                min_pct = float(parts[0])
                max_pct = float(parts[1])
            except ValueError:
                return "⚠️ Los valores deben ser números (ej. /spread 1-2%)"

            if min_pct >= max_pct:
                return "⚠️ El valor mínimo debe ser menor que el máximo"

            # Buscar posiciones en el rango de porcentaje
            matches = []
            for i in range(max_positions):
                sp = _spread_from_pair(costs[i], revenues[i])
                if sp is None:
                    continue
                if min_pct <= sp <= max_pct:
                    matches.append({
                        'pos': i + 1,
                        'spread': sp,
                        'vol': costs[i].quantity + revenues[i].quantity
                    })

            if not matches:
                return f"⚠️ No hay posiciones con spread entre {min_pct}% y {max_pct}%"

            # Formatear respuesta
            lines = [
                f"📊 <b>Posiciones con spread {min_pct}%–{max_pct}%</b>",
                f"• Total encontradas: <b>{len(matches)}</b> posiciones",
                ""
            ]

            # Mostrar primeras 10 para no saturar
            for idx, m in enumerate(matches[:10], 1):
                lines.append(
                    f"{idx:2d}. Pos <code>#{m['pos']:3d}</code> → Spread: <b>{m['spread']:.2f}%</b> | Vol: {format_vol(m['vol'])}")

            if len(matches) > 10:
                lines.append(f"   ... y {len(matches) - 10} más")

            lines.append("")
            lines.append(
                "💡 *Usa /spread N para ver detalles de una posición específica*")

            return "\n".join(lines) + ai_meta({"type": "spread_percent_range", "matches": len(matches)})

        # Si es un solo porcentaje (ej. /spread 1.25%)
        else:
            try:
                target_pct = float(percent_str)
            except ValueError:
                return "⚠️ Valor inválido. Usa: /spread 1.25%"

            # Encontrar la posición con spread más cercano
            best_pos = None
            best_spread = None
            best_diff = float('inf')
            best_vol = 0

            for i in range(max_positions):
                sp = _spread_from_pair(costs[i], revenues[i])
                if sp is None:
                    continue

                diff = abs(sp - target_pct)
                if diff < best_diff:
                    best_diff = diff
                    best_pos = i + 1
                    best_spread = sp
                    best_vol = costs[i].quantity + revenues[i].quantity

            if best_pos is None:
                return f"⚠️ No se pudo encontrar posición cercana a {target_pct}%"

            # Calcular precisión del match
            accuracy = 100 - (best_diff / target_pct *
                              100) if target_pct > 0 else 0

            # Formatear respuesta con explicación
            lines = [
                f"🎯 <b>Búsqueda: {target_pct}%</b>",
                f"• Posición más cercana: <b>#{best_pos}</b>",
                f"• Spread real: <b>{best_spread:.2f}%</b>",
                f"• Diferencia: {best_diff:.3f}% ({accuracy:.1f}% de precisión)",
                f"• Volumen visible: <b>{format_vol(best_vol)} USDT</b>",
                ""
            ]

            # Añadir contexto
            if best_spread > target_pct:
                lines.append(
                    f"📈 Este spread es **{best_spread - target_pct:.2f}% MAYOR** al objetivo")
            else:
                lines.append(
                    f"📉 Este spread es **{target_pct - best_spread:.2f}% MENOR** al objetivo")

            lines.append("")
            lines.append(
                f"💡 *Para ver el spread exacto: `/spread {best_pos}`*")

            return "\n".join(lines)

    # ===========================================
    # CASO 5: Análisis de viabilidad (ej. /spread >0.7)
    # ===========================================
    if token.startswith('>'):
        # Extraer el valor numérico (quitando el '>')
        threshold_str = token[1:].strip()

        try:
            threshold = float(threshold_str)
        except ValueError:
            return "⚠️ Valor inválido. Usa: /spread >0.7 o /spread >1.23"

        # Validar rango lógico
        if threshold <= 0:
            return "⚠️ El umbral debe ser mayor a 0%"
        if threshold > 10:
            return "⚠️ El umbral es muy alto (>10%). No hay posiciones con spreads tan altos."

        # Calcular el rango de spreads a analizar: [threshold, threshold + 0.3]
        spread_min = threshold
        spread_max = threshold + 0.3

        # PASO 1: Encontrar las posiciones que caen en este rango de spreads
        positions_in_range = []
        first_pos = None
        last_pos = None

        for i in range(max_positions):
            sp = _spread_from_pair(costs[i], revenues[i])
            if sp is None:
                continue

            if spread_min <= sp <= spread_max:
                positions_in_range.append({
                    'pos': i + 1,
                    'spread': sp,
                    'vol_actual': costs[i].quantity + revenues[i].quantity
                })
                if first_pos is None:
                    first_pos = i + 1
                last_pos = i + 1

        if not positions_in_range:
            # Buscar la posición más cercana para dar recomendación
            closest_pos = None
            closest_spread = None
            closest_diff = float('inf')

            for i in range(max_positions):
                sp = _spread_from_pair(costs[i], revenues[i])
                if sp is None:
                    continue
                diff = abs(sp - threshold)
                if diff < closest_diff:
                    closest_diff = diff
                    closest_pos = i + 1
                    closest_spread = sp

            if closest_pos:
                direction = "aumentar" if closest_spread < threshold else "disminuir"
                return (
                    f"⚠️ No hay spreads entre {spread_min:.2f}% y {spread_max:.2f}%\n\n"
                    f"• El spread más cercano a {threshold}% es **{closest_spread:.2f}%** en posición #{closest_pos}\n"
                    f"• Prueba con: `/spread >{closest_spread:.1f}` para {direction} el umbral"
                )
            else:
                return f"⚠️ No se encontraron posiciones cerca de {threshold}%"

        # PASO 2: Obtener datos históricos de la última hora desde RAM
        rw = get_global()
        if not rw:
            return "⚠️ No hay datos históricos disponibles. El worker debe estar activo."

        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

        # Recolectar volúmenes históricos para el rango de posiciones
        historical_volumes = []
        snapshot_count = 0

        with rw.lock:
            dq = rw.pair_index.get(pair, [])
            for snap in reversed(dq):
                if snap.timestamp < cutoff:
                    break

                snapshot_count += 1

                # Obtener datos de ese snapshot
                b_hist, s_hist = _ordered_lists(snap)
                if len(b_hist) < last_pos or len(s_hist) < last_pos:
                    continue  # Snapshot no tiene suficientes posiciones

                # Sumar volumen en el rango de posiciones
                total_vol_range = 0
                for pos_idx in range(first_pos - 1, last_pos):
                    if pos_idx < len(b_hist) and pos_idx < len(s_hist):
                        total_vol_range += b_hist[pos_idx].quantity + \
                            s_hist[pos_idx].quantity

                historical_volumes.append(total_vol_range)

        if not historical_volumes:
            return "⚠️ No hay suficientes snapshots históricos en la última hora."

        # PASO 3: Calcular métricas
        avg_vol_per_snapshot = mean(historical_volumes)
        max_vol = max(historical_volumes)
        min_vol = min(historical_volumes)

        # Proyectar volumen por hora (asumiendo snapshots cada ~2 minutos)
        snapshots_per_hour = len(historical_volumes)
        estimated_hourly_volume = avg_vol_per_snapshot * \
            (60 / 2)  # 2 min entre snapshots

        # PASO 4: Determinar nivel de rotación
        if estimated_hourly_volume < 1000:
            rotation = "🔴 BAJA"
            recommendation = "No recomendado para arbitraje. Volumen insuficiente."
        elif estimated_hourly_volume < 3000:
            rotation = "🟡 MEDIA"
            recommendation = "Aceptable para operaciones moderadas. Monitorear liquidez."
        else:
            rotation = "🟢 ALTA"
            recommendation = "✅ Ideal para arbitraje. Buen volumen y rotación."

        # PASO 5: Construir mensaje
            lines = [
                f"📊 <b>ANÁLISIS DE VIABILIDAD</b>",
                f"• Umbral: <b>>{threshold}%</b> | Rango: {spread_min:.2f}% – {spread_max:.2f}%",
                f"• Bloque analizado: <b>Posiciones {first_pos} – {last_pos}</b>",
                f"• Total posiciones en rango: {len(positions_in_range)}",
                "",
                f"📈 <b>Volumen histórico (última hora)</b>",
                f"• Snapshots analizados: {snapshot_count}",
                f"• Volumen promedio por snapshot: <b>{format_vol(avg_vol_per_snapshot)} USDT</b>",
                f"• Volumen mínimo: {format_vol(min_vol)} USDT",
                f"• Volumen máximo: {format_vol(max_vol)} USDT",
                f"• <b>Volumen estimado por hora: {format_vol(estimated_hourly_volume)} USDT</b>",
                "",
                f"🔄 <b>Rotación: {rotation}</b>",
                f"• {recommendation}",
                "",
                f"💡 <b>Posiciones en el bloque:</b>"
            ]

        # Mostrar primeras 5 posiciones del bloque
        for idx, p in enumerate(positions_in_range[:5], 1):
            lines.append(
                f"  {idx}. #{p['pos']:3d} → Spread: {p['spread']:.2f}% | Vol actual: {p['vol_actual']:.2f}")

        if len(positions_in_range) > 5:
            lines.append(f"  ... y {len(positions_in_range) - 5} más")

        lines.append("")
        lines.append(
            f"🔍 *Para ver detalles de una posición: `/spread {first_pos}`*")

        return "\n".join(lines)

    # Si llegamos aquí, el comando no se reconoce
    return (
        "⚠️ **Comando no reconocido**\n\n"
        "Formatos soportados:\n"
        "• `/spread` - Promedio primeras 5 posiciones\n"
        "• `/spread 10` - Spread en posición 10\n"
        "• `/spread 10-20` - Promedio en posiciones 10 a 20\n"
        "• `/spread 1%` - Posición más cercana a 1%\n"
        "• `/spread >1.2` - Análisis de viabilidad"
    )
