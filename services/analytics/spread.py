from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import Tuple, List, Optional

from core.ram_window import get_global
from core import db as core_db
from types import SimpleNamespace


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
    """Ordena compradores y vendedores según libro de órdenes real.
    
    - BUY: mayor precio primero (el que más paga)
    - SELL: menor precio primero (el que vende más barato)
    """
    buys = [ad for ad in snapshot.ads if ad.side == 'buy']
    sells = [ad for ad in snapshot.ads if ad.side == 'sell']
    
    # Compra: mayor precio primero (quien más paga)
    buys_sorted = sorted(buys, key=lambda a: a.price)
    
    # Venta: menor precio primero (quien vende más barato)
    sells_sorted = sorted(sells, key=lambda a: a.price, reverse=True)
    
    return buys_sorted, sells_sorted


def _spread_from_pair(buy, sell) -> Optional[float]:
    """Calcula spread % entre un comprador y un vendedor específicos."""
    try:
        return ((buy.price - sell.price) / sell.price) * 100
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
        f"📊 **{range_text}**",
        f"• Spread promedio: **{avg_spread:.2f}%**",
        f"• Rango de spreads: {min_spread:.2f}% – {max_spread:.2f}%",
        f"• Volumen visible promedio: {avg_vol:.2f} USDT",
        ""
    ]
    
    # Añadir interpretación
    if avg_spread > 2.0:
        lines.append("✅ **Spread AMPLIO** - Buen momento para operar")
    elif avg_spread > 1.0:
        lines.append("⚖️ **Spread MODERADO** - Evaluar volumen")
    else:
        lines.append("⚠️ **Spread ESTRECHO** - Mercado competitivo")
    
    if avg_vol > 10000:
        lines.append("💰 **Alto volumen** - Liquidez abundante")
    elif avg_vol > 5000:
        lines.append("💵 **Volumen medio** - Liquidez suficiente")
    else:
        lines.append("🔄 **Bajo volumen** - Puede haber demoras")
    
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
    - /spread >X       : Análisis de viabilidad (próxima fase)
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
    buys, sells = _ordered_lists(snap)
    if not buys or not sells:
        return "⚠️ Datos insuficientes en el snapshot actual."
    
    max_positions = min(len(buys), len(sells))
    token = (" ".join(args)).strip() if args else ""
    
    # ===========================================
    # CASO 1: Sin argumentos → primeras 5 posiciones
    # ===========================================
    if not token:
        n = min(5, max_positions)
        spreads = []
        vols = []
        for i in range(n):
            sp = _spread_from_pair(buys[i], sells[i])
            if sp is not None:
                spreads.append(sp)
                vols.append(buys[i].quantity + sells[i].quantity)
        
        return _format_spread_result("Primeras 5 posiciones", spreads, vols, 1, n)
    
    # ===========================================
    # CASO 2: Número específico (ej. /spread 10)
    # ===========================================
    if token.isdigit():
        idx = int(token) - 1
        if idx < 0 or idx >= max_positions:
            return f"⚠️ Posición {token} fuera de rango (máx: {max_positions})"
        
        sp = _spread_from_pair(buys[idx], sells[idx])
        if sp is None:
            return f"⚠️ No se pudo calcular spread para posición {token}"
        
        vol = buys[idx].quantity + sells[idx].quantity
        
        # Mensaje específico para una posición
        return (
            f"📌 **Posición #{token}**\n"
            f"• Spread: **{sp:.2f}%**\n"
            f"• Volumen visible: {vol:.2f} USDT\n"
            f"• Precio compra: {buys[idx].price:.4f}\n"
            f"• Precio venta: {sells[idx].price:.4f}\n"
            f"\n💡 *Un spread positivo indica oportunidad de arbitraje*"
        )
    
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
                sp = _spread_from_pair(buys[i], sells[i])
                if sp is None:
                    continue
                if min_pct <= sp <= max_pct:
                    matches.append({
                        'pos': i + 1,
                        'spread': sp,
                        'vol': buys[i].quantity + sells[i].quantity
                    })
            
            if not matches:
                return f"⚠️ No hay posiciones con spread entre {min_pct}% y {max_pct}%"
            
            # Formatear respuesta
            lines = [
                f"📊 **Posiciones con spread {min_pct}%–{max_pct}%**",
                f"• Total encontradas: **{len(matches)}** posiciones",
                ""
            ]
            
            # Mostrar primeras 10 para no saturar
            for idx, m in enumerate(matches[:10], 1):
                lines.append(f"{idx:2d}. Pos #{m['pos']:3d} → Spread: **{m['spread']:.2f}%** | Vol: {m['vol']:.2f}")
            
            if len(matches) > 10:
                lines.append(f"   ... y {len(matches) - 10} más")
            
            lines.append("")
            lines.append("💡 *Usa /spread N para ver detalles de una posición específica*")
            
            return "\n".join(lines)
        
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
                sp = _spread_from_pair(buys[i], sells[i])
                if sp is None:
                    continue
                
                diff = abs(sp - target_pct)
                if diff < best_diff:
                    best_diff = diff
                    best_pos = i + 1
                    best_spread = sp
                    best_vol = buys[i].quantity + sells[i].quantity
            
            if best_pos is None:
                return f"⚠️ No se pudo encontrar posición cercana a {target_pct}%"
            
            # Calcular precisión del match
            accuracy = 100 - (best_diff / target_pct * 100) if target_pct > 0 else 0
            
            # Formatear respuesta con explicación
            lines = [
                f"🎯 **Búsqueda: {target_pct}%**",
                f"• Posición más cercana: **#{best_pos}**",
                f"• Spread real: **{best_spread:.2f}%**",
                f"• Diferencia: {best_diff:.3f}% ({accuracy:.1f}% de precisión)",
                f"• Volumen visible: {best_vol:.2f} USDT",
                ""
            ]
            
            # Añadir contexto
            if best_spread > target_pct:
                lines.append(f"📈 Este spread es **{best_spread - target_pct:.2f}% MAYOR** al objetivo")
            else:
                lines.append(f"📉 Este spread es **{target_pct - best_spread:.2f}% MENOR** al objetivo")
            
            lines.append("")
            lines.append(f"💡 *Para ver el spread exacto: `/spread {best_pos}`*")
            
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
            sp = _spread_from_pair(buys[i], sells[i])
            if sp is None:
                continue
            
            if spread_min <= sp <= spread_max:
                positions_in_range.append({
                    'pos': i + 1,
                    'spread': sp,
                    'vol_actual': buys[i].quantity + sells[i].quantity
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
                sp = _spread_from_pair(buys[i], sells[i])
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
                        total_vol_range += b_hist[pos_idx].quantity + s_hist[pos_idx].quantity
                
                historical_volumes.append(total_vol_range)
        
        if not historical_volumes:
            return "⚠️ No hay suficientes snapshots históricos en la última hora."
        
        # PASO 3: Calcular métricas
        avg_vol_per_snapshot = mean(historical_volumes)
        max_vol = max(historical_volumes)
        min_vol = min(historical_volumes)
        
        # Proyectar volumen por hora (asumiendo snapshots cada ~2 minutos)
        snapshots_per_hour = len(historical_volumes)
        estimated_hourly_volume = avg_vol_per_snapshot * (60 / 2)  # 2 min entre snapshots
        
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
            f"📊 **ANÁLISIS DE VIABILIDAD**",
            f"• Umbral: **>{threshold}%** | Rango: {spread_min:.2f}% – {spread_max:.2f}%",
            f"• Bloque analizado: **Posiciones {first_pos} – {last_pos}**",
            f"• Total posiciones en rango: {len(positions_in_range)}",
            "",
            f"**📈 Volumen histórico (última hora)**",
            f"• Snapshots analizados: {snapshot_count}",
            f"• Volumen promedio por snapshot: {avg_vol_per_snapshot:.2f} USDT",
            f"• Volumen mínimo: {min_vol:.2f} USDT",
            f"• Volumen máximo: {max_vol:.2f} USDT",
            f"• **Volumen estimado por hora: {estimated_hourly_volume:.0f} USDT**",
            "",
            f"**🔄 Rotación: {rotation}**",
            f"• {recommendation}",
            "",
            f"**💡 Posiciones en el bloque:**"
        ]
        
        # Mostrar primeras 5 posiciones del bloque
        for idx, p in enumerate(positions_in_range[:5], 1):
            lines.append(f"  {idx}. #{p['pos']:3d} → Spread: {p['spread']:.2f}% | Vol actual: {p['vol_actual']:.2f}")
        
        if len(positions_in_range) > 5:
            lines.append(f"  ... y {len(positions_in_range) - 5} más")
        
        lines.append("")
        lines.append(f"🔍 *Para ver detalles de una posición: `/spread {first_pos}`*")
        
        return "\n".join(lines)
    
    # Si llegamos aquí, el comando no se reconoce
    return (
        "⚠️ **Comando no reconocido**\n\n"
        "Formatos soportados:\n"
        "• `/spread` - Promedio primeras 5 posiciones\n"
        "• `/spread 10` - Spread en posición 10\n"
        "• `/spread 10-20` - Promedio en posiciones 10 a 20\n"
        "• `/spread 1%` - (próximamente) Posición más cercana a 1%\n"
        "• `/spread >1.2` - (próximamente) Análisis de viabilidad"
    )

'''
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
'''