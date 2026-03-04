from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev
from typing import List, Optional

from core.ram_window import get_global
from core import pipeline
from core.processor import format_num, ai_meta


def _get_volatility_history(pair: str, hours: int = 6) -> List[dict]:
    """Obtiene historial de volatilidad por hora."""
    rw = get_global()
    if not rw:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    history = []

    with rw.lock:
        dq = rw.pair_index.get(pair, [])
        hourly_buckets = {}

        for snap in reversed(dq):
            if snap.timestamp < cutoff:
                break

            hour_key = snap.timestamp.strftime("%H:00")
            if hour_key not in hourly_buckets:
                hourly_buckets[hour_key] = []

            # Calcular spread promedio del snapshot como proxy de volatilidad
            buys = sorted([ad for ad in snap.ads if ad.side ==
                          'buy'], key=lambda x: x.price, reverse=True)
            sells = sorted([ad for ad in snap.ads if ad.side ==
                           'sell'], key=lambda x: x.price)

            if buys and sells:
                spreads = []
                for i in range(min(10, len(buys), len(sells))):  # primeras 10
                    if sells[i].price > 0:
                        sp = ((buys[i].price - sells[i].price) /
                              sells[i].price) * 100
                        spreads.append(sp)
                if spreads:
                    hourly_buckets[hour_key].append(mean(spreads))

        # Promediar por hora
        for hour, values in hourly_buckets.items():
            if values:
                history.append({
                    'hour': hour,
                    'volatility': mean(values),
                    'samples': len(values)
                })

    return sorted(history, key=lambda x: x['hour'])


def _interpret_volatility(coef_var: float) -> dict:
    """Interpreta el coeficiente de variación en lenguaje humano."""
    if coef_var is None:
        coef_var = 0.0

    if coef_var < 1.0:
        return {
            'level': 'BAJA',
            'emoji': '🟢',
            'desc': 'Precios muy estables, spreads predecibles',
            'advice': 'Ideal para estrategias conservadoras. Puedes usar spreads más ajustados.'
        }
    elif coef_var < 2.0:
        return {
            'level': 'MODERADA',
            'emoji': '🟡',
            'desc': 'Movimientos normales del mercado',
            'advice': 'Buen momento para operar con spreads entre 0.8% y 1.2%'
        }
    elif coef_var < 3.0:
        return {
            'level': 'ALTA',
            'emoji': '🟠',
            'desc': 'Precios fluctuando significativamente',
            'advice': 'Oportunidades de arbitraje, pero usar órdenes límite y monitorear'
        }
    else:
        return {
            'level': 'EXTREMA',
            'emoji': '🔴',
            'desc': 'Mercado errático, posible noticia o evento',
            'advice': '⚠️ CAUTELA EXTREMA. Spreads >2% posibles pero alto riesgo'
        }


def handle_volatility(args: List[str], pair: str = 'USDT-COP') -> str:
    """Procesa /volatilidad con formato amigable."""

    fiat = pair.split('-')[1] if '-' in pair else 'COP'

    # Obtener datos actuales
    data = pipeline.build_data_from_db({
        'monedas': {fiat: {}},
        'ponderacion_volumen': True
    })

    if not data or fiat not in data:
        return f"⚠️ No hay datos suficientes para {pair}"

    # Extraer coeficiente de variación
    info_key = f"{fiat.lower()}_buy"
    coef_var = data.get('analisis', {}).get(info_key, {}).get('coef_var', 0)

    # Obtener histórico
    history = _get_volatility_history(pair, hours=6)

    # Calcular variación en la moneda
    precio_actual = data[fiat].get('promedio_buy_tasa', 0)

    if not precio_actual or coef_var is None:
        return f"⚠️ Datos insuficientes en base de datos para realizar el cálculo de volatilidad para {pair}."

    variacion_valor = precio_actual * (coef_var / 100)  # Aproximación

    # Interpretar volatilidad
    interp = _interpret_volatility(coef_var)

    # Construir mensaje
    lines = [
        f"📊 <b>ANÁLISIS DE VOLATILIDAD</b> ({pair})",
        "",
        f"🌡️ <b>Nivel actual: {interp['emoji']} {interp['level']}</b>",
        f"• Coeficiente: {coef_var:.2f} (escala 0-5+)",
        f"• Variación precio: ± {variacion_valor:.0f} {fiat} en última hora",
        f"• Confianza: {max(0, 100 - coef_var*10):.0f}%",
        "",
        f"📈 <b>Tendencia últimas 6h:</b>"
    ]

    if not history:
        lines.append("⚠️ *Recolectando datos históricos en memoria...*")
    else:
        # Agregar gráfico de barras textual
        for h in history[-6:]:  # últimas 6 horas
            bars = int(h['volatility'] * 5)  # Escalar para visualización
            bar_str = "▰" * min(bars, 20) + "▱" * max(0, 20 - min(bars, 20))

            # Color según nivel
            level_info = _interpret_volatility(h['volatility'])
            lines.append(
                f"{level_info['emoji']} {h['hour']} {bar_str} {h['volatility']:.1f} ({level_info['level'].lower()})")

    # Recomendación
    lines.extend([
        "",
        f"💡 <b>Recomendación:</b>",
        f"<i>{interp['advice']}</i>",
        "",
        f"📚 <b>¿Qué es volatilidad?</b>",
        f"Mide cuánto fluctúa el precio. {interp['desc']}",
        "",
        f"🔍 <i>Usa <code>/spread &gt;{1.0 if coef_var < 2 else 1.5}</code> para ver oportunidades</i>"
    ])

    meta = {
        "type": "volatility_analysis",
        "pair": pair,
        "coef_var": coef_var,
        "level": interp['level'],
        "confidence": max(0, 100 - coef_var*10)
    }

    return "\n".join(lines) + ai_meta(meta)
