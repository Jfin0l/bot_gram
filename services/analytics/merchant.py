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
                    vol += ad.quantity
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
            lines.append(f"• `{m}`: {c} ads | {v:.1f} USDT total | {avg:.2f} USDT/ad")

        lines.append("\n💡 *Bots tipicamente: alta frecuencia + bajo volumen por ad*")
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
    # CASO 7: Perfil de merchant especifico (@nombre o nombre directo)
    # ===========================================
    if token.startswith('@'):
        name = token[1:].strip()
    else:
        name = token.strip()

    if not name:
        return (
            "⚠️ Comando no reconocido.\n\n"
            "**Comandos disponibles:**\n"
            "• `/merchant` - Top 10 global\n"
            "• `/merchant buy` - Top compradores\n"
            "• `/merchant sell` - Top vendedores\n"
            "• `/merchant bots` - Detectar bots\n"
            "• `/merchant grandes` - Volumen destacado\n"
            "• `/merchant search <texto>` - Buscar merchants\n"
            "• `/merchant @usuario` - Perfil de merchant"
        )

    return _build_merchant_profile(name, pair)
