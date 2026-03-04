import json
import statistics
from config import fmt


def format_num(val, dec=2):
    """Formatea número con separadores de miles y decimales fijos."""
    try:
        return f"{float(val):,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(val)


def format_vol(val):
    """Formatea volumen USDT (miles sin decimales si es grande)."""
    try:
        v = float(val)
        if v >= 1000:
            return f"{v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return str(val)


def ai_meta(data_dict: dict) -> str:
    """Genera un bloque de metadatos JSON para agentes IA, oculto tras spoiler."""
    try:
        json_str = json.dumps(data_dict, ensure_ascii=False)
        # Separar un poco y poner en spoiler para que sea discreto
        return f"\n\n<tg-spoiler>📦 <code>{json_str}</code></tg-spoiler>"
    except Exception:
        return ""


def analyze_ads(pair, buy_ads, sell_ads):
    """Calcula métricas básicas de spread y volumen"""
    if not buy_ads or not sell_ads:
        return None

    buy_prices = [float(ad["price"]) for ad in buy_ads]
    sell_prices = [float(ad["price"]) for ad in sell_ads]

    avg_buy = sum(buy_prices) / len(buy_prices)
    avg_sell = sum(sell_prices) / len(sell_prices)
    spread = ((avg_sell - avg_buy) / avg_buy) * 100

    return {
        "pair": pair,
        "avg_buy": avg_buy,
        "avg_sell": avg_sell,
        "spread_pct": spread,
        "count_buy": len(buy_ads),
        "count_sell": len(sell_ads)
    }
