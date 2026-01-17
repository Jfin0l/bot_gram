# core/processor.py
import statistics
from config import fmt

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
        "avg_buy": fmt(avg_buy),
        "avg_sell": fmt(avg_sell),
        "spread_pct": fmt(spread),
        "count_buy": len(buy_ads),
        "count_sell": len(sell_ads)
    }
