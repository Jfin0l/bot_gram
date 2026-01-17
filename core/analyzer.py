# core/analyzer.py
import numpy as np
from datetime import datetime, UTC

def analyze_ads(pair: str, buy_ads: list, sell_ads: list):
    """Analiza los anuncios y devuelve métricas resumen para el snapshot."""
    if not buy_ads or not sell_ads:
        return None

    buy_prices = [float(ad["price"]) for ad in buy_ads]
    sell_prices = [float(ad["price"]) for ad in sell_ads]

    avg_buy = np.mean(buy_prices)
    avg_sell = np.mean(sell_prices)
    spread = (avg_sell - avg_buy) / avg_buy * 100

    # ponderado por volumen
    buy_weights = [float(ad["dynamicMaxSingleTransAmount"]) for ad in buy_ads]
    sell_weights = [float(ad["dynamicMaxSingleTransAmount"]) for ad in sell_ads]

    avg_buy_weighted = np.average(buy_prices, weights=buy_weights)
    avg_sell_weighted = np.average(sell_prices, weights=sell_weights)

    total_volume = sum(buy_weights + sell_weights)
    coef_var = np.std(buy_prices + sell_prices) / np.mean(buy_prices + sell_prices)

    top_buy = buy_ads[0]
    top_sell = sell_ads[0]

    result = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "pair": pair,
        "rows_fetched": len(buy_ads) + len(sell_ads),
        "avg_price_simple": round((avg_buy + avg_sell) / 2, 2),
        "avg_price_weighted": round((avg_buy_weighted + avg_sell_weighted) / 2, 2),
        "spread_pct": round(spread, 2),
        "coef_var": round(coef_var, 4),
        "total_exposed_volume": round(total_volume, 2),
        "top1_price": float(top_sell["price"]),
        "top1_vol": float(top_sell["dynamicMaxSingleTransAmount"]),
        "top1_nick": top_sell["nickName"],
        "top3_prices": str([float(ad["price"]) for ad in sell_ads[:3]]),
        "arb_estimate_cop_to_ves_pct": 0,
        "arb_estimate_ves_to_cop_pct": 0
    }
    return result