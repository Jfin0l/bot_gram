# adapters/binance_p2p.py
import requests
import logging

log = logging.getLogger(__name__)

def _fetch_ads(tradeType: str, fiat: str):
    """
    Llama al endpoint oficial de Binance P2P y devuelve la lista 'data'.
    """
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
    }

    payload = {
        "page": 1,
        "rows": 10,
        "asset": "USDT",
        "tradeType": tradeType,
        "fiat": fiat,
        "publisherType": None,
        "merchantCheck": False
    }

    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        log.info(f"🔹 {len(data)} anuncios {tradeType} obtenidos ({fiat})")
        return data
    except Exception as e:
        log.error(f"❌ Error al obtener {tradeType}-{fiat}: {e}")
        return []


def get_ads(fiat="COP"):
    """Devuelve (buy_ads, sell_ads) simplificados."""
    buy_data = _fetch_ads("BUY", fiat)
    sell_data = _fetch_ads("SELL", fiat)

    def simplify(raw_list):
        ads = []
        for adv in raw_list:
            try:
                ad = adv["adv"]
                advertiser = adv.get("advertiser", {})
                ads.append({
                    "price": round(float(ad["price"]), 1),
                    "min": float(ad["minSingleTransAmount"]),
                    "max": float(ad["dynamicMaxSingleTransAmount"]),
                    "nick": advertiser.get("nickName", "N/A"),
                    "orders": advertiser.get("monthOrderCount", 0),
                    "rate": advertiser.get("monthFinishRate", "0%")
                })
            except Exception as e:
                log.warning(f"Formato inesperado: {e}")
        return ads

    buy_ads = simplify(buy_data)
    sell_ads = simplify(sell_data)

    log.info(f"✅ {len(buy_ads)} BUY y {len(sell_ads)} SELL ({fiat})")
    return buy_ads, sell_ads