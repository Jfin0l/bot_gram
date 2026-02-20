# adapters/binance_p2p.py
import requests
import logging

log = logging.getLogger(__name__)

def _fetch_ads(tradeType: str, fiat: str, min_rows: int = 100, max_pages: int = 3):
    """
    Llama al endpoint oficial de Binance P2P y devuelve la lista 'data'.
    """
    url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
    }

    collected = []
    # Decide rows per page; some endpoints limit to small numbers (10), so use 50 as a reasonable per-page size
    rows_per_page = 50
    try:
        for page in range(1, max_pages + 1):
            payload = {
                "page": page,
                "rows": rows_per_page,
                "asset": "USDT",
                "tradeType": tradeType,
                "fiat": fiat,
                "publisherType": None,
                "merchantCheck": False
            }
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json().get("data", [])
            if not data:
                break
            collected.extend(data)
            log.info(f"🔹 page {page}: {len(data)} anuncios {tradeType} obtenidos ({fiat})")
            if len(collected) >= min_rows:
                break

        log.info(f"🔹 total {len(collected)} anuncios {tradeType} recolectados ({fiat})")
        return collected
    except Exception as e:
        log.error(f"❌ Error al obtener {tradeType}-{fiat}: {e}")
        return collected


def get_ads(fiat="COP", min_rows: int = 100, max_pages: int = 3):
    """Devuelve (buy_ads, sell_ads) simplificados.
    Intenta paginar hasta `min_rows` anuncios por lado, con un máximo de `max_pages` páginas.
    """
    buy_data = _fetch_ads("BUY", fiat, min_rows=min_rows, max_pages=max_pages)
    sell_data = _fetch_ads("SELL", fiat, min_rows=min_rows, max_pages=max_pages)

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