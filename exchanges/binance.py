# exchanges/binance.py
import requests
import logging
from typing import List, Tuple, Dict, Any
from .interface import ExchangeInterface

log = logging.getLogger(__name__)

class BinanceExchange(ExchangeInterface):
    @property
    def name(self) -> str:
        return "Binance"

    def get_ads(self, fiat: str, asset: str = "USDT") -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        buy_data = self._fetch_ads("BUY", fiat, asset)
        sell_data = self._fetch_ads("SELL", fiat, asset)
        
        return self._simplify(buy_data, "buy"), self._simplify(sell_data, "sell")

    def _fetch_ads(self, tradeType: str, fiat: str, asset: str, max_pages: int = 5) -> List[Dict]:
        url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
        }
        collected = []
        try:
            for page in range(1, max_pages + 1):
                payload = {
                    "page": page,
                    "rows": 10,
                    "asset": asset,
                    "tradeType": tradeType,
                    "fiat": fiat,
                    "publisherType": None,
                    "merchantCheck": False
                }
                r = requests.post(url, headers=headers, json=payload, timeout=10)
                r.raise_for_status()
                data = r.json().get("data", [])
                if not data: break
                collected.extend(data)
                if len(collected) >= 50: break
            return collected
        except Exception as e:
            log.error(f"❌ Error en Binance {tradeType}-{fiat}: {e}")
            return collected

    def _simplify(self, raw_list: List[Dict], side: str) -> List[Dict]:
        ads = []
        for item in raw_list:
            try:
                adv = item.get("adv", {})
                advertiser = item.get("advertiser", {})
                ads.append({
                    'price': float(adv["price"]),
                    'quantity': float(adv.get("tradableQuantity") or adv.get("surplusAmount") or 0),
                    'merchant_name': advertiser.get("nickName", advertiser.get("nick", "N/A")),
                    'min_limit': float(adv.get("minSingleTransAmount") or 0),
                    'max_limit': float(adv.get("dynamicMaxSingleTransAmount") or 0),
                    'payment_method': ", ".join([m.get("tradeMethodName", "") for m in adv.get("tradeMethods", [])]),
                    'side': side
                })
            except Exception:
                continue
        return ads
