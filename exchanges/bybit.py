# exchanges/bybit.py
from typing import List, Tuple, Dict, Any
from .interface import ExchangeInterface

class BybitExchange(ExchangeInterface):
    @property
    def name(self) -> str:
        return "Bybit"

    def get_ads(self, fiat: str, asset: str = "USDT", min_ads: int = 100) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        # Placeholder para implementación futura
        return [], []
