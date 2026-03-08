# exchanges/interface.py
from abc import ABC, abstractmethod
from typing import List, Tuple, Dict, Any

class ExchangeInterface(ABC):
    @abstractmethod
    def get_ads(self, fiat: str, asset: str = "USDT", min_ads: int = 100) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Debe retornar una tupla (buy_ads, sell_ads).
        Cada anuncio debe ser un diccionario con la estructura:
        {
            'price': float,
            'quantity': float,
            'merchant_name': str,
            'min_limit': float,
            'max_limit': float,
            'payment_method': str,
            'side': str ('buy' o 'sell')
        }
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Nombre amigable del exchange."""
        pass
