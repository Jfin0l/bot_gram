# exchanges/factory.py
from typing import Dict
from .interface import ExchangeInterface
from .binance import BinanceExchange
from .bybit import BybitExchange
from .okx import OkxExchange

class ExchangeFactory:
    _exchanges: Dict[str, ExchangeInterface] = {
        "binance": BinanceExchange(),
        "bybit": BybitExchange(),
        "okx": OkxExchange()
    }

    @classmethod
    def get_exchange(cls, name: str) -> ExchangeInterface:
        name = name.lower()
        if name not in cls._exchanges:
            raise ValueError(f"Exchange '{name}' no soportado.")
        return cls._exchanges[name]

    @classmethod
    def list_exchanges(cls) -> list:
        return list(cls._exchanges.keys())
