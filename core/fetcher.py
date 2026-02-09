import logging
from adapters import binance_p2p
from core import db

log = logging.getLogger(__name__)


def fetch_and_store(exchange: str = "binance", fiats=None):
    """Realiza una única petición por fiat/tradeType y guarda la respuesta cruda en la DB."""
    if fiats is None:
        fiats = ["COP", "VES"]

    db.init_db()

    for fiat in fiats:
        for tt in ("BUY", "SELL"):
            try:
                # usamos el método interno que devuelve la lista 'data'
                raw = binance_p2p._fetch_ads(tt, fiat)
                db.save_raw_response(exchange, fiat, tt, raw)
                log.info(f"💾 Guardado raw {exchange} {fiat} {tt} ({len(raw)} items)")
            except Exception as e:
                log.error(f"Error fetch/store {exchange} {fiat} {tt}: {e}")
