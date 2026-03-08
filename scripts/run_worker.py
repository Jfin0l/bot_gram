#!/usr/bin/env python3
import logging
import threading
import time
from typing import Optional
from core.scheduler import start_scheduler
from core.app_config import CONFIG
from core import ram_window, aggregator
from adapters import binance_p2p

logger = logging.getLogger(__name__)


_ingest_thread: Optional[threading.Thread] = None
_ingest_stop_event: threading.Event = threading.Event()
_sched = None
_window = None


def _ingest_loop(window: ram_window.RamWindow, stop_event: threading.Event, interval: int = 60, min_rows: int = 100):
    """
    Loop de ingesta: obtiene anuncios por par y exchange y los añade a RAM.
    """
    from exchanges.factory import ExchangeFactory
    
    while not stop_event.wait(0):
        try:
            # Por ahora solo procesamos Binance, pero la estructura ya permite expansión
            exchanges = ["binance"] 
            for ex_name in exchanges:
                ex_instance = ExchangeFactory.get_exchange(ex_name)
                
                for pair in CONFIG.get('pares', []):
                    fiat = pair.split('-')[1]
                    try:
                        buy_ads, sell_ads = ex_instance.get_ads(fiat=fiat, min_ads=min_rows)
                        
                        # Combinar y normalizar etiquetas
                        ads = []
                        for a in buy_ads:
                            a['side'] = 'buy' # asegurar normalización
                            ads.append(a)
                        for a in sell_ads:
                            a['side'] = 'sell'
                            ads.append(a)
                            
                        window.append_snapshot(pair, ads, exchange=ex_name)
                    except Exception as e:
                        logger.error(f"Error ingestando {pair} desde {ex_name}: {e}")
                        
        except Exception as e:
            logger.exception("Error en ingest loop: %s", e)
            
        if stop_event.wait(interval):
            break


def start_worker(fetch_interval: int = 300, snapshot_interval: int = 600, ingest_interval: int = 60, ingest_min_rows: int = 100):
    """Start scheduler, RAM window, aggregator and ingest thread."""
    global _ingest_thread, _ingest_stop_event, _sched, _window
    if _sched is not None:
        return
    _sched = start_scheduler(
        CONFIG, fetch_interval=fetch_interval, snapshot_interval=snapshot_interval)
    _window = ram_window.init_global(window_seconds=6 * 3600)
    aggregator.start_aggregator(_window, bucket_seconds=600)

    _ingest_stop_event.clear()
    _ingest_thread = threading.Thread(target=_ingest_loop, args=(
        _window, _ingest_stop_event, ingest_interval, ingest_min_rows), daemon=False)
    _ingest_thread.start()


def stop_worker():
    """Stop ingest, aggregator, ram window and scheduler."""
    global _ingest_thread, _ingest_stop_event, _sched, _window
    try:
        if _ingest_stop_event is not None:
            _ingest_stop_event.set()
    except Exception as e:
        logger.warning("Error setting ingest stop event: %s", e)
    try:
        if _ingest_thread is not None:
            _ingest_thread.join(timeout=5)
    except Exception as e:
        logger.warning("Error joining ingest thread: %s", e)
    try:
        aggregator.stop_aggregator()
    except Exception as e:
        logger.warning("Error stopping aggregator: %s", e)
    try:
        ram_window.stop_global()
    except Exception as e:
        logger.warning("Error stopping ram_window: %s", e)
    try:
        if _sched is not None:
            _sched.shutdown()
    except Exception as e:
        logger.warning("Error shutting down scheduler: %s", e)
    _ingest_thread = None
    _sched = None
    _window = None


def main():
    # start worker components
    start_worker()

    try:
        # blocking loop
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        stop_worker()


if __name__ == "__main__":
    main()
