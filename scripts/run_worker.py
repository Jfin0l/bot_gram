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


def _ingest_loop(window: ram_window.RamWindow, stop_event: threading.Event, interval: int = 120, min_rows: int = 100):
    """Simple ingest loop: fetch ads per pair and append to RAM every `interval` seconds.
    Stops when `stop_event` is set.
    """
    while not stop_event.wait(0):
        try:
            for pair in CONFIG.get('pares', []):
                fiat = pair.split('-')[1]
                # allow more pages (10 ads per page) to reach ~min_rows target
                buy_ads, sell_ads = binance_p2p.get_ads(
                    fiat=fiat, min_rows=min_rows, max_pages=12)
                ads = []
                for a in buy_ads:
                    ads.append({
                        'price': a.get('price'),
                        'quantity': a.get('quantity') or 0,
                        'merchant_name': a.get('nick') or a.get('merchant'),
                        'side': 'buy',
                        'min_limit': a.get('min') or a.get('min_limit') or 0,
                        'max_limit': a.get('max') or a.get('max_limit') or 0,
                        'payment_method': a.get('payment_method') or ''
                    })
                for a in sell_ads:
                    ads.append({
                        'price': a.get('price'),
                        'quantity': a.get('quantity') or 0,
                        'merchant_name': a.get('nick') or a.get('merchant'),
                        'side': 'sell',
                        'min_limit': a.get('min') or a.get('min_limit') or 0,
                        'max_limit': a.get('max') or a.get('max_limit') or 0,
                        'payment_method': a.get('payment_method') or ''
                    })
                window.append_snapshot(pair, ads)
        except Exception as e:
            logger.exception("Error en ingest loop: %s", e)
        # wait with early exit
        if stop_event.wait(interval):
            break


def start_worker(fetch_interval: int = 300, snapshot_interval: int = 600, ingest_interval: int = 120, ingest_min_rows: int = 100):
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
