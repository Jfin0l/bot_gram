"""Minimal aggregator: compute 10-minute buckets from RAM window and persist to DB.

Simple, single-node periodic flush. Intended for experimental validation only (V1).
"""
from datetime import datetime, timezone, timedelta
import threading
from core import ram_window, db


def _compute_bucket(snaps):
    prices = []
    volumes = []
    for s in snaps:
        for ad in s.ads:
            prices.append(ad.price)
            volumes.append(ad.quantity)
    if not prices:
        return None
    avg_price = sum(prices) / len(prices)
    min_price = min(prices)
    max_price = max(prices)
    total_vol = sum(volumes)
    # simple spread estimate: max_sell - min_buy across snapshots
    buys = [p for p, s in ((ad.price, ad.side) for snap in snaps for ad in snap.ads) if s == 'buy']
    sells = [p for p, s in ((ad.price, ad.side) for snap in snaps for ad in snap.ads) if s == 'sell']
    spread = None
    if buys and sells:
        spread = (max(sells) - min(buys)) / max(0.000001, min(buys)) * 100.0
    # volatility: sample stddev
    import math
    mean = avg_price
    var = sum((p - mean) ** 2 for p in prices) / max(1, len(prices))
    volatility = math.sqrt(var)
    return {
        'avg_price': avg_price,
        'min_price': min_price,
        'max_price': max_price,
        'volume': total_vol,
        'spread_pct': spread,
        'volatility': volatility,
        'sample_count': len(prices),
    }


class Aggregator:
    def __init__(self, window: ram_window.RamWindow, bucket_seconds: int = 600):
        self.window = window
        self.bucket_seconds = bucket_seconds
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread:
            return
        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        self._thread = t

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)

    def _run(self):
        while not self._stop.wait(self.bucket_seconds):
            try:
                self.flush_once()
            except Exception:
                pass

    def flush_once(self):
        now = datetime.now(timezone.utc)
        bucket_start = now.replace(second=0, microsecond=0) - timedelta(seconds=now.minute % (self.bucket_seconds // 60) * 60)
        # for each pair in RAM, aggregate last bucket_seconds
        with self.window.lock:
            for pair, dq in list(self.window.pair_index.items()):
                cutoff = now - timedelta(seconds=self.bucket_seconds)
                snaps = [s for s in dq if s.timestamp >= cutoff]
                if not snaps:
                    continue
                metrics = _compute_bucket(snaps)
                if metrics is None:
                    continue
                db.save_aggregated_price(
                    pair=pair,
                    bucket_start=bucket_start.isoformat(),
                    avg_price=metrics['avg_price'],
                    min_price=metrics['min_price'],
                    max_price=metrics['max_price'],
                    volume=metrics['volume'],
                    spread_pct=metrics['spread_pct'],
                    volatility=metrics['volatility'],
                    sample_count=metrics['sample_count'],
                )


_GLOBAL_AGG: Aggregator = None


def start_aggregator(window: ram_window.RamWindow, bucket_seconds: int = 600):
    global _GLOBAL_AGG
    if _GLOBAL_AGG is None:
        _GLOBAL_AGG = Aggregator(window, bucket_seconds=bucket_seconds)
        _GLOBAL_AGG.start()
    return _GLOBAL_AGG


def stop_aggregator():
    global _GLOBAL_AGG
    if _GLOBAL_AGG is not None:
        try:
            _GLOBAL_AGG.stop()
        except Exception:
            pass
        _GLOBAL_AGG = None
