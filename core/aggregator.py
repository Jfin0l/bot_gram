"""Minimal aggregator: compute 10-minute buckets from RAM window and persist to DB.

Simple, single-node periodic flush. Intended for experimental validation only (V1).
"""
from datetime import datetime, timezone, timedelta
import threading
from core import ram_window, db


def _compute_bucket(snaps):
    prices = []
    volumes = []

    # Metricas de spread por snapshot
    snapshot_spreads = []
    total_volumes = []

    for s in snaps:
        # Ordenar ads por side
        buys = sorted([ad for ad in s.ads if ad.side == 'buy'],
                      key=lambda x: x.price, reverse=True)
        sells = sorted([ad for ad in s.ads if ad.side ==
                       'sell'], key=lambda x: x.price)

        # Calcular spread promedio de los primeros 50 para este snapshot
        n_limit = min(50, len(buys), len(sells))
        if n_limit > 0:
            sp_list = []
            for i in range(n_limit):
                if sells[i].price > 0:
                    sp_list.append(
                        ((buys[i].price - sells[i].price) / sells[i].price) * 100.0)
            if sp_list:
                snapshot_spreads.append(sum(sp_list) / len(sp_list))

        total_vol_snap = sum(ad.quantity for ad in s.ads)
        total_volumes.append(total_vol_snap)

        for ad in s.ads:
            prices.append(ad.price)
            volumes.append(ad.quantity)

    if not prices:
        return None

    avg_price = sum(prices) / len(prices)
    min_price = min(prices)
    max_price = max(prices)
    total_vol = sum(volumes)

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
        'spread_pct_bucket': sum(snapshot_spreads) / len(snapshot_spreads) if snapshot_spreads else 0,
        'volatility': volatility,
        'sample_count': len(prices),
        'total_exposed_vol': sum(total_volumes) / len(total_volumes) if total_volumes else 0
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
        bucket_start = now.replace(second=0, microsecond=0) - timedelta(
            seconds=now.minute % (self.bucket_seconds // 60) * 60)
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
                    spread_pct=metrics['spread_pct_bucket'],
                    volatility=metrics['volatility'],
                    sample_count=metrics['sample_count'],
                )

                # Guardar metricas financieras historicas
                db.save_market_metric(
                    pair, 'avg_spread_top50', metrics['spread_pct_bucket'])
                db.save_market_metric(
                    pair, 'total_volume', metrics['total_exposed_vol'])


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
