from __future__ import annotations
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Callable, Tuple
import threading
import traceback

# detectors (optional imports)
try:
    from core.detectors.volatility import detect_volatility
except Exception:
    detect_volatility = None
try:
    from core.detectors.liquidity import detect_liquidity
except Exception:
    detect_liquidity = None
try:
    from core.detectors.merchant import detect_merchant_activity
except Exception:
    detect_merchant_activity = None


@dataclass
class Ad:
    price: float
    quantity: float
    merchant: str
    side: str  # 'buy' or 'sell'
    min_limit: float
    max_limit: float
    payment_method: str
    id_hint: Optional[str] = None


@dataclass
class Snapshot:
    timestamp: datetime
    pair: str
    ads: List[Ad] = field(default_factory=list)


@dataclass
class MetricsCache:
    top_buy_price: Optional[float] = None
    top_sell_price: Optional[float] = None
    total_volume: float = 0.0
    sample_count: int = 0
    mean: float = 0.0
    m2: float = 0.0  # for variance (Welford)

    def update_with_snapshot(self, snapshot: Snapshot):
        # update simple metrics from snapshot
        buys = [ad for ad in snapshot.ads if ad.side == 'buy']
        sells = [ad for ad in snapshot.ads if ad.side == 'sell']
        if buys:
            min_buy = min(ad.price for ad in buys)
            if self.top_buy_price is None or min_buy < self.top_buy_price:
                self.top_buy_price = min_buy
        if sells:
            max_sell = max(ad.price for ad in sells)
            if self.top_sell_price is None or max_sell > self.top_sell_price:
                self.top_sell_price = max_sell
        vol = sum(ad.quantity for ad in snapshot.ads)
        self.total_volume += vol
        # update Welford (per-snapshot mean)
        for ad in snapshot.ads:
            self.sample_count += 1
            delta = ad.price - self.mean
            self.mean += delta / self.sample_count
            delta2 = ad.price - self.mean
            self.m2 += delta * delta2

    def evict_snapshot(self, snapshot: Snapshot):
        # best-effort decremental; accurate full recalculation done in compaction
        vol = sum(ad.quantity for ad in snapshot.ads)
        self.total_volume = max(0.0, self.total_volume - vol)
        # decremental Welford is complex; mark for compaction if needed

    def variance(self) -> Optional[float]:
        if self.sample_count < 2:
            return None
        return self.m2 / (self.sample_count - 1)


class RamWindow:
    def __init__(self, window_seconds: int = 6 * 3600):
        self.window_seconds = window_seconds
        self.snapshots: deque[Snapshot] = deque()
        self.pair_index: Dict[str, deque[Snapshot]] = {}
        self.merchant_index: Dict[str, deque[Tuple[datetime, Ad]]] = {}
        self.cache_metrics: Dict[str, MetricsCache] = {}
        self.lock = threading.RLock()
        self._stop_event = threading.Event()
        self._compaction_interval = 60
        self._aggregator_thread: Optional[threading.Thread] = None

    def append_snapshot(self, pair: str, ads: List[dict], timestamp: Optional[datetime] = None):
        ts = timestamp or datetime.now(timezone.utc)
        ad_objs = []
        for a in ads:
            try:
                ad = Ad(
                    price=float(a.get('price')),
                    quantity=float(a.get('quantity', 0) or 0),
                    merchant=str(a.get('merchant_name') or a.get('nick') or 'unknown'),
                    side=str(a.get('side') or a.get('tradeType', '')).lower(),
                    min_limit=float(a.get('min_limit', a.get('min') or 0) or 0),
                    max_limit=float(a.get('max_limit', a.get('max') or 0) or 0),
                    payment_method=str(a.get('payment_method') or a.get('payMethods') or ''),
                    id_hint=a.get('id') or a.get('advId')
                )
                ad_objs.append(ad)
            except Exception:
                continue

        snap = Snapshot(timestamp=ts, pair=pair, ads=ad_objs)

        with self.lock:
            self.snapshots.append(snap)
            self.pair_index.setdefault(pair, deque()).append(snap)
            for ad in ad_objs:
                self.merchant_index.setdefault(ad.merchant, deque()).append((ts, ad))
            # update metrics cache
            mc = self.cache_metrics.setdefault(pair, MetricsCache())
            mc.update_with_snapshot(snap)
            self._evict_old_locked()
            # run detectors in background to avoid blocking ingestion
            try:
                t = threading.Thread(target=self._run_detectors, args=(pair, ts, snap), daemon=True)
                t.start()
            except Exception:
                pass

    def _run_detectors(self, pair: str, ts: datetime, snap: Snapshot):
        # Run available detectors; each detector should be robust and use core.db.save_event
        try:
            if detect_volatility:
                try:
                    detect_volatility(self, pair)
                except Exception:
                    traceback.print_exc()
            if detect_liquidity:
                try:
                    detect_liquidity(self, pair)
                except Exception:
                    traceback.print_exc()
            if detect_merchant_activity:
                try:
                    # optionally pass the latest snapshot for merchant detector
                    detect_merchant_activity(self, pair, latest_snapshot=snap)
                except Exception:
                    traceback.print_exc()
        except Exception:
            # defensive: do not let detector failures bubble up
            traceback.print_exc()

    def _evict_old_locked(self):
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.window_seconds)
        while self.snapshots and self.snapshots[0].timestamp < cutoff:
            old = self.snapshots.popleft()
            # remove from pair_index
            dq = self.pair_index.get(old.pair)
            if dq:
                try:
                    if dq[0] is old:
                        dq.popleft()
                except Exception:
                    pass
            # prune merchant_index entries older than cutoff
            for merchant, mdq in list(self.merchant_index.items()):
                while mdq and mdq[0][0] < cutoff:
                    mdq.popleft()
                if not mdq:
                    self.merchant_index.pop(merchant, None)
            # update cache metrics
            mc = self.cache_metrics.get(old.pair)
            if mc:
                mc.evict_snapshot(old)

    def get_live_spread(self, pair: str) -> Optional[float]:
        with self.lock:
            mc = self.cache_metrics.get(pair)
            if not mc or mc.top_buy_price is None or mc.top_sell_price is None:
                return None
            return mc.top_sell_price - mc.top_buy_price

    def get_merchant_activity(self, merchant: str, seconds: int = 300) -> Dict[str, int]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=seconds)
        with self.lock:
            dq = self.merchant_index.get(merchant, deque())
            count = 0
            buy = 0
            sell = 0
            for ts, ad in reversed(dq):
                if ts < cutoff:
                    break
                count += 1
                if ad.side == 'buy':
                    buy += 1
                elif ad.side == 'sell':
                    sell += 1
            return {'merchant': merchant, 'count': count, 'buy': buy, 'sell': sell}

    def get_liquidity(self, pair: str, min_price: Optional[float] = None, max_price: Optional[float] = None) -> Dict[str, float]:
        with self.lock:
            dq = self.pair_index.get(pair, deque())
            buy_vol = 0.0
            sell_vol = 0.0
            for snap in dq:
                for ad in snap.ads:
                    if min_price is not None and ad.price < min_price:
                        continue
                    if max_price is not None and ad.price > max_price:
                        continue
                    if ad.side == 'buy':
                        buy_vol += ad.quantity
                    elif ad.side == 'sell':
                        sell_vol += ad.quantity
            return {'pair': pair, 'buy_volume': buy_vol, 'sell_volume': sell_vol}

    def get_volatility(self, pair: str) -> Optional[float]:
        with self.lock:
            mc = self.cache_metrics.get(pair)
            if not mc:
                return None
            var = mc.variance()
            if var is None:
                return None
            import math

            return math.sqrt(var)

    def start_periodic_compaction(self):
        def run():
            while not self._stop_event.wait(self._compaction_interval):
                with self.lock:
                    # full recompute for caches to avoid drift
                    for pair in list(self.pair_index.keys()):
                        snaps = list(self.pair_index.get(pair, []))
                        mc = MetricsCache()
                        for s in snaps:
                            mc.update_with_snapshot(s)
                        self.cache_metrics[pair] = mc

        t = threading.Thread(target=run, daemon=True)
        t.start()
        self._aggregator_thread = t

    def stop(self):
        self._stop_event.set()
        if self._aggregator_thread:
            self._aggregator_thread.join(timeout=1)


# simple module-level singleton for convenience
_GLOBAL_WINDOW: Optional[RamWindow] = None

def init_global(window_seconds: int = 6 * 3600) -> RamWindow:
    global _GLOBAL_WINDOW
    if _GLOBAL_WINDOW is None:
        _GLOBAL_WINDOW = RamWindow(window_seconds=window_seconds)
        _GLOBAL_WINDOW.start_periodic_compaction()
    return _GLOBAL_WINDOW


def get_global() -> RamWindow:
    global _GLOBAL_WINDOW
    if _GLOBAL_WINDOW is None:
        # lazy-init global window with default timeout
        return init_global()
    return _GLOBAL_WINDOW


def stop_global():
    global _GLOBAL_WINDOW
    if _GLOBAL_WINDOW is not None:
        try:
            _GLOBAL_WINDOW.stop()
        except Exception:
            pass
        _GLOBAL_WINDOW = None
