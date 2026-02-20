"""Detector simple de volatilidad basado en desviación estándar relativa.

Lee umbrales desde `core.app_config.DETECTORS['volatility']` y persiste eventos
usando `core.db.save_event`.
"""
from datetime import datetime, timezone
from typing import Optional

from core import app_config
from core import db


def _compute_mean_and_count(ram_window, pair: str):
    # accumulate mean and count by iterating snapshots for the pair
    total = 0.0
    count = 0
    with ram_window.lock:
        dq = ram_window.pair_index.get(pair, [])
        for snap in dq:
            for ad in snap.ads:
                try:
                    total += float(ad.price)
                    count += 1
                except Exception:
                    continue
    mean = (total / count) if count else None
    return mean, count


def detect_volatility(ram_window, pair: str) -> Optional[dict]:
    cfg = app_config.DETECTORS.get('volatility', {})
    if not cfg.get('enabled', True):
        return None

    stddev = ram_window.get_volatility(pair)
    mean, count = _compute_mean_and_count(ram_window, pair)
    min_samples = int(cfg.get('min_samples', 50) or 50)
    if stddev is None or mean is None or count < min_samples:
        return None

    stddev_pct = stddev / mean if mean > 0 else None
    threshold = float(cfg.get('stddev_pct_threshold', 0.02) or 0.02)
    if stddev_pct is None:
        return None

    if stddev_pct >= threshold:
        # severity scale: 1 = low, 2 = medium, 3 = high (based on multiples)
        mult = stddev_pct / threshold
        if mult >= 3:
            severity = 3
        elif mult >= 2:
            severity = 2
        else:
            severity = 1

        ts = datetime.now(timezone.utc).isoformat()
        details = {
            'stddev': stddev,
            'mean': mean,
            'stddev_pct': stddev_pct,
            'sample_count': count,
            'threshold': threshold,
        }
        # use dedup with detector-specific debounce
        dedup = int(cfg.get('debounce_seconds', 300) or 300)
        db.save_event_dedup('volatility', pair, ts, details=details, severity=severity, dedup_seconds=dedup)
        return {'timestamp': ts, 'pair': pair, 'severity': severity, 'details': details}

    return None
