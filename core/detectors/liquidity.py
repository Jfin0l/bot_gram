"""Detector de liquidez y desequilibrio simple.

Detecta bajos volúmenes agregados o desequilibrios entre sides y persiste
eventos en la tabla `events` usando `core.db.save_event`.
"""
from datetime import datetime, timezone
from typing import Optional

from core import app_config
from core import db


def detect_liquidity(ram_window, pair: str) -> Optional[dict]:
    cfg = app_config.DETECTORS.get('liquidity', {})
    if not cfg.get('enabled', True):
        return None

    vols = ram_window.get_liquidity(pair)
    buy_vol = float(vols.get('buy_volume') or 0.0)
    sell_vol = float(vols.get('sell_volume') or 0.0)

    buy_thresh = float(cfg.get('buy_volume_threshold', 10.0) or 10.0)
    sell_thresh = float(cfg.get('sell_volume_threshold', 10.0) or 10.0)
    imbalance_ratio_thr = float(cfg.get('imbalance_ratio', 10.0) or 10.0)

    details = {
        'buy_volume': buy_vol,
        'sell_volume': sell_vol,
        'buy_threshold': buy_thresh,
        'sell_threshold': sell_thresh,
    }

    now = datetime.now(timezone.utc).isoformat()

    # low liquidity on either side
    if buy_vol < buy_thresh or sell_vol < sell_thresh:
        # severity: 1 = low on one side, 2 = low on both sides
        severity = 2 if (buy_vol < buy_thresh and sell_vol < sell_thresh) else 1
        dedup = int(cfg.get('debounce_seconds', 300) or 300)
        db.save_event_dedup('low_liquidity', pair, now, details=details, severity=severity, dedup_seconds=dedup)
        return {'timestamp': now, 'pair': pair, 'event': 'low_liquidity', 'severity': severity, 'details': details}

    # imbalance detection
    minv = min(buy_vol, sell_vol) if (buy_vol > 0 and sell_vol > 0) else 0
    if minv > 0:
        ratio = max(buy_vol, sell_vol) / minv
        details['imbalance_ratio'] = ratio
        details['imbalance_ratio_threshold'] = imbalance_ratio_thr
        if ratio >= imbalance_ratio_thr:
            severity = 2 if ratio >= (imbalance_ratio_thr * 2) else 1
            dedup = int(cfg.get('debounce_seconds', 300) or 300)
            db.save_event_dedup('liquidity_imbalance', pair, now, details=details, severity=severity, dedup_seconds=dedup)
            return {'timestamp': now, 'pair': pair, 'event': 'liquidity_imbalance', 'severity': severity, 'details': details}

    return None
