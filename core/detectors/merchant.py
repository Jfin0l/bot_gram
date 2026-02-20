"""Detector de actividad anómala de merchants.

Comprueba si un `merchant` publica muchas ofertas en la ventana reciente y
registra un evento `merchant_activity` en la tabla `events`.

Usa un debouncer en memoria para evitar eventos repetidos frecuentes.
"""
from datetime import datetime, timezone
from typing import Optional, Dict

from core import app_config
from core import db

# simple in-memory debouncer: merchant -> last event ISO timestamp
_last_event: Dict[str, str] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_merchant_activity(ram_window, pair: str, latest_snapshot=None) -> Optional[dict]:
    cfg = app_config.DETECTORS.get('merchant', {})
    if not cfg.get('enabled', True):
        return None

    thresh = int(cfg.get('activity_count_threshold', 20) or 20)
    window_seconds = int(cfg.get('activity_window_seconds', 300) or 300)
    debounce = int(cfg.get('debounce_seconds', 300) or 300)

    # Focus on merchants present in the latest snapshot if provided, otherwise scan all merchants
    merchants = set()
    if latest_snapshot is not None:
        for ad in latest_snapshot.ads:
            merchants.add(ad.merchant)
    else:
        merchants = set(ram_window.merchant_index.keys())

    for m in merchants:
        stats = ram_window.get_merchant_activity(m, seconds=window_seconds)
        count = int(stats.get('count', 0) or 0)
        if count >= thresh:
            last_ts = _last_event.get(m)
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()
            if last_ts:
                try:
                    last_dt = datetime.fromisoformat(last_ts)
                    delta = (now - last_dt).total_seconds()
                    if delta < debounce:
                        # skip due to debounce
                        continue
                except Exception:
                    pass

            # severity proportional to how many times threshold was exceeded
            mult = count / thresh if thresh > 0 else 1.0
            if mult >= 3:
                severity = 3
            elif mult >= 2:
                severity = 2
            else:
                severity = 1

            details = {
                'merchant': m,
                'count': count,
                'buy': stats.get('buy', 0),
                'sell': stats.get('sell', 0),
                'window_seconds': window_seconds,
            }
            # include source snapshot timestamp if available
            if latest_snapshot is not None:
                try:
                    details['source_snapshot_ts'] = latest_snapshot.timestamp.isoformat()
                except Exception:
                    pass

            ts_iso = now_iso
            dedup = int(cfg.get('debounce_seconds', 300) or 300)
            # match by merchant so we don't duplicate same-merchant events
            db.save_event_dedup('merchant_activity', pair, ts_iso, details=details, severity=severity, dedup_seconds=dedup, match_details={'merchant': m})
            _last_event[m] = ts_iso
            return {'timestamp': ts_iso, 'pair': pair, 'merchant': m, 'severity': severity, 'details': details}

    return None
