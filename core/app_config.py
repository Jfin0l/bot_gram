"""Centralized app and detectors configuration.

Defaults are provided and can be overridden with environment variables.
Keep this file small and dependency-free so other modules can import it safely.
"""
from dataclasses import dataclass
import os
import json
from typing import Any, Dict


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "y")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


# Basic app-level defaults
WINDOW_SECONDS = _env_int("WINDOW_SECONDS", 6 * 3600)
INGEST_MIN_ROWS = _env_int("INGEST_MIN_ROWS", 100)


# Detector defaults. These are intentionally simple threshold-based configs.
DETECTORS: Dict[str, Any] = {
    "volatility": {
        "enabled": _env_bool("DETECTOR_VOLATILITY_ENABLED", True),
        # percent (fraction) threshold on price stddev / mean to consider volatile
        "stddev_pct_threshold": _env_float("DETECTOR_VOLATILITY_STDDEV_PCT", 0.02),
        "min_samples": _env_int("DETECTOR_VOLATILITY_MIN_SAMPLES", 50),
        "debounce_seconds": _env_int("DETECTOR_VOLATILITY_DEBOUNCE", 300),
    },
    "liquidity": {
        "enabled": _env_bool("DETECTOR_LIQUIDITY_ENABLED", True),
        # absolute volume thresholds per pair over the window
        "buy_volume_threshold": _env_float("DETECTOR_LIQUIDITY_BUY_VOL", 10.0),
        "sell_volume_threshold": _env_float("DETECTOR_LIQUIDITY_SELL_VOL", 10.0),
        "debounce_seconds": _env_int("DETECTOR_LIQUIDITY_DEBOUNCE", 300),
    },
    "merchant": {
        "enabled": _env_bool("DETECTOR_MERCHANT_ENABLED", True),
        # count of ads from the same merchant in recent seconds to flag
        "activity_count_threshold": _env_int("DETECTOR_MERCHANT_COUNT", 20),
        "activity_window_seconds": _env_int("DETECTOR_MERCHANT_WINDOW", 300),
        "debounce_seconds": _env_int("DETECTOR_MERCHANT_DEBOUNCE", 300),
    },
}


def load_extra_from_env() -> None:
    """Optional: load a JSON blob from `DETECTORS_CONFIG_JSON` env var to override.

    Example: export DETECTORS_CONFIG_JSON='{"volatility": {"stddev_pct_threshold": 0.03}}'
    """
    blob = os.getenv("DETECTORS_CONFIG_JSON")
    if not blob:
        return
    try:
        data = json.loads(blob)
        for k, v in data.items():
            if k in DETECTORS and isinstance(v, dict):
                DETECTORS[k].update(v)
    except Exception:
        # be permissive; callers can log if they want
        pass


# run-time load
load_extra_from_env()


def get_config() -> Dict[str, Any]:
    return {
        "window_seconds": WINDOW_SECONDS,
        "ingest_min_rows": INGEST_MIN_ROWS,
        "detectors": DETECTORS,
    }


# Unified CONFIG — single source of truth for the entire application.
# Previously duplicated in telegram_bot_main.py and scripts/run_notifier.py.
CONFIG = {
    "pares": ["USDT-COP", "USDT-VES"],
    "monedas": {"COP": {"rows": 20, "page": 3}, "VES": {"rows": 20, "page": 5}},
    "filas_tasa_remesa": 5,
    "ponderacion_volumen": True,
    "limite_outlier": 0.025,
    "umbral_volatilidad": 3,
}

__all__ = ["get_config", "WINDOW_SECONDS", "INGEST_MIN_ROWS", "DETECTORS", "CONFIG"]
