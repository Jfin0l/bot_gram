#!/usr/bin/env python3
"""Prueba de validación RAM: genera snapshots sintéticos (>=100 ads por lado),
los inserta en `RamWindow`, espera la ventana para forzar eviction y mide
resumen de huella usando `tracemalloc`.
"""
import time
import tracemalloc
import gc
from datetime import datetime, timezone

from core.ram_window import RamWindow
from core.detectors.volatility import detect_volatility
from core.detectors.liquidity import detect_liquidity


def make_ads(n_per_side: int = 100):
    ads = []
    for i in range(n_per_side):
        ads.append({
            'price': 100.0 + i * 0.01,
            'quantity': 1.0,
            'merchant_name': f'merchant_buy_{i}',
            'side': 'buy',
            'min': 1,
            'max': 1000,
            'id': f'b{i}',
        })
        ads.append({
            'price': 110.0 + i * 0.01,
            'quantity': 1.0,
            'merchant_name': f'merchant_sell_{i}',
            'side': 'sell',
            'min': 1,
            'max': 1000,
            'id': f's{i}',
        })
    return ads


def print_top_stats(snap_a, snap_b, limit=10):
    stats = snap_b.compare_to(snap_a, 'lineno')
    print('\nTop memory differences:')
    for s in stats[:limit]:
        print(s)


def run():
    print('Inicio prueba RAM/eviction')
    # ventana pequeña para forzar eviction durante la prueba
    rw = RamWindow(window_seconds=5)

    tracemalloc.start()
    base = tracemalloc.take_snapshot()

    N = 12
    print(f'Append {N} snapshots, cada una con ~200 ads (100 buy + 100 sell)')
    for i in range(N):
        ads = make_ads(100)
        rw.append_snapshot('COP-VES', ads, timestamp=datetime.now(timezone.utc))
        print(f'  appended {i+1}/{N} -> snapshots_in_memory={len(rw.snapshots)}')
        # pequeña pausa para que timestamps avancen
        time.sleep(0.5)

    print('Esperando para permitir eviction (sleep window + 1s)')
    time.sleep(6)
    gc.collect()

    after = tracemalloc.take_snapshot()
    print(f'Final: snapshots in window: {len(rw.snapshots)}')
    vol = rw.get_liquidity('COP-VES')
    print(f'Liquidity summary: buy_volume={vol.get("buy_volume")}, sell_volume={vol.get("sell_volume")}')
    spread = rw.get_live_spread('COP-VES')
    print(f'Live spread (if available): {spread}')

    # run volatility and liquidity detectors as smoke
    evt_v = detect_volatility(rw, 'COP-VES')
    print('Volatility detector result:', evt_v)
    evt_l = detect_liquidity(rw, 'COP-VES')
    print('Liquidity detector result:', evt_l)

    print_top_stats(base, after, limit=12)
    tracemalloc.stop()


if __name__ == '__main__':
    run()
