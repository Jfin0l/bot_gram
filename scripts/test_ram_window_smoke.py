#!/usr/bin/env python3
"""Simple smoke test for core.ram_window"""
from datetime import datetime, timezone, timedelta
from core.ram_window import init_global, get_global


def main():
    rw = init_global(window_seconds=60)
    now = datetime.now(timezone.utc)
    ads = []
    for i in range(10):
        ads.append({
            'price': 100 + i,
            'quantity': 1.0 + i * 0.1,
            'merchant_name': f'merchant_{i%3}',
            'side': 'buy' if i % 2 == 0 else 'sell',
            'min_limit': 1,
            'max_limit': 1000,
            'payment_method': 'P2P'
        })

    # append three snapshots spaced 1s apart
    for j in range(3):
        ts = now - timedelta(seconds=3 - j)
        rw.append_snapshot('USDT-COP', ads, timestamp=ts)

    print('Live spread:', rw.get_live_spread('USDT-COP'))
    print('Merchant activity merchant_1:', rw.get_merchant_activity('merchant_1', seconds=60))
    print('Liquidity:', rw.get_liquidity('USDT-COP'))
    print('Volatility:', rw.get_volatility('USDT-COP'))


if __name__ == '__main__':
    main()
