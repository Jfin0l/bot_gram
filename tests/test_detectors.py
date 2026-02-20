import time
from datetime import datetime, timezone

from core.ram_window import RamWindow


def make_ads(n_per_side: int = 100, price_start: float = 100.0, spread: float = 10.0):
    ads = []
    for i in range(n_per_side):
        ads.append({
            'price': price_start + i * 0.01,
            'quantity': 1.0,
            'merchant_name': f'merchant_buy_{i}',
            'side': 'buy',
            'min': 1,
            'max': 1000,
            'id': f'b{i}',
        })
        ads.append({
            'price': price_start + spread + i * 0.01,
            'quantity': 1.0,
            'merchant_name': f'merchant_sell_{i}',
            'side': 'sell',
            'min': 1,
            'max': 1000,
            'id': f's{i}',
        })
    return ads


def test_detectors_smoke():
    rw = RamWindow(window_seconds=60)
    ads = make_ads(100)
    rw.append_snapshot('COP-VES', ads, timestamp=datetime.now(timezone.utc))

    # import detectors lazily so tests only fail on real errors
    from core.detectors.volatility import detect_volatility
    from core.detectors.liquidity import detect_liquidity
    from core.detectors.merchant import detect_merchant_activity

    v = detect_volatility(rw, 'COP-VES')
    assert v is None or isinstance(v, dict)
    l = detect_liquidity(rw, 'COP-VES')
    assert l is None or isinstance(l, dict)
    m = detect_merchant_activity(rw, 'COP-VES', latest_snapshot=rw.snapshots[-1])
    assert m is None or isinstance(m, dict)
