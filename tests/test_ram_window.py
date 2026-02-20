import time
from datetime import datetime, timezone

from core.ram_window import RamWindow


def test_ram_window_eviction():
    rw = RamWindow(window_seconds=1)
    # append 3 snapshots with small sleep to cause eviction
    for i in range(3):
        ads = [{'price': 100.0 + i, 'quantity': 1, 'merchant_name': f'm{i}', 'side': 'buy', 'id': str(i)}]
        rw.append_snapshot('COP-VES', ads, timestamp=datetime.now(timezone.utc))
        time.sleep(0.6)

    # after sleeps, oldest should be evicted and snapshots length <= 2
    assert len(rw.snapshots) <= 2
