#!/usr/bin/env python3
from core.scheduler import start_scheduler
from p2p_info import CONFIG


def main():
    sched = start_scheduler(CONFIG, fetch_interval=600, snapshot_interval=600)
    try:
        # blocking loop
        import time
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()


if __name__ == "__main__":
    main()
