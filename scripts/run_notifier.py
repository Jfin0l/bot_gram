#!/usr/bin/env python3
from core.notifier import start_notifier_scheduler
from core.app_config import CONFIG


def main():
    # dry_run=True evita enviar mensajes reales si no se configuró BOT_TOKEN
    sched = start_notifier_scheduler(CONFIG, interval=3600, dry_run=True)
    try:
        import time
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()


if __name__ == '__main__':
    main()
