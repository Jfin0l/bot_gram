#!/usr/bin/env python3
"""
Runner para el bot Telegram con complementos (RAM, ingest, aggregator).
Uso: python3 -m scripts.run_bot
"""
from services.telegram_bot_main import main as bot_main
from scripts.run_worker import start_worker, stop_worker


def main():
    # start worker components (scheduler, RAM, aggregator, ingest)
    start_worker()
    try:
        # start bot (blocking)
        bot_main()
    finally:
        # ensure worker components are stopped on exit
        stop_worker()


if __name__ == "__main__":
    main()
