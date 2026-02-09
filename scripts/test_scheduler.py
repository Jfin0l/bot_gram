#!/usr/bin/env python3
"""Test runner: inicia el scheduler con intervalos muy cortos y lo detiene tras unos segundos."""
from core.scheduler import start_scheduler
from core import db

# config mínimo para snapshot
CONFIG = {
    "monedas": {"COP": {"rows": 20, "page": 2}, "VES": {"rows": 20, "page": 4}},
    "filas_tasa_remesa": 5,
    "ponderacion_volumen": True,
    "limite_outlier": 0.025,
}


def main():
    sched = start_scheduler(CONFIG, fetch_interval=3, snapshot_interval=4)
    try:
        import time
        time.sleep(10)
    finally:
        sched.shutdown()
        print('Scheduler stopped')


if __name__ == '__main__':
    main()
