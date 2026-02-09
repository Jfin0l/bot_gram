import logging
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from core import fetcher, snapshot

log = logging.getLogger(__name__)


def start_scheduler(config, fetch_interval: int = 300, snapshot_interval: int = 300):
    """Inicia un scheduler en background que:
    - ejecuta `fetcher.fetch_and_store` cada `fetch_interval` segundos
    - ejecuta `snapshot.create_and_store_snapshots` cada `snapshot_interval` segundos
    Devuelve la instancia `BackgroundScheduler` para control del proceso.
    """
    sched = BackgroundScheduler()

    def job_fetch():
        log.info(f"[{datetime.utcnow().isoformat()}] Scheduler: Ejecutando fetch_and_store")
        try:
            fetcher.fetch_and_store()
        except Exception as e:
            log.exception(f"Error en job_fetch: {e}")

    def job_snapshot():
        log.info(f"[{datetime.utcnow().isoformat()}] Scheduler: Ejecutando create_and_store_snapshots")
        try:
            snapshot.create_and_store_snapshots(config)
        except Exception as e:
            log.exception(f"Error en job_snapshot: {e}")

    sched.add_job(job_fetch, "interval", seconds=fetch_interval, id="fetch_job")
    sched.add_job(job_snapshot, "interval", seconds=snapshot_interval, id="snapshot_job")

    sched.start()
    log.info(f"Scheduler iniciado (fetch every {fetch_interval}s, snapshot every {snapshot_interval}s)")
    return sched
