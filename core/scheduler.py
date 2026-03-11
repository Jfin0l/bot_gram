import logging
from datetime import datetime
from core import fetcher, snapshot

log = logging.getLogger(__name__)


def start_scheduler(config, fetch_interval: int = 300, snapshot_interval: int = 300):
    """Start a background scheduler for periodic fetch and snapshot.
    If `apscheduler` is not available, return a dummy scheduler with `shutdown()`.
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except Exception:
        log.warning("apscheduler not available; scheduler disabled (fallback).")

        class DummySched:
            def shutdown(self):
                pass

        return DummySched()

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

    def job_merchant_stats():
        log.info("Scheduler: Actualizando merchant_stats...")
        try:
            from core.merchant_stats import store_hourly_merchant_stats
            for pair in config.get('pares', []):
                store_hourly_merchant_stats(pair)
        except Exception as e:
            log.exception(f"Error en job_merchant_stats: {e}")

    def job_cleanup_db():
        log.info("Scheduler: Ejecutando limpieza de DB (retención 30 días)...")
        try:
            from core.db import cleanup_old_data
            cleanup_old_data(days=30)
        except Exception as e:
            log.exception(f"Error en job_cleanup_db: {e}")

    sched.add_job(job_fetch, "interval", seconds=fetch_interval, id="fetch_job")
    sched.add_job(job_snapshot, "interval", seconds=snapshot_interval, id="snapshot_job")
    sched.add_job(job_merchant_stats, "interval", hours=1, id="merchant_stats_job")
    sched.add_job(job_cleanup_db, "cron", hour=3, id="cleanup_job") # A las 3 AM

    sched.start()
    log.info(f"Scheduler iniciado (fetch every {fetch_interval}s, snapshot every {snapshot_interval}s)")
    return sched
