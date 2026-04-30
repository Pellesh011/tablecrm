import asyncio
import logging
import sys

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    JobExecutionEvent,
)
from database.db import database  # Импорт базы данных
from jobs.jobs import scheduler  # Импорт настроенного планировщика

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _on_job_event(event: JobExecutionEvent):
    if event.exception:
        logger.error("APScheduler job failed: job_id=%s", event.job_id, exc_info=True)
        return

    if event.code == EVENT_JOB_MISSED:
        logger.warning("APScheduler job missed: job_id=%s", event.job_id)
        return

    logger.info("APScheduler job executed: job_id=%s", event.job_id)


def _log_registered_jobs():
    jobs = scheduler.get_jobs()
    if not jobs:
        logger.warning("APScheduler started but no jobs are registered")
        return

    logger.info("APScheduler registered jobs: %s", [job.id for job in jobs])
    for job in jobs:
        logger.info(
            "APScheduler job: id=%s, next_run_time=%s, trigger=%s",
            job.id,
            getattr(job, "next_run_time", None),
            getattr(job, "trigger", None),
        )


async def main():
    logger.info("Starting Job Scheduler...")

    # 1. Глобальное подключение к БД при старте
    if not database.is_connected:
        await database.connect()
        logger.info("Database connected globally.")

    logging.getLogger("apscheduler").setLevel(logging.INFO)
    scheduler.add_listener(
        _on_job_event,
        EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
    )
    scheduler.start()
    _log_registered_jobs()

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping scheduler...")
        scheduler.shutdown()
    finally:
        if database.is_connected:
            await database.disconnect()
            logger.info("Database disconnected.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
