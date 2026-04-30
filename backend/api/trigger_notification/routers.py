from fastapi import APIRouter
from jobs.triggers_job.job import run

router = APIRouter(tags=["trigger_notification"], prefix="/trigger_notifications")


@router.get("/test")
async def test():
    print(await run())
