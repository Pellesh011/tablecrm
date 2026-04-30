import hashlib
import os
import time

from api.installs.schemas import Install, InstallCreate, InstallSettings
from fastapi import APIRouter, HTTPException
from functions.installs import (
    add_md5_to_install,
    create_install,
    delink_install,
    get_install,
    install_bundle_user,
    set_settings_install,
)

router = APIRouter(prefix="/install", tags=["install"])
ios_key = os.getenv("IOS_MD5")


@router.get("/", response_model=Install)
async def get_install_route(md5key: str = None, devicetoken: str = None):
    install = await get_install(md5key=md5key, devicetoken=devicetoken)
    if not install:
        raise HTTPException(detail="Устройство не найдено", status_code=400)
    return {"id": install.id, "md5key": install.md5key}


@router.post("/new/ios/{key}/", response_model=Install)
async def create_install_route(key: str, install: InstallCreate):
    if key != ios_key:
        raise HTTPException(detail="Неверный ключ", status_code=400)

    install_id = await create_install(install=install.dict())

    if not install_id[0]:
        raise HTTPException(
            detail="Устройство с таким devicetoken - уже зарегистрировано",
            status_code=400,
        )

    md5 = hashlib.md5(f"{time.time()}{install_id[1]}".encode("utf-8")).hexdigest()
    await add_md5_to_install(install_id=install_id[1], md5key=md5)

    return {"id": install_id[1], "md5key": md5}


@router.post("/ios/link/")
async def install_bundle_user_route(tgtoken: str, md5key: str):
    status = await install_bundle_user(tg_token=tgtoken, md5key=md5key)
    if not status:
        raise HTTPException(
            detail="Устройство/касса не найдены или уже связаны", status_code=400
        )
    return {"success": "link created"}


@router.post("/ios/delink/")
async def delink_install_bundle_user_route(tgtoken: str, md5key: str):
    status = await delink_install(tg_token=tgtoken, md5key=md5key)
    if not status:
        raise HTTPException(detail="Устройство не найдено", status_code=400)
    return {"success": "delink bundle"}


@router.patch("/settings/{md5key}/")
async def set_settings_install_route(md5key: str, settings: InstallSettings):
    await set_settings_install(md5key=md5key, settings=settings.dict())
    return {"success": "settings updated"}
