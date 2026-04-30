"""
api/chats/max/max_routes.py

FastAPI маршруты для подключения и управления Max-ботом.
Структура аналогична telegram_routes.py.
"""

import logging
from typing import Optional

import aiohttp
from api.chats import crud
from api.chats.auth import get_current_user_for_avito as get_current_user
from api.chats.avito.avito_factory import _decrypt_credential, _encrypt_credential
from api.chats.max.max_client import (
    MaxAPIError,
    MaxClient,
    delete_webhook,
    get_me,
    set_webhook,
)
from api.chats.max.max_constants import MAX_SVG_ICON
from api.chats.max.max_handler import handle_update
from common.utils.url_helper import get_app_url_for_environment
from database.db import channel_credentials, channels, database
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chats/max", tags=["chats-max"])


class MaxConnectRequest(BaseModel):
    bot_token: str
    channel_name: Optional[str] = None


def _get_app_url() -> str:
    url = (get_app_url_for_environment() or "").rstrip("/")
    if url and not url.startswith("http"):
        url = f"https://{url}"
    return url


@router.get("/")
async def get_max_api_info():
    """Информация об интеграции Max."""
    return {
        "service": "Max Messenger Bot API Integration",
        "version": "1.0",
        "base_url": "/chats/max",
        "endpoints": {
            "connect": {
                "method": "POST",
                "path": "/chats/max/connect",
                "description": "Подключение Max-бота",
                "auth_required": True,
            },
            "status": {
                "method": "GET",
                "path": "/chats/max/status",
                "description": "Статус подключения",
                "auth_required": True,
            },
            "webhook": {
                "method": "POST",
                "path": "/chats/max/webhook/{channel_id}",
                "description": "Webhook для входящих обновлений Max",
                "auth_required": False,
            },
            "disconnect": {
                "method": "DELETE",
                "path": "/chats/max/disconnect",
                "description": "Отключить бота",
                "auth_required": True,
            },
        },
    }


@router.get("/video/{token}")
async def get_max_video(token: str, request: Request):
    query_token = request.query_params.get("token")
    if not query_token:
        raise HTTPException(status_code=401, detail="Token required")

    from api.chats.auth import get_current_user

    user = await get_current_user(query_token)
    cashbox_id = user.cashbox_id

    channel = await crud.get_channel_by_cashbox(cashbox_id, "MAX")
    if not channel:
        raise HTTPException(status_code=404, detail="MAX channel not found")

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel["id"])
            & (channel_credentials.c.cashbox_id == cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not creds:
        raise HTTPException(status_code=404, detail="Credentials not found")

    bot_token = _decrypt_credential(creds["api_key"])
    client = MaxClient(bot_token)

    video_info = await client.get_video_info(token)
    urls = video_info.get("urls", {})

    video_url = None
    if isinstance(urls, dict):
        for quality in ("mp4_720", "mp4_480", "mp4_360", "mp4_240", "mp4_144"):
            video_url = urls.get(quality)
            if video_url:
                break
        if not video_url:
            video_url = urls.get("hls")
    elif isinstance(urls, list):
        for item in urls:
            if isinstance(item, dict) and item.get("url"):
                video_url = item["url"]
                break

    if not video_url:
        raise HTTPException(status_code=404, detail="Video URL not found")

    headers = {
        "User-Agent": "PostmanRuntime/7.51.1",
        "Cookie": "tstc=p",
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(video_url, headers=headers) as resp:
            if resp.status != 200:
                raise HTTPException(
                    status_code=resp.status, detail="Video fetch failed"
                )
            content = await resp.read()
            return Response(
                content=content,
                media_type="video/mp4",
                headers={
                    "Accept-Ranges": "none",
                    "Access-Control-Allow-Origin": "*",
                },
            )


@router.post("/connect")
async def connect_max_channel(
    payload: MaxConnectRequest,
    user=Depends(get_current_user),
):
    """
    Подключить Max-бота.
    """
    cashbox_id = user.cashbox_id

    try:
        bot_info = await get_me(payload.bot_token)
    except MaxAPIError as exc:
        raise HTTPException(status_code=401, detail=f"Невалидный токен бота Max: {exc}")

    bot_name: str = bot_info.get("name") or bot_info.get("username") or "Max Bot"
    bot_user_id: Optional[int] = bot_info.get("user_id")
    channel_name: str = payload.channel_name or f"Max — {bot_name}"

    existing_channel = await crud.get_channel_by_cashbox(cashbox_id, "MAX")

    if not existing_channel:
        channel_id = await database.execute(
            channels.insert().values(
                name=channel_name,
                type="MAX",
                cashbox_id=cashbox_id,
                svg_icon=MAX_SVG_ICON,
                description=f"Max Bot Integration: {bot_name}",
                is_active=True,
            )
        )
    else:
        channel_id = existing_channel["id"]

        if payload.channel_name:
            await database.execute(
                channels.update()
                .where(channels.c.id == channel_id)
                .values(name=channel_name)
            )

    encrypted_token = _encrypt_credential(payload.bot_token)

    existing_creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.cashbox_id == cashbox_id)
        )
    )

    # Убрали "bot_name" из cred_values
    cred_values = {
        "api_key": encrypted_token,
        "api_secret": "",
        "is_active": True,
        "avito_user_id": bot_user_id,
    }

    if existing_creds:
        await database.execute(
            channel_credentials.update()
            .where(
                (channel_credentials.c.channel_id == channel_id)
                & (channel_credentials.c.cashbox_id == cashbox_id)
            )
            .values(**cred_values)
        )
    else:
        await database.execute(
            channel_credentials.insert().values(
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                **cred_values,
            )
        )

    app_url = _get_app_url()
    webhook_registered = False
    webhook_url = None
    webhook_error = None

    if app_url:
        webhook_url = f"{app_url}/api/v1/chats/max/webhook/{channel_id}"
        try:
            await set_webhook(payload.bot_token, webhook_url)
            webhook_registered = True
            logger.info("[Max] Webhook registered: %s", webhook_url)
        except MaxAPIError as exc:
            webhook_error = str(exc)
            logger.warning("[Max] Webhook registration failed: %s", exc)
    else:
        webhook_error = "APP_URL не настроен; webhook не зарегистрирован"

    return {
        "success": True,
        "channel_id": channel_id,
        "cashbox_id": cashbox_id,
        "bot_name": bot_name,
        "bot_user_id": bot_user_id,
        "webhook_registered": webhook_registered,
        "webhook_url": webhook_url if webhook_registered else None,
        "webhook_error": webhook_error,
    }


@router.get("/status")
async def get_max_status(user=Depends(get_current_user)):
    """Проверить статус подключения Max-бота."""
    channel = await crud.get_channel_by_cashbox(user.cashbox_id, "MAX")
    if not channel:
        raise HTTPException(status_code=404, detail="Max-канал не подключён")

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel["id"])
            & (channel_credentials.c.cashbox_id == user.cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not creds:
        raise HTTPException(status_code=404, detail="Учётные данные Max не найдены")

    bot_token = _decrypt_credential(creds["api_key"])

    try:
        bot_info = await get_me(bot_token)
    except MaxAPIError as exc:
        raise HTTPException(status_code=502, detail=f"Max API недоступен: {exc}")

    subscriptions: dict = {}
    try:
        client = MaxClient(bot_token)
        subscriptions = await client.get_subscriptions()
    except Exception:
        pass

    return {
        "success": True,
        "channel_id": channel["id"],
        "bot": bot_info,
        "subscriptions": subscriptions,
    }


@router.delete("/disconnect")
async def disconnect_max_channel(user=Depends(get_current_user)):
    """
    Отключить Max-бота:
    — удаляет webhook
    — деактивирует credentials и канал
    """
    channel = await crud.get_channel_by_cashbox(user.cashbox_id, "MAX")
    if not channel:
        raise HTTPException(status_code=404, detail="Max-канал не найден")

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel["id"])
            & (channel_credentials.c.cashbox_id == user.cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )

    if creds:
        try:
            bot_token = _decrypt_credential(creds["api_key"])
            await delete_webhook(bot_token)
        except Exception as exc:
            logger.warning("[Max] delete_webhook on disconnect: %s", exc)

        await database.execute(
            channel_credentials.update()
            .where(channel_credentials.c.id == creds["id"])
            .values(is_active=False)
        )

    await database.execute(
        channels.update().where(channels.c.id == channel["id"]).values(is_active=False)
    )

    return {"success": True, "message": "Max-канал отключён"}


@router.post("/webhook/{channel_id}", include_in_schema=False)
async def max_webhook(channel_id: int, request: Request):
    """
    Webhook для получения обновлений от Max Bot API.
    Max отправляет POST с JSON-телом вида:
        {"update_type": "message_created", "timestamp": ..., "message": {...}}
    Возвращает {"ok": true} при успешной обработке (ожидание платформы).
    """

    channel = await database.fetch_one(
        channels.select().where(channels.c.id == channel_id)
    )
    if not channel or channel.get("type") != "MAX" or not channel.get("is_active"):
        raise HTTPException(status_code=404, detail="Max-канал не найден")

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not creds:
        raise HTTPException(status_code=404, detail="Учётные данные канала не найдены")

    bot_token = _decrypt_credential(creds["api_key"])
    cashbox_id: int = creds["cashbox_id"]

    try:
        update = await request.json()
    except Exception:
        update = {}

    if not update:
        return {"ok": True}

    await handle_update(
        update=update,
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        bot_token=bot_token,
    )

    return {"ok": True}


@router.post("/webhook/register")
async def register_max_webhook(
    webhook_url: Optional[str] = None,
    user=Depends(get_current_user),
):
    """Принудительно (пере-)зарегистрировать webhook."""
    channel = await crud.get_channel_by_cashbox(user.cashbox_id, "MAX")
    if not channel:
        raise HTTPException(status_code=404, detail="Max-канал не подключён")

    creds = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel["id"])
            & (channel_credentials.c.cashbox_id == user.cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not creds:
        raise HTTPException(status_code=404, detail="Учётные данные не найдены")

    bot_token = _decrypt_credential(creds["api_key"])

    if not webhook_url:
        app_url = _get_app_url()
        if not app_url:
            raise HTTPException(status_code=400, detail="APP_URL не настроен")
        webhook_url = f"{app_url}/api/v1/chats/max/webhook/{channel['id']}"

    try:
        result = await set_webhook(bot_token, webhook_url)
        return {
            "success": True,
            "message": "Webhook зарегистрирован",
            "webhook_url": webhook_url,
            "result": result,
        }
    except MaxAPIError as exc:
        raise HTTPException(
            status_code=400, detail=f"Ошибка регистрации webhook: {exc}"
        )
