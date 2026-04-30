import secrets
from typing import Optional

from api.chats import crud
from api.chats.auth import get_current_user_for_avito
from api.chats.avito.avito_factory import _decrypt_credential, _encrypt_credential
from api.chats.telegram.telegram_client import get_me, get_webhook_info, set_webhook
from api.chats.telegram.telegram_constants import TELEGRAM_SVG_ICON
from api.chats.telegram.telegram_handler import handle_update
from common.utils.url_helper import get_app_url_for_environment
from database.db import channel_credentials, channels, database
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/chats/telegram", tags=["chats-telegram"])


class TelegramConnectRequest(BaseModel):
    bot_token: str
    channel_name: Optional[str] = None


@router.get("/")
async def get_telegram_api_info():
    return {
        "service": "Telegram Bot API Integration",
        "version": "1.0",
        "base_url": "/chats/telegram",
        "endpoints": {
            "connect": {
                "method": "POST",
                "path": "/chats/telegram/connect",
                "description": "Подключение Telegram бота",
                "auth_required": True,
            },
            "status": {
                "method": "GET",
                "path": "/chats/telegram/status",
                "description": "Проверка статуса подключения и webhook",
                "auth_required": True,
            },
            "webhook": {
                "method": "POST",
                "path": "/chats/telegram/webhook/{channel_id}",
                "description": "Webhook для входящих обновлений Telegram",
                "auth_required": False,
            },
        },
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json",
        },
        "authentication": {
            "methods": [
                "Query parameter: ?token=YOUR_TOKEN",
                "Header: Authorization: Bearer YOUR_TOKEN",
            ]
        },
    }


@router.post("/connect")
async def connect_telegram_channel(
    payload: TelegramConnectRequest, user=Depends(get_current_user_for_avito)
):
    cashbox_id = user.cashbox_id

    try:
        bot_info = await get_me(payload.bot_token)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Bot token invalid: {e}")

    bot_username = bot_info.get("username") or "bot"
    channel_name = payload.channel_name or f"Telegram - {bot_username}"

    existing_channel = await database.fetch_one(
        channels.select().where(channels.c.name == channel_name)
    )
    if existing_channel:
        channel_name = f"{channel_name} ({cashbox_id})"
        existing_channel = await database.fetch_one(
            channels.select().where(channels.c.name == channel_name)
        )

    if not existing_channel:
        channel_id = await database.execute(
            channels.insert().values(
                name=channel_name,
                type="TELEGRAM",
                cashbox_id=cashbox_id,
                svg_icon=TELEGRAM_SVG_ICON,
                description=f"Telegram Bot Integration for {bot_username}",
                is_active=True,
            )
        )
    else:
        channel_id = existing_channel["id"]

    encrypted_bot_token = _encrypt_credential(payload.bot_token)
    webhook_secret = secrets.token_urlsafe(32)

    existing_credentials = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.cashbox_id == cashbox_id)
        )
    )

    if existing_credentials:
        await database.execute(
            channel_credentials.update()
            .where(
                (channel_credentials.c.channel_id == channel_id)
                & (channel_credentials.c.cashbox_id == cashbox_id)
            )
            .values(
                api_key=encrypted_bot_token,
                api_secret=webhook_secret,
                is_active=True,
            )
        )
    else:
        await database.execute(
            channel_credentials.insert().values(
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                api_key=encrypted_bot_token,
                api_secret=webhook_secret,
                is_active=True,
            )
        )

    app_url = get_app_url_for_environment()
    if not app_url:
        raise HTTPException(
            status_code=500,
            detail="APP_URL not configured. Set APP_URL to public backend URL.",
        )

    if not app_url.startswith("http"):
        app_url = f"https://{app_url}"

    webhook_url = f"{app_url.rstrip('/')}/api/v1/chats/telegram/webhook/{channel_id}"

    try:
        await set_webhook(payload.bot_token, webhook_url, webhook_secret)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to set webhook: {e}")

    return {
        "success": True,
        "channel_id": channel_id,
        "bot_username": bot_username,
        "webhook_url": webhook_url,
    }


@router.get("/status")
async def get_telegram_status(user=Depends(get_current_user_for_avito)):
    channel = await crud.get_channel_by_cashbox(user.cashbox_id, "TELEGRAM")
    if not channel:
        raise HTTPException(status_code=404, detail="Telegram channel not found")

    credentials = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel["id"])
            & (channel_credentials.c.cashbox_id == user.cashbox_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )
    if not credentials:
        raise HTTPException(status_code=404, detail="Telegram credentials not found")

    bot_token = _decrypt_credential(credentials["api_key"])
    bot_info = await get_me(bot_token)
    webhook_info = await get_webhook_info(bot_token)

    return {
        "success": True,
        "channel_id": channel["id"],
        "bot": bot_info,
        "webhook": webhook_info,
    }


@router.post("/webhook/{channel_id}", include_in_schema=False)
async def telegram_webhook(channel_id: int, request: Request):
    secret_header = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    channel = await database.fetch_one(
        channels.select().where(channels.c.id == channel_id)
    )
    if not channel or channel.get("type") != "TELEGRAM" or not channel.get("is_active"):
        raise HTTPException(status_code=404, detail="Telegram channel not found")

    credentials = await database.fetch_one(
        channel_credentials.select().where(
            (channel_credentials.c.channel_id == channel_id)
            & (channel_credentials.c.is_active.is_(True))
        )
    )

    if not credentials:
        raise HTTPException(status_code=404, detail="Channel credentials not found")

    if credentials.get("api_secret") and secret_header != credentials.get("api_secret"):
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    bot_token = _decrypt_credential(credentials["api_key"])
    cashbox_id = credentials["cashbox_id"]

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    result = await handle_update(
        update=payload,
        channel_id=channel_id,
        cashbox_id=cashbox_id,
        bot_token=bot_token,
    )
    return result
