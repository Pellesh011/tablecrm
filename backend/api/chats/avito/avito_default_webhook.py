import logging
from typing import Optional

from api.chats.avito.avito_handler import AvitoHandler
from api.chats.avito.avito_webhook import process_avito_webhook
from api.chats.avito.schemas import AvitoWebhookResponse
from database.db import channel_credentials, channels, database
from fastapi import APIRouter, Query, Request
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/avito", tags=["avito-webhook"])


@router.post("/hook", response_model=AvitoWebhookResponse)
async def receive_avito_webhook_default(
    request: Request,
    cashbox_id: Optional[int] = Query(
        None, description="Cashbox ID (optional, can be passed in URL)"
    ),
):
    try:
        body = await request.body()
        signature_header = request.headers.get("X-Avito-Signature")

        # Если cashbox_id передан в URL, используем его, иначе определяем автоматически
        if cashbox_id is not None:
            is_valid, webhook_data, _ = await process_avito_webhook(
                body, signature_header
            )
            if not is_valid:
                logger.warning("Invalid webhook received")
                return {
                    "success": False,
                    "message": "Invalid webhook signature or structure",
                }
            # cashbox_id уже есть из URL
        else:
            is_valid, webhook_data, cashbox_id = await process_avito_webhook(
                body, signature_header
            )
            if not is_valid or not cashbox_id:
                logger.warning("Could not determine cashbox_id")
                return {"success": False, "message": "Could not determine cashbox_id"}

        try:
            from api.chats.avito.avito_types import AvitoWebhook

            raw_value = (webhook_data.get("payload") or {}).get("value") or {}
            logger.debug(
                f"[Avito DEBUG] raw webhook value keys={list(raw_value.keys())}, "
                f"user_id={raw_value.get('user_id')}, userId={raw_value.get('userId')}, "
                f"author_id={raw_value.get('author_id')}, authorId={raw_value.get('authorId')}"
            )
            webhook = AvitoWebhook(**webhook_data)
        except Exception as e:
            return {"success": False, "message": f"Invalid webhook structure: {str(e)}"}

        payload = webhook_data.get("payload") or {}
        event_type = payload.get("type", "")

        user_id = None
        if hasattr(webhook.payload, "value") and webhook.payload.value:
            user_id = getattr(webhook.payload.value, "user_id", None)

        # Если есть user_id, пытаемся найти канал по нему (для обратной совместимости)
        if user_id:
            channels_query = (
                select([channel_credentials.c.channel_id])
                .select_from(
                    channel_credentials.join(
                        channels, channel_credentials.c.channel_id == channels.c.id
                    )
                )
                .where(
                    and_(
                        channels.c.type == "AVITO",
                        channels.c.is_active.is_(True),
                        channel_credentials.c.avito_user_id == user_id,
                        channel_credentials.c.cashbox_id == cashbox_id,
                        channel_credentials.c.is_active.is_(True),
                    )
                )
            )
            channels_result = await database.fetch_all(channels_query)
            raw_value = (webhook_data.get("payload") or {}).get("value")

            if channels_result and len(channels_result) > 1:
                results = []
                for channel_row in channels_result:
                    channel_id = channel_row["channel_id"]
                    result = await AvitoHandler.handle_webhook_event(
                        webhook, cashbox_id, channel_id, raw_payload_value=raw_value
                    )
                    results.append(result)
                success_result = next(
                    (r for r in results if r.get("success")),
                    results[-1] if results else None,
                )
                result = (
                    success_result
                    if success_result
                    else {
                        "success": False,
                        "message": "Failed to process webhook for any channel",
                    }
                )
            elif channels_result:
                channel_id = channels_result[0]["channel_id"]
                result = await AvitoHandler.handle_webhook_event(
                    webhook, cashbox_id, channel_id, raw_payload_value=raw_value
                )
            else:
                result = await AvitoHandler.handle_webhook_event(
                    webhook, cashbox_id, raw_payload_value=raw_value
                )
        else:
            raw_value = (webhook_data.get("payload") or {}).get("value")
            result = await AvitoHandler.handle_webhook_event(
                webhook, cashbox_id, raw_payload_value=raw_value
            )

        return {
            "success": result.get("success", False),
            "message": result.get("message", "Event processed"),
            "chat_id": result.get("chat_id"),
            "message_id": result.get("message_id"),
        }

    except Exception as e:
        logger.error(f"Error processing Avito webhook: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}


@router.post("/hook/{cashbox_id}", response_model=AvitoWebhookResponse)
async def receive_avito_webhook_with_cashbox(
    request: Request,
    cashbox_id: int,
):
    try:
        body = await request.body()
        signature_header = request.headers.get("X-Avito-Signature")

        is_valid, webhook_data, _ = await process_avito_webhook(body, signature_header)
        if not is_valid:
            return {
                "success": False,
                "message": "Invalid webhook signature or structure",
            }

        # Преобразуем в объект AvitoWebhook
        from api.chats.avito.avito_types import AvitoWebhook

        webhook = AvitoWebhook(**webhook_data)

        raw_value = (webhook_data.get("payload") or {}).get("value") or {}
        result = await AvitoHandler.handle_webhook_event(
            webhook, cashbox_id, raw_payload_value=raw_value
        )

        return {
            "success": result.get("success", False),
            "message": result.get("message", "Event processed"),
            "chat_id": result.get("chat_id"),
            "message_id": result.get("message_id"),
        }
    except Exception as e:
        logger.error(f"Error processing Avito webhook: {e}")
        return {"success": False, "message": f"Error: {str(e)}"}
