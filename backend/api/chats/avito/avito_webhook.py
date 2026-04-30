import hashlib
import hmac
import json
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

AVITO_WEBHOOK_SECRET = ""


def verify_webhook_signature(
    request_body: bytes, signature_header: str, webhook_secret: Optional[str] = None
) -> bool:
    try:
        secret = webhook_secret or AVITO_WEBHOOK_SECRET

        if not secret:
            logger.warning(
                "Webhook secret not configured - skipping signature verification (dev mode)"
            )
            return True

        calculated_signature = hmac.new(
            secret.encode(), request_body, hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(calculated_signature, signature_header)

    except Exception as e:
        logger.error(f"Error verifying webhook signature: {e}")
        return False


def validate_webhook_structure(webhook_data: Dict[str, Any]) -> bool:
    required_fields = ["id", "timestamp", "payload"]

    for field in required_fields:
        if field not in webhook_data:
            logger.error(f"Missing required field in webhook: {field}")
            return False

    return True


async def get_cashbox_id_for_avito_webhook(
    webhook_data: Dict[str, Any],
) -> Optional[int]:
    from database.db import channel_credentials, channels, chats, database
    from sqlalchemy import and_, select

    try:
        payload = webhook_data.get("payload", {})
        payload_value = payload.get("value", {}) if isinstance(payload, dict) else {}

        external_chat_id = payload_value.get("chat_id") or payload_value.get("chatId")
        if external_chat_id is not None:
            external_chat_id = str(external_chat_id).strip() or None

        user_id_raw = payload_value.get("user_id") or payload_value.get("userId")
        user_id = None
        if user_id_raw is not None:
            try:
                user_id = int(user_id_raw)
            except (TypeError, ValueError):
                user_id = user_id_raw

        logger.info(
            f"[Avito Webhook] external_chat_id={external_chat_id}, user_id={user_id}"
        )

        if external_chat_id:
            query = (
                select([chats.c.id, chats.c.cashbox_id, chats.c.channel_id])
                .select_from(chats.join(channels, chats.c.channel_id == channels.c.id))
                .where(
                    and_(
                        channels.c.type == "AVITO",
                        channels.c.is_active.is_(True),
                        chats.c.external_chat_id == str(external_chat_id),
                    )
                )
                .limit(1)
            )
            existing_chat = await database.fetch_one(query)
            if existing_chat:
                logger.info("[Avito Webhook] Found cashbox via existing chat")
                return existing_chat["cashbox_id"]

        if user_id is not None:
            user_id_int = user_id if isinstance(user_id, int) else None
            if user_id_int is None and isinstance(user_id, str):
                try:
                    user_id_int = int(user_id)
                except (TypeError, ValueError):
                    pass

            if user_id_int is not None:
                query = (
                    select([channel_credentials.c.cashbox_id])
                    .select_from(
                        channel_credentials.join(
                            channels, channel_credentials.c.channel_id == channels.c.id
                        )
                    )
                    .where(
                        and_(
                            channels.c.type == "AVITO",
                            channels.c.is_active.is_(True),
                            channel_credentials.c.avito_user_id == user_id_int,
                            channel_credentials.c.is_active.is_(True),
                        )
                    )
                    .limit(1)
                )
                creds = await database.fetch_one(query)
                if creds:
                    logger.info("[Avito Webhook] Found cashbox via avito_user_id")
                    return creds["cashbox_id"]
        logger.warning(
            f"Could not determine cashbox_id from webhook. "
            f"external_chat_id={external_chat_id}, user_id={user_id}"
        )
        return None

    except Exception as e:
        logger.error(f"Error getting cashbox_id for Avito webhook: {e}")
        return None


async def process_avito_webhook(
    request_body: bytes,
    signature_header: Optional[str] = None,
    webhook_secret: Optional[str] = None,
) -> tuple[bool, Dict[str, Any], Optional[int]]:
    try:
        if signature_header:
            if not verify_webhook_signature(
                request_body, signature_header, webhook_secret
            ):
                logger.error("Webhook signature verification failed")
                return False, {}, None

        try:
            webhook_data = json.loads(request_body.decode())
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"Failed to parse webhook JSON: {e}")
            return False, {}, None

        if not validate_webhook_structure(webhook_data):
            logger.error("Invalid webhook structure")
            return False, webhook_data, None

        cashbox_id = await get_cashbox_id_for_avito_webhook(webhook_data)

        if not cashbox_id:
            logger.error(
                "Could not determine cashbox_id - no active Avito credentials found"
            )
            return False, webhook_data, None

        return True, webhook_data, cashbox_id

    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return False, {}, None
