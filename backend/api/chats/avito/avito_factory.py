import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken
from database.db import channel_credentials, database

from .avito_client import AvitoClient

logger = logging.getLogger(__name__)

ENCRYPTION_KEY = os.getenv(
    "AVITO_ENCRYPTION_KEY", "EH_FuKzYnSVwVnDYLW4u8AZ3j8K_1O_Kg4LXlmkJHEg="
)

try:
    cipher = Fernet(ENCRYPTION_KEY.encode())
except Exception as e:
    logger.error(f"Invalid AVITO_ENCRYPTION_KEY: {e}")
    logger.error("Make sure AVITO_ENCRYPTION_KEY is a valid Fernet key")
    cipher = None


def _encrypt_credential(credential: str) -> str:
    if not cipher:
        raise RuntimeError(
            "Encryption cipher not initialized - check AVITO_ENCRYPTION_KEY"
        )

    if not credential:
        return ""

    encrypted = cipher.encrypt(credential.encode())
    return encrypted.decode()


def _decrypt_credential(encrypted: str) -> str:
    if not cipher:
        raise RuntimeError(
            "Encryption cipher not initialized - check AVITO_ENCRYPTION_KEY"
        )

    if not encrypted:
        return ""

    try:
        decrypted = cipher.decrypt(encrypted.encode())
        return decrypted.decode()
    except InvalidToken as e:
        logger.error(f"Failed to decrypt credential: {e}")
        logger.error(
            "This usually means the AVITO_ENCRYPTION_KEY is wrong or has changed"
        )
        raise


async def create_avito_client(
    channel_id: int, cashbox_id: int, on_token_refresh: Optional[Callable] = None
) -> Optional[AvitoClient]:
    try:
        credentials = await database.fetch_one(
            channel_credentials.select().where(
                (channel_credentials.c.channel_id == channel_id)
                & (channel_credentials.c.cashbox_id == cashbox_id)
                & (channel_credentials.c.is_active.is_(True))
            )
        )

        if not credentials:
            logger.warning(
                f"No active credentials found for channel={channel_id}, cashbox={cashbox_id}"
            )
            return None

        api_key = _decrypt_credential(credentials["api_key"])
        api_secret = _decrypt_credential(credentials["api_secret"])
        access_token = (
            _decrypt_credential(credentials["access_token"])
            if credentials.get("access_token")
            else None
        )
        refresh_token = (
            _decrypt_credential(credentials["refresh_token"])
            if credentials.get("refresh_token")
            else None
        )

        token_expires_at = credentials.get("token_expires_at")

        user_id = credentials.get("avito_user_id")

        client = AvitoClient(
            api_key=api_key,
            api_secret=api_secret,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires_at,
            on_token_refresh=on_token_refresh,
            user_id=user_id,
        )

        if not user_id:
            try:
                retrieved_user_id = await client._get_user_id()
                await database.execute(
                    channel_credentials.update()
                    .where(
                        (channel_credentials.c.channel_id == channel_id)
                        & (channel_credentials.c.cashbox_id == cashbox_id)
                    )
                    .values(
                        avito_user_id=retrieved_user_id, updated_at=datetime.utcnow()
                    )
                )
            except Exception as e:
                logger.warning(f"Could not retrieve and save user_id: {e}")

        return client

    except Exception as e:
        logger.error(f"Failed to create AvitoClient: {e}")
        return None


async def save_token_callback(
    channel_id: int, cashbox_id: int, token_data: Dict[str, Any]
):
    try:
        access_token = token_data.get("access_token")
        refresh_token = token_data.get("refresh_token")
        expires_in = token_data.get("expires_in", 3600)

        encrypted_access_token = (
            _encrypt_credential(str(access_token)) if access_token else None
        )
        encrypted_refresh_token = (
            _encrypt_credential(str(refresh_token)) if refresh_token else None
        )

        token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

        await database.execute(
            channel_credentials.update()
            .where(
                (channel_credentials.c.channel_id == channel_id)
                & (channel_credentials.c.cashbox_id == cashbox_id)
            )
            .values(
                access_token=encrypted_access_token,
                refresh_token=encrypted_refresh_token,
                token_expires_at=token_expires_at,
                updated_at=datetime.utcnow(),
            )
        )

    except Exception as e:
        logger.error(f"Failed to save tokens: {e}")


async def validate_avito_credentials(
    api_key: str, api_secret: str, access_token: Optional[str] = None
) -> bool:
    try:
        client = AvitoClient(
            api_key=api_key, api_secret=api_secret, access_token=access_token
        )

        result = await client.get_access_token()

        if result:
            return True
        else:
            logger.warning("Avito credentials validation failed")
            return False

    except Exception as e:
        logger.error(f"Avito credentials validation error: {e}")
        return False
