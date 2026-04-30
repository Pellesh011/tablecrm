import logging
from datetime import datetime

from api.chats.avito.avito_factory import create_avito_client, save_token_callback
from common.decorators import ensure_db_connection
from database.db import channel_credentials, channels, database
from sqlalchemy import and_, select

logger = logging.getLogger(__name__)


@ensure_db_connection
async def check_avito_accounts_status():
    """
    Проверяет статусы всех активных Avito аккаунтов.
    """

    # Получаем все активные credentials для Avito каналов
    query = (
        select(
            [
                channel_credentials.c.id,
                channel_credentials.c.channel_id,
                channel_credentials.c.cashbox_id,
                channel_credentials.c.avito_user_id,
            ]
        )
        .select_from(
            channel_credentials.join(
                channels, channel_credentials.c.channel_id == channels.c.id
            )
        )
        .where(
            and_(
                channels.c.type == "AVITO",
                channels.c.is_active.is_(True),
                channel_credentials.c.is_active.is_(True),
            )
        )
    )

    all_credentials = await database.fetch_all(query)

    if not all_credentials:
        logger.info("No active Avito credentials found for status check")
        return

    logger.info(f"Checking status for {len(all_credentials)} Avito accounts")

    checked_count = 0
    success_count = 0
    error_count = 0

    for cred in all_credentials:
        try:
            channel_id = cred["channel_id"]
            cashbox_id = cred["cashbox_id"]
            cred_id = cred["id"]

            # Создаем клиент
            client = await create_avito_client(
                channel_id=channel_id,
                cashbox_id=cashbox_id,
                on_token_refresh=lambda token_data, ch_id=channel_id, cb_id=cashbox_id: save_token_callback(
                    ch_id, cb_id, token_data
                ),
            )

            if not client:
                logger.warning(
                    f"Could not create Avito client for channel {channel_id}, cashbox {cashbox_id}"
                )
                # Обновляем статус как ошибку
                await database.execute(
                    channel_credentials.update()
                    .where(channel_credentials.c.id == cred_id)
                    .values(
                        last_status_code=0,
                        last_status_check_at=datetime.utcnow(),
                        connection_status="error",
                        updated_at=datetime.utcnow(),
                    )
                )
                error_count += 1
                continue

            # Проверяем статус
            status_result = await client.check_status()

            # Обновляем статус в БД
            await database.execute(
                channel_credentials.update()
                .where(channel_credentials.c.id == cred_id)
                .values(
                    last_status_code=status_result.get("status_code"),
                    last_status_check_at=datetime.utcnow(),
                    connection_status=status_result.get("connection_status"),
                    updated_at=datetime.utcnow(),
                )
            )

            checked_count += 1

            if status_result.get("success"):
                success_count += 1
                logger.debug(
                    f"Status check successful for channel {channel_id}, cashbox {cashbox_id}: {status_result.get('status_code')}"
                )
            else:
                error_count += 1
                logger.warning(
                    f"Status check failed for channel {channel_id}, cashbox {cashbox_id}: "
                    f"status_code={status_result.get('status_code')}, "
                    f"connection_status={status_result.get('connection_status')}"
                )

        except Exception as e:
            error_count += 1
            logger.error(
                f"Error checking status for credential {cred.get('id')}: {e}",
                exc_info=True,
            )

            # Обновляем статус как ошибку
            try:
                await database.execute(
                    channel_credentials.update()
                    .where(channel_credentials.c.id == cred["id"])
                    .values(
                        last_status_code=0,
                        last_status_check_at=datetime.utcnow(),
                        connection_status="error",
                        updated_at=datetime.utcnow(),
                    )
                )
            except Exception as update_error:
                logger.error(f"Failed to update status in DB: {update_error}")

    logger.info(
        f"Avito status check completed: checked={checked_count}, "
        f"success={success_count}, errors={error_count}"
    )
