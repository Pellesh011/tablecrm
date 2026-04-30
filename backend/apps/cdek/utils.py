from datetime import datetime
from typing import Optional

from apps.cdek.client import CdekClient
from apps.cdek.models import cdek_credentials
from database.db import (
    database,
    integrations,
    integrations_to_cashbox,
    users_cboxes_relation,
)
from sqlalchemy import func, select


async def create_cdek_client(integration_cashboxes_id: int) -> Optional[CdekClient]:
    creds = await database.fetch_one(
        cdek_credentials.select().where(
            cdek_credentials.c.integration_cashboxes == integration_cashboxes_id
        )
    )
    if not creds:
        return None

    async def save_token(access_token: str, expires_at: datetime):
        await database.execute(
            cdek_credentials.update()
            .where(cdek_credentials.c.integration_cashboxes == integration_cashboxes_id)
            .values(
                access_token=access_token,
                token_expires_at=expires_at,
                updated_at=func.now(),
            )
        )

    token_expires = creds["token_expires_at"]
    if token_expires and token_expires.tzinfo:
        token_expires = token_expires.replace(tzinfo=None)

    return CdekClient(
        account=creds["account"],
        secure_password=creds["secure_password"],
        access_token=creds["access_token"],
        token_expires_at=token_expires,
        token_saver=save_token,
    )


async def get_or_create_cdek_integration() -> int:
    query = select(integrations.c.id).where(integrations.c.name == "СДЭК")
    existing_id = await database.fetch_val(query)
    if existing_id:
        return existing_id

    insert_values = {
        "name": "СДЭК",
        "status": True,
        "description_short": "Интеграция со службой доставки СДЭК",
        "is_public": True,
        "cost": 0,
    }
    new_id = await database.execute(integrations.insert().values(**insert_values))
    return new_id


async def integration_info(cashbox_id: int, id_integration: int) -> dict:
    query = (
        select(
            integrations_to_cashbox.c.installed_by,
            users_cboxes_relation.c.token,
            integrations_to_cashbox.c.id,
            integrations_to_cashbox.c.status,
        )
        .where(users_cboxes_relation.c.cashbox_id == cashbox_id)
        .select_from(users_cboxes_relation)
        .join(
            integrations_to_cashbox,
            users_cboxes_relation.c.id == integrations_to_cashbox.c.installed_by,
        )
        .where(integrations_to_cashbox.c.integration_id == id_integration)
    )
    return await database.fetch_one(query)


async def get_cdek_credentials(integration_cashboxes_id: int) -> dict:
    query = cdek_credentials.select().where(
        cdek_credentials.c.integration_cashboxes == integration_cashboxes_id
    )
    return await database.fetch_one(query)


async def save_cdek_credentials(
    integration_cashboxes: int, account: str, secure_password: str
):
    existing = await get_cdek_credentials(integration_cashboxes)
    if existing:
        await database.execute(
            cdek_credentials.update()
            .where(cdek_credentials.c.integration_cashboxes == integration_cashboxes)
            .values(
                account=account, secure_password=secure_password, updated_at=func.now()
            )
        )
    else:
        await database.execute(
            cdek_credentials.insert().values(
                integration_cashboxes=integration_cashboxes,
                account=account,
                secure_password=secure_password,
            )
        )
