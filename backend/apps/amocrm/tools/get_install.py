from typing import Literal, Optional

from apps.amocrm.tools.models.AmoInstallInfoModel import AmoInstallInfoModel
from database.db import (
    amo_install,
    amo_install_table_cashboxes,
    amo_settings,
    amo_settings_load_types,
    database,
)
from sqlalchemy import and_, select


async def get_install_by_cashbox(
    cashbox_id: int, type_install: Literal["leads", "contacts"]
) -> Optional[AmoInstallInfoModel]:
    query = (
        select(amo_install)
        .join(amo_settings, amo_install.c.from_widget == amo_settings.c.id)
        .join(
            amo_settings_load_types,
            amo_settings.c.load_type_id == amo_settings_load_types.c.id,
        )
        .join(
            amo_install_table_cashboxes,
            amo_install_table_cashboxes.c.amo_install_group_id
            == amo_install.c.install_group_id,
        )
        .where(amo_install_table_cashboxes.c.cashbox_id == cashbox_id)
    )
    if type_install == "contacts":
        query = query.where(
            and_(
                amo_install.c.active == True, amo_settings_load_types.c.contacts == True
            )
        )
    elif type_install == "leads":
        query = query.where(
            and_(amo_install.c.active == True, amo_settings_load_types.c.leads == True)
        )
    else:
        raise ValueError("Неверное значение для type_install")

    amo_install_info = await database.fetch_one(query)
    if not amo_install_info:
        raise Exception("Install Not Found")

    return AmoInstallInfoModel(
        id=amo_install_info.id,
        referrer=amo_install_info.referrer,
        access_token=amo_install_info.access_token,
        group_id=amo_install_info.install_group_id,
    )
