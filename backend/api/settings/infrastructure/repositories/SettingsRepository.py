from typing import Optional

from database.db import database, docs_sales_settings


class SettingsRepository:
    @staticmethod
    async def fetch_settings_by_ids(settings_ids: list[int]) -> dict[int, dict]:
        if not settings_ids:
            return {}

        query = docs_sales_settings.select().where(
            docs_sales_settings.c.id.in_(settings_ids)
        )
        rows = await database.fetch_all(query)

        settings_map: dict[int, dict] = {}
        for row in rows:
            settings_map[row["id"]] = dict(row)

        return settings_map

    async def fetch_settings_by_id(self, settings_id: int) -> Optional[dict]:
        settings_map = await self.fetch_settings_by_ids([settings_id])
        return settings_map.get(settings_id)
