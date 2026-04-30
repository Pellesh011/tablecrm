from collections import defaultdict

from database.db import SegmentObjectType, database, segment_objects
from sqlalchemy import select


class ContragentRepository:
    @staticmethod
    async def fetch_segments_by_contragent_ids(
        contragent_ids: list[int],
    ) -> dict[int, list[int]]:
        segments_map = defaultdict(list)
        if contragent_ids:
            query = (
                select(segment_objects.c.object_id, segment_objects.c.segment_id)
                .where(
                    segment_objects.c.object_id.in_(contragent_ids),
                    segment_objects.c.object_type == SegmentObjectType.contragents,
                )
                .distinct()
            )
            rows = await database.fetch_all(query)
            for row in rows:
                segments_map[row["object_id"]].append(row["segment_id"])
        return segments_map

    async def fetch_segments_by_contragent_id(self, contragent_id: int) -> list[int]:
        segments_map = await self.fetch_segments_by_contragent_ids([contragent_id])
        return segments_map.get(contragent_id, [])
