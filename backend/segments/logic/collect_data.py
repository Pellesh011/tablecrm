from database.db import SegmentObjectType, contragents, database, segment_objects
from segments.constants import SegmentChangeType
from segments.helpers.collect_obj_ids import collect_objects
from sqlalchemy.sql import select


class ContragentsData:
    def __init__(self, segment_obj):
        self.segment_obj = segment_obj

    async def collect(self):
        contragents = await self.get_contragents_data()
        added_contragents = await self.added_contragents_data()
        deleted_contragents = await self.deleted_contragents_data()
        exited_contragents = await self.exited_contragents_data()
        entered_contragents = await self.entered_contragents_data()

        return {
            "contragents": contragents,
            "added_contragents": added_contragents,
            "deleted_contragents": deleted_contragents,
            "exited_contragents": exited_contragents,
            "entered_contragents": entered_contragents,
        }

    async def get_contragents_data(self):
        contragents_ids = await collect_objects(
            self.segment_obj.id,
            SegmentObjectType.contragents.value,
            SegmentChangeType.active.value,
        )
        query = contragents.select().where(contragents.c.id.in_(contragents_ids))
        objs = await database.fetch_all(query)
        return [{"id": obj.id, "name": obj.name, "phone": obj.phone} for obj in objs]

    async def added_contragents_data(self):
        contragents_ids = await collect_objects(
            self.segment_obj.id,
            SegmentObjectType.contragents.value,
            SegmentChangeType.new.value,
        )
        query = contragents.select().where(contragents.c.id.in_(contragents_ids))
        objs = await database.fetch_all(query)
        return [{"id": obj.id, "name": obj.name, "phone": obj.phone} for obj in objs]

    async def deleted_contragents_data(self):
        contragents_ids = await collect_objects(
            self.segment_obj.id,
            SegmentObjectType.contragents.value,
            SegmentChangeType.removed.value,
        )
        query = contragents.select().where(contragents.c.id.in_(contragents_ids))
        objs = await database.fetch_all(query)
        return [{"id": obj.id, "name": obj.name, "phone": obj.phone} for obj in objs]

    async def exited_contragents_data(self):
        query = (
            select(contragents)
            .select_from(
                segment_objects.join(
                    contragents, segment_objects.c.object_id == contragents.c.id
                )
            )
            .where(
                segment_objects.c.segment_id == self.segment_obj.id,
                segment_objects.c.object_type == SegmentObjectType.contragents.value,
                segment_objects.c.valid_to.isnot(None),
            )
        )
        objs = await database.fetch_all(query)
        return [
            {
                "id": obj.id,
                "name": obj.name,
                "phone": obj.phone,
            }
            for obj in objs
        ]

    async def entered_contragents_data(self):
        query = (
            select(contragents)
            .select_from(
                segment_objects.join(
                    contragents, segment_objects.c.object_id == contragents.c.id
                )
            )
            .where(
                segment_objects.c.segment_id == self.segment_obj.id,
                segment_objects.c.object_type == SegmentObjectType.contragents.value,
            )
        )
        objs = await database.fetch_all(query)
        return [
            {
                "id": obj.id,
                "name": obj.name,
                "phone": obj.phone,
            }
            for obj in objs
        ]
