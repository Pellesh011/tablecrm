from datetime import datetime

from database.db import SegmentObjectType, database, segment_objects
from segments.constants import SegmentChangeType
from segments.helpers.batch_insert import insert_in_batches
from segments.helpers.collect_obj_ids import collect_objects
from sqlalchemy import and_, update


class SegmentLogic:
    """
    Класс для создания логики обновления сегментов.
    """

    def __init__(self, segment_obj):
        self.segment_obj = segment_obj

    async def collect_id_changes(self, new_ids: dict):
        """Сбор всех изменений относительно последнего среза"""

        docs_sales_id_to_update = new_ids.get("docs_sales", [])
        contragents_id_to_update = new_ids.get("contragents", [])
        nomenclatures_id_to_update = new_ids.get("nomenclatures", [])

        docs_sales_ids = await collect_objects(
            self.segment_obj.id,
            SegmentObjectType.docs_sales.value,
            SegmentChangeType.active.value,
        )

        contragents_ids = await collect_objects(
            self.segment_obj.id,
            SegmentObjectType.contragents.value,
            SegmentChangeType.active.value,
        )

        nomenclatures_ids = await collect_objects(
            self.segment_obj.id,
            SegmentObjectType.nomenclatures.value,
            SegmentChangeType.active.value,
        )

        changes = {
            SegmentObjectType.docs_sales.value: {},
            SegmentObjectType.contragents.value: {},
            SegmentObjectType.nomenclatures.value: {},
        }

        # собираем изменения документов продаж
        changes[SegmentObjectType.docs_sales.value][SegmentChangeType.new.value] = list(
            set(docs_sales_id_to_update) - set(docs_sales_ids)
        )
        changes[SegmentObjectType.docs_sales.value][SegmentChangeType.removed.value] = (
            list(set(docs_sales_ids) - set(docs_sales_id_to_update))
        )
        changes[SegmentObjectType.docs_sales.value][
            SegmentChangeType.active.value
        ] = docs_sales_id_to_update

        # собираем изменения контрагентов
        changes[SegmentObjectType.contragents.value][SegmentChangeType.new.value] = (
            list(set(contragents_id_to_update) - set(contragents_ids))
        )
        changes[SegmentObjectType.contragents.value][
            SegmentChangeType.removed.value
        ] = list(set(contragents_ids) - set(contragents_id_to_update))
        changes[SegmentObjectType.contragents.value][
            SegmentChangeType.active.value
        ] = contragents_id_to_update

        # собираем изменения номенклатур
        changes[SegmentObjectType.nomenclatures.value][SegmentChangeType.new.value] = (
            list(set(nomenclatures_id_to_update) - set(nomenclatures_ids))
        )
        changes[SegmentObjectType.nomenclatures.value][
            SegmentChangeType.removed.value
        ] = list(set(nomenclatures_ids) - set(nomenclatures_id_to_update))
        changes[SegmentObjectType.nomenclatures.value][
            SegmentChangeType.active.value
        ] = nomenclatures_id_to_update

        return changes

    async def update_data(self, changes: dict):
        """Метод для записи среза (изменненых id объектов) в сегменте."""

        for object_type, value_data in changes.items():
            if value_data.get(SegmentChangeType.new.value):
                added_data = []
                for id in value_data[SegmentChangeType.new.value]:
                    added_data.append(
                        {
                            "segment_id": self.segment_obj.id,
                            "object_id": id,
                            "object_type": object_type,
                            "valid_from": datetime.now(),
                        }
                    )
                if added_data:
                    await insert_in_batches(
                        segment_objects, added_data, batch_size=3000
                    )

            if ids := value_data.get(SegmentChangeType.removed.value):
                batch_size = 30000  # чуть меньше лимита
                for i in range(0, len(ids), batch_size):
                    chunk = ids[i : i + batch_size]
                    query = (
                        update(segment_objects)
                        .where(
                            and_(
                                segment_objects.c.object_id.in_(chunk),
                                segment_objects.c.object_type == object_type,
                                segment_objects.c.valid_to.is_(None),
                            )
                        )
                        .values(valid_to=datetime.now())
                    )
                    await database.execute(query)
        return

    async def update_segment_data_in_db(self, new_ids: dict):
        """Обновление в БД. Возвращаем changes чтобы верхний уровень мог использовать diff."""
        changes = await self.collect_id_changes(new_ids)
        await self.update_data(changes)

        return changes
