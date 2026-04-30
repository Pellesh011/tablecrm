import asyncio
import json
from datetime import datetime

from database.db import (
    SegmentObjectType,
    SegmentStatus,
    database,
    segments,
)
from segments.actions.actions import SegmentActions
from segments.helpers.functions import format_contragent_text_notifications
from segments.logger import logger
from segments.logic.collect_data import ContragentsData
from segments.logic.logic import SegmentLogic
from segments.query.queries import (
    SegmentCriteriaQuery,
    fetch_contragent_by_id,
    get_token_by_segment_id,
)
from segments.websockets import notify


class Segments:
    def __init__(self, segment_id: int = None):
        self.segment_id = segment_id
        self.segment_obj = None
        self.logic = None
        self.query = None
        self.actions = None

    async def async_init(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_id)
        )
        self.logic = SegmentLogic(self.segment_obj)
        self.query = SegmentCriteriaQuery(
            self.segment_obj.cashbox_id, json.loads(self.segment_obj.criteria)
        )
        self.actions = SegmentActions(self.segment_obj)

    async def refresh_segment_obj(self):
        self.segment_obj = await database.fetch_one(
            segments.select().where(segments.c.id == self.segment_obj.id)
        )

    async def update_segment_datetime(self):
        await database.execute(
            segments.update()
            .where(segments.c.id == self.segment_id)
            .values(
                updated_at=datetime.now(),
                previous_update_at=self.segment_obj.updated_at,
            )
        )
        await self.async_init()

    async def set_status_in_progress(self):
        await database.execute(
            segments.update()
            .where(segments.c.id == self.segment_id)
            .values(status=SegmentStatus.in_process.value)
        )

    async def set_status_calculated(self):
        await database.execute(
            segments.update()
            .where(segments.c.id == self.segment_id)
            .values(status=SegmentStatus.calculated.value)
        )

    async def update_segment(self):
        try:
            start = datetime.now()
            await self.set_status_in_progress()
            new_ids = await self.query.collect_ids()

            # теперь update_segment_data_in_db возвращает changes
            changes = await self.logic.update_segment_data_in_db(new_ids)
            # changes пример:
            # {
            #   "contragents": {"new": [...], "removed": [...], "active": [...]},
            #   "docs_sales": {...}
            # }

            # получаем token для отправки WS
            token = await get_token_by_segment_id(self.segment_id)

            # собираем задачи по уведомлениям
            notify_tasks = []

            contr_changes = changes.get(SegmentObjectType.contragents.value, {})
            # добавленные
            for cid in contr_changes.get("new", []):
                row = await fetch_contragent_by_id(cid)
                if not row:
                    continue
                name = getattr(row, "name", None) or row.get("name")
                phone = getattr(row, "phone", None) or row.get("phone")
                text = format_contragent_text_notifications(
                    "new_contragent", self.segment_obj.name, name or "", phone or ""
                )
                payload = {
                    "type": "contragent_added",
                    "text": text,
                    "contragent": {"id": cid, "name": name, "phone": phone},
                }
                notify_tasks.append(
                    notify(
                        ws_token=token,
                        event="segment_member_added",
                        segment_id=self.segment_id,
                        payload=payload,
                    )
                )

            # удалённые
            for cid in contr_changes.get("removed", []):
                row = await fetch_contragent_by_id(cid)
                # Если контрагент был удалён из справочника — всё равно отправим базовый текст
                name = None
                phone = None
                if row:
                    name = getattr(row, "name", None) or row.get("name")
                    phone = getattr(row, "phone", None) or row.get("phone")
                text = format_contragent_text_notifications(
                    "removed_contragent",
                    self.segment_obj.name,
                    name or "Неизвестно",
                    phone or "Неизвестно",
                )
                payload = {
                    "type": "contragent_removed",
                    "text": text,
                    "contragent": {"id": cid, "name": name, "phone": phone},
                }
                notify_tasks.append(
                    notify(
                        ws_token=token,
                        event="segment_member_removed",
                        segment_id=self.segment_id,
                        payload=payload,
                    )
                )

            if notify_tasks:
                # параллельно отправляем все уведомления
                await asyncio.gather(*notify_tasks)

            # далее стандартная логика
            await self.actions.start_actions()
            await self.update_segment_datetime()
            await self.set_status_calculated()
            logger.info(
                f"Segment {self.segment_id} updated successfully. Start - {start}. Took {datetime.now() - start}"
            )
        except Exception as e:
            await self.set_status_calculated()
            logger.exception(
                f"Ошибка при обновлении сегмента {self.segment_obj.id}: {e}"
            )

    async def collect_data(self):
        data_obj = ContragentsData(self.segment_obj)

        return await data_obj.collect()


async def update_segment_task(segment_id: int):
    token = await get_token_by_segment_id(segment_id)
    segment = Segments(segment_id)
    logger.info(f"Starting update for segment {segment_id} with token {token}")

    await segment.async_init()
    payload = {
        "type": "recalc_start",
        "segment_name": segment.segment_obj.name,
        "segment_id": segment_id,
    }
    await notify(
        ws_token=token, event="recalc_start", segment_id=segment_id, payload=payload
    )

    if getattr(segment.segment_obj, "is_deleted", False):
        logger.info(f"Segment {segment_id} is deleted; skip update.")
        payload = {
            "type": "recalc_fail_410",
            "segment_name": segment.segment_obj.name,
            "segment_id": segment_id,
        }
        await notify(
            ws_token=token,
            event="recalc_fail_410",
            segment_id=segment_id,
            payload=payload,
        )
        return

    if segment.segment_obj:
        await segment.update_segment()
        payload = {
            "type": "recalc_finish",
            "segment_name": segment.segment_obj.name,
            "segment_id": segment_id,
        }
        await notify(
            ws_token=token,
            event="recalc_finish",
            segment_id=segment_id,
            payload=payload,
        )
    else:
        payload = {
            "type": "recalc_fail_404",
            "segment_name": segment.segment_obj.name,
            "segment_id": segment_id,
        }
        await notify(
            ws_token=token,
            event="recalc_fail_404",
            segment_id=segment_id,
            payload=payload,
        )
