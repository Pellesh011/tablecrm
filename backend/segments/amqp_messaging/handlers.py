from typing import Any, Mapping, Optional

from aio_pika import IncomingMessage
from api.webhooks.producer import WebhookMessage
from api.webhooks.schemas import SegmentWebhookPayloadData
from common.amqp_messaging.common.core.EventHandler import IEventHandler
from database.db import database, segments
from segments.main import update_segment_task


class SegmentStartHandler(IEventHandler):
    async def __call__(
        self, event: Mapping[str, Any], message: Optional[IncomingMessage] = None
    ):
        segment_message = WebhookMessage(**event)
        segment_message_data = SegmentWebhookPayloadData(**segment_message.data)

        actions_dict = (
            segment_message_data.actions.dict(exclude_none=True)
            if segment_message_data.actions is not None
            else {}
        )
        criteria_dict = (
            segment_message_data.criteria.dict(exclude_none=True)
            if segment_message_data.criteria is not None
            else {}
        )

        update_query = (
            segments.update()
            .where(
                segments.c.id == segment_message_data.segment_id,
                segments.c.is_deleted == False,
            )
            .where(segments.c.cashbox_id == segment_message_data.cashbox_id)
            .values(criteria=criteria_dict, actions=actions_dict)
        )

        await database.execute(update_query)
        await update_segment_task(segment_message_data.segment_id)
