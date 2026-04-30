import json
from datetime import datetime

from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.utils.ioc.ioc import ioc


async def send_segment_notification(
    recipient_ids: list[str] = None,
    notification_text: str = None,
    segment_id: int = None,
) -> bool:
    try:
        notification_data = {
            "type": "segment_notification",
            "recipients": recipient_ids or [],
            "text": notification_text,
            "timestamp": datetime.now().timestamp(),
        }
        factory = ioc.get(IRabbitFactory)
        messaging = await factory()
        await messaging.publish_to_queue(
            "notification_queue", json.dumps(notification_data).encode()
        )
        return True
    except Exception as e:
        print(f"Failed to send notification for segment {segment_id}: {str(e)}")
        return False
