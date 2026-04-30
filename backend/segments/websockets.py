from datetime import datetime
from typing import Any, Dict, Optional

from ws_manager import manager


async def notify(
    ws_token: str, event: str, segment_id: int, payload: Optional[Dict[str, Any]] = None
):
    message = {
        "event": event,
        "segment_id": segment_id,
        "timestamp": datetime.now().isoformat(),
    }
    if payload is not None:
        message["payload"] = payload

    # если token пустой — просто выход (никто не слушает)
    if not ws_token:
        return

    await manager.send_message(ws_token, message)
