from api.webhooks.producer import WebhookProducer
from api.webhooks.schemas import (
    SegmentWebhookPayloadData,
    WebhookEventType,
    WebhookPayload,
)
from database.db import database, integrations, users_cboxes_relation
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import ValidationError
from starlette import status

router = APIRouter(tags=["webhooks"])

security = HTTPBearer()

EVENT_MODEL_PROPERTIES = {WebhookEventType.SEGMENTS_START: SegmentWebhookPayloadData}


@router.post("/webhooks")
async def webhook_entry(
    request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Единая точка входа для обработки всех входящих вебхуков системы.
    Валидирует client_secret пользовательской интеграции из Bearer token.
    Payload формат:
    {
        "event": "segments.start...",
        "data": {...}
    }

    Пример:
    {
      "event": "segment.start",
      "data": {
        "segment_id": 68,
        "criteria": {"purchases": {"is_fully_paid": true, "date_range": {"gte_seconds_ago": 604800}}},
        "actions": null
      }
    }

    Args:
        request: Request объект с данными вебхука
        credentials: HTTPAuthorizationCredentials из Bearer токена
    Raises:
        HTTPException: 401 если секретный ключ недействителен
    """
    secret_key = credentials.credentials

    integration_query = integrations.select().where(
        integrations.c.client_secret == secret_key, integrations.c.status == True
    )
    integration = await database.fetch_one(integration_query)

    cashbox_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.id == integration.owner
    )
    cashbox_id = (await database.fetch_one(cashbox_query)).cashbox_id

    if not integration:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid secret key!"
        )

    try:
        payload = WebhookPayload(**await request.json())
        payload.data["cashbox_id"] = cashbox_id
        validation_model = EVENT_MODEL_PROPERTIES[payload.event]
        data = validation_model(**payload.data)
    except ValidationError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Invalid webhook payload!"
        )

    if payload.event not in integration.scopes:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Event '{payload.event}' not allowed in integration scopes. Allowed: {integration.scopes}",
        )

    consumer = WebhookProducer(payload.event, data.dict(exclude_none=True))
    await consumer.produce()

    return {"status": "queued"}
