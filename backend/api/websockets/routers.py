import json

from database.db import database, users, users_cboxes_relation
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from ws_manager import manager

router = APIRouter(tags=["ws"])


@router.websocket("/ws/{ws_token}/")
async def websocket(ws_token: str, websocket: WebSocket):
    """Вебсокет"""
    query = users.select().where(users_cboxes_relation.c.token == ws_token)
    user = await database.fetch_one(query=query)
    if user:
        await manager.connect(ws_token, websocket)
        try:
            while True:
                data = await websocket.receive_text()
                try:
                    data_json = json.loads(data)
                    if "super_secret_token" in data_json:
                        super_secret_token = "143a2854998b0c3ab1e0f38b5a66d12024cd088b9eac8ae39df6161313d254fd"
                        if data_json["super_secret_token"] == super_secret_token:
                            tokens_list = data_json["tokens_list"]
                            user = data_json["user"]
                            for token in tokens_list:
                                await manager.send_message(token, user)
                except ValueError:
                    pass
        except WebSocketDisconnect:
            await manager.disconnect(ws_token, websocket)
    else:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
