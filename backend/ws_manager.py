from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, token: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.setdefault(token, []).append(websocket)

    async def disconnect(self, token: str, ws: WebSocket):
        if token in self.active_connections:
            self.active_connections[token].remove(ws)
            if not self.active_connections[token]:
                del self.active_connections[token]

    async def send_message(self, token: str, message: dict):
        for ws in self.active_connections.get(token, []):
            try:
                await ws.send_json(message)
            except Exception as e:
                print(e)


manager = ConnectionManager()
