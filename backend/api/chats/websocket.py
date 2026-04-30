import base64
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from api.chats import crud
from api.chats.auth import get_current_user
from api.chats.producer import chat_producer
from api.chats.schemas import S3_CHAT_FILE_SAVE_FORMAT
from common.utils.url_helper import get_app_url_for_environment
from database.db import (
    MessageType,
    channel_credentials,
    chat_messages,
    database,
    pictures,
)
from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/ws", tags=["chats-ws"])
logger = logging.getLogger(__name__)


def _parse_data_url(data_url: str) -> Optional[Tuple[bytes, Optional[str]]]:
    """Декодировать data: URL в (bytes, content_type). Telegram API не принимает data: URL."""
    if not data_url.startswith("data:") or "," not in data_url:
        return None
    header, encoded = data_url.split(",", 1)
    content_type = header.split(";")[0].split(":")[1] if ":" in header else None
    try:
        return base64.b64decode(encoded), content_type
    except Exception:
        return None


def _normalize_telegram_file_url(file_url: Optional[str]) -> Optional[str]:
    if not file_url:
        return file_url
    if isinstance(file_url, str) and file_url.strip().startswith("data:"):
        return file_url

    app_url = get_app_url_for_environment()
    if not app_url:
        return file_url

    if not app_url.startswith("http"):
        app_url = f"https://{app_url}"

    scheme, host = app_url.split("://", 1)

    if file_url.startswith("http://") or file_url.startswith("https://"):
        if "api.telegram.org/" in file_url:
            return file_url
        if host in file_url:
            normalized = file_url.split(host, 1)[-1].lstrip("/")
            if normalized.startswith(host):
                normalized = normalized.split(host, 1)[-1].lstrip("/")
            if normalized.startswith("api/v1/photos/"):
                return f"{scheme}://{host}/{normalized}"
            if normalized.startswith("photos/") or normalized.startswith(
                "chats_files/"
            ):
                return f"{scheme}://{host}/api/v1/photos/{normalized}"
        return file_url

    normalized = file_url.lstrip("/")
    if normalized.startswith("api/v1/photos/"):
        return f"{app_url.rstrip('/')}/{normalized}"
    if normalized.startswith("photos/") or normalized.startswith("chats_files/"):
        return f"{app_url.rstrip('/')}/api/v1/photos/{normalized}"
    return f"{app_url.rstrip('/')}/{normalized}"


async def _is_duplicate_message(
    chat_id: int,
    sender_type: str,
    message_type: str,
    content: str,
    window_seconds: int = 5,
) -> bool:
    if not content:
        return False

    last_message = await database.fetch_one(
        chat_messages.select()
        .where(
            (chat_messages.c.chat_id == chat_id)
            & (chat_messages.c.sender_type == sender_type)
            & (chat_messages.c.message_type == message_type)
        )
        .order_by(chat_messages.c.created_at.desc())
        .limit(1)
    )

    if not last_message:
        return False

    last_content = (last_message["content"] or "").strip()
    if last_content != content.strip():
        return False

    last_created = last_message.get("created_at")
    if not last_created:
        return False

    delta = datetime.utcnow() - last_created
    return delta.total_seconds() <= window_seconds


@dataclass
class ChatConnectionInfo:
    websocket: WebSocket
    user_id: int
    user_type: str
    connected_at: datetime


class ChatConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[ChatConnectionInfo]] = {}

    async def connect(
        self, chat_id: int, websocket: WebSocket, user_id: int, user_type: str
    ):
        if chat_id not in self.active_connections:
            self.active_connections[chat_id] = []

        connection_info = ChatConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            user_type=user_type,
            connected_at=datetime.utcnow(),
        )

        self.active_connections[chat_id].append(connection_info)

    async def disconnect(
        self, chat_id: int, websocket: WebSocket
    ) -> Optional[ChatConnectionInfo]:
        if chat_id in self.active_connections:
            connection_info = None
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    connection_info = conn_info
                    self.active_connections[chat_id].remove(conn_info)
                    break

            if not self.active_connections[chat_id]:
                del self.active_connections[chat_id]

            if connection_info:
                return connection_info
            else:
                return None
        else:
            return None

    def get_connection_info(
        self, chat_id: int, websocket: WebSocket
    ) -> Optional[ChatConnectionInfo]:
        if chat_id in self.active_connections:
            for conn_info in self.active_connections[chat_id]:
                if conn_info.websocket == websocket:
                    return conn_info
        return None

    def get_connected_users(self, chat_id: int) -> List[Dict]:
        if chat_id not in self.active_connections:
            return []

        users = []
        for conn_info in self.active_connections[chat_id]:
            users.append(
                {
                    "user_id": conn_info.user_id,
                    "user_type": conn_info.user_type,
                    "connected_at": conn_info.connected_at.isoformat(),
                }
            )

        return users

    async def broadcast_to_chat(
        self,
        chat_id: int,
        message: dict,
        exclude_websocket: Optional[
            WebSocket
        ] = None,  # FIX: не отправляем эхо отправителю
    ):
        if chat_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[chat_id]):
                if exclude_websocket and conn_info.websocket is exclude_websocket:
                    continue
                try:
                    await conn_info.websocket.send_json(message)
                except Exception as e:
                    disconnected_clients.append(i)

            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[chat_id].pop(i)
                except Exception:
                    pass


chat_manager = ChatConnectionManager()


@dataclass
class CashboxConnectionInfo:
    websocket: WebSocket
    user_id: int
    cashbox_id: int
    connected_at: datetime


class CashboxConnectionManager:
    def __init__(self):
        self.active_connections: Dict[int, List[CashboxConnectionInfo]] = {}

    async def connect(self, cashbox_id: int, websocket: WebSocket, user_id: int):
        if cashbox_id not in self.active_connections:
            self.active_connections[cashbox_id] = []

        connection_info = CashboxConnectionInfo(
            websocket=websocket,
            user_id=user_id,
            cashbox_id=cashbox_id,
            connected_at=datetime.utcnow(),
        )

        self.active_connections[cashbox_id].append(connection_info)

    async def disconnect(
        self, cashbox_id: int, websocket: WebSocket
    ) -> Optional[CashboxConnectionInfo]:
        if cashbox_id in self.active_connections:
            connection_info = None
            for conn_info in self.active_connections[cashbox_id]:
                if conn_info.websocket == websocket:
                    connection_info = conn_info
                    self.active_connections[cashbox_id].remove(conn_info)
                    break

            if not self.active_connections[cashbox_id]:
                del self.active_connections[cashbox_id]

            return connection_info
        return None

    async def broadcast_to_cashbox(self, cashbox_id: int, message: dict):
        if cashbox_id in self.active_connections:
            disconnected_clients = []
            for i, conn_info in enumerate(self.active_connections[cashbox_id]):
                try:
                    await conn_info.websocket.send_json(message)
                except Exception:
                    disconnected_clients.append(i)

            for i in reversed(disconnected_clients):
                try:
                    self.active_connections[cashbox_id].pop(i)
                except Exception:
                    pass


cashbox_manager = CashboxConnectionManager()


@router.websocket("/chats/all/{token}/")
async def websocket_all_chats(websocket: WebSocket, token: str):
    try:
        await websocket.accept()
    except Exception as e:
        return

    cashbox_id = None
    try:
        try:
            user = await get_current_user(token)
        except HTTPException as e:
            error_detail = e.detail if hasattr(e, "detail") else str(e)
            try:
                await websocket.send_json(
                    {
                        "error": "Unauthorized",
                        "detail": error_detail,
                        "status_code": e.status_code,
                    }
                )
                await websocket.close(code=1008)
            except Exception:
                pass
            return
        except Exception as e:
            try:
                await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
                await websocket.close(code=1008)
            except Exception:
                pass
            return

        cashbox_id = user.cashbox_id
        await cashbox_manager.connect(cashbox_id, websocket, user.user)

        try:
            await websocket.send_json(
                {
                    "type": "connected",
                    "cashbox_id": cashbox_id,
                    "user_id": user.user,
                    "message": "Successfully connected to all chats",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
        except Exception as e:
            pass

        while True:
            try:
                data = await websocket.receive_text()
                message_data = json.loads(data)
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError:
                continue
            except Exception as e:
                continue

    except WebSocketDisconnect:
        if cashbox_id is not None:
            try:
                await cashbox_manager.disconnect(cashbox_id, websocket)
            except Exception:
                pass
    except Exception as e:
        if cashbox_id is not None:
            try:
                await cashbox_manager.disconnect(cashbox_id, websocket)
            except Exception:
                pass


@router.websocket("/chats/{chat_id}/{token}/")
async def websocket_chat(chat_id: int, websocket: WebSocket, token: str):
    await websocket.accept()

    try:
        try:
            user = await get_current_user(token)
        except HTTPException as e:
            error_detail = e.detail if hasattr(e, "detail") else str(e)
            await websocket.send_json(
                {
                    "error": "Unauthorized",
                    "detail": error_detail,
                    "status_code": e.status_code,
                }
            )
            await websocket.close(code=1008)
            return
        except Exception as e:
            await websocket.send_json({"error": "Unauthorized", "detail": str(e)})
            await websocket.close(code=1008)
            return

        chat = await crud.get_chat(chat_id)
        if not chat:
            await websocket.send_json({"error": "Chat not found", "chat_id": chat_id})
            await websocket.close(code=1008)
            return

        chat_cashbox_id = (
            chat.get("cashbox_id") if isinstance(chat, dict) else chat.cashbox_id
        )

        if chat_cashbox_id != user.cashbox_id:
            await websocket.send_json(
                {
                    "error": "Access denied",
                    "detail": "Chat belongs to different cashbox",
                    "chat_cashbox_id": chat_cashbox_id,
                    "user_cashbox_id": user.cashbox_id,
                }
            )
            await websocket.close(code=1008)
            return

        user_type = "OPERATOR"

        await chat_manager.connect(chat_id, websocket, user.user, user_type)

        try:
            await chat_producer.send_user_connected_event(chat_id, user.user, user_type)
        except Exception as e:
            pass

        try:
            await websocket.send_json(
                {
                    "type": "connected",
                    "chat_id": chat_id,
                    "user_id": user.user,
                    "user_type": user_type,
                    "message": "Successfully connected to chat",
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
        except Exception as e:
            pass

        while True:
            try:
                data = await websocket.receive_text()
                message_data = json.loads(data)

                event_type = message_data.get("type", "message")
            except WebSocketDisconnect:
                raise
            except json.JSONDecodeError as e:
                await websocket.send_json({"error": "Invalid JSON", "detail": str(e)})
                continue
            except Exception as e:
                try:
                    await websocket.send_json(
                        {"error": "Failed to process message", "detail": str(e)}
                    )
                except:
                    pass
                continue

            if event_type == "message":
                sender_type = message_data.get("sender_type", "OPERATOR").upper()
                message_type = message_data.get("message_type", "TEXT").upper()

                content_value = message_data.get("content", "")
                if await _is_duplicate_message(
                    chat_id, sender_type, message_type, content_value
                ):
                    try:
                        await websocket.send_json(
                            {
                                "type": "duplicate_message",
                                "chat_id": chat_id,
                                "message_type": message_type,
                                "timestamp": datetime.utcnow().isoformat(),
                            }
                        )
                    except Exception:
                        pass
                    continue

                try:
                    db_message = await crud.create_message_and_update_chat(
                        chat_id=chat_id,
                        sender_type=sender_type,
                        content=content_value,
                        message_type=message_type,
                        status="SENT",
                        source="web",
                    )
                except Exception as e:
                    await websocket.send_json(
                        {"error": "Failed to save message", "detail": str(e)}
                    )
                    continue

                file_urls = []
                if message_data.get("files"):
                    file_urls.extend(message_data.get("files") or [])
                if message_data.get("image_url"):
                    file_urls.append(message_data.get("image_url"))
                if message_data.get("file_url"):
                    file_urls.append(message_data.get("file_url"))
                if (
                    message_type == "IMAGE"
                    and not file_urls
                    and isinstance(content_value, str)
                    and content_value.strip().startswith("data:")
                ):
                    file_urls = [content_value.strip()]

                normalized_files = []
                for url in file_urls:
                    if isinstance(url, str):
                        normalized_files.append(_normalize_telegram_file_url(url))
                file_urls = normalized_files

                urls_to_insert = []
                for url in file_urls:
                    if not isinstance(url, str):
                        continue
                    if url.strip().startswith("data:"):
                        parsed = _parse_data_url(url)
                        if parsed:
                            data_url_bytes, data_url_content_type = parsed
                            try:
                                import io
                                import os
                                from uuid import uuid4

                                import aioboto3

                                extension = "jpg"
                                if data_url_content_type:
                                    if "png" in (data_url_content_type or ""):
                                        extension = "png"
                                    elif "gif" in (data_url_content_type or ""):
                                        extension = "gif"
                                    elif "webp" in (data_url_content_type or ""):
                                        extension = "webp"
                                s3_session = aioboto3.Session()
                                s3_data = {
                                    "service_name": "s3",
                                    "endpoint_url": os.environ.get("S3_URL"),
                                    "aws_access_key_id": os.environ.get("S3_ACCESS"),
                                    "aws_secret_access_key": os.environ.get(
                                        "S3_SECRET"
                                    ),
                                }
                                bucket_name = "5075293c-docs_generated"

                                file_link = S3_CHAT_FILE_SAVE_FORMAT.format(
                                    prefix="photos",
                                    cashbox_id=chat["cashbox_id"],
                                    channel_id=chat["channel_id"],
                                    date_path=datetime.utcnow().strftime("%Y/%m/%d"),
                                    message_id=db_message["id"],
                                    message_hex=uuid4().hex[:8],
                                    extension=extension,
                                )

                                async with s3_session.client(**s3_data) as s3:
                                    await s3.upload_fileobj(
                                        io.BytesIO(data_url_bytes),
                                        bucket_name,
                                        file_link,
                                    )
                                urls_to_insert.append(file_link)
                            except Exception:
                                urls_to_insert.append(url)
                        else:
                            urls_to_insert.append(url)
                    else:
                        urls_to_insert.append(url)

                if urls_to_insert:
                    for url in urls_to_insert:
                        try:
                            await database.execute(
                                pictures.insert().values(
                                    entity="messages",
                                    entity_id=db_message["id"],
                                    url=url,
                                    is_main=False,
                                    is_deleted=False,
                                    owner=user.user,
                                    cashbox=user.cashbox_id,
                                )
                            )
                        except Exception:
                            pass

                    if (
                        message_type != "TEXT"
                        and isinstance(db_message.get("content"), str)
                        and db_message.get("content", "").strip().startswith("[")
                    ):
                        try:
                            content_url = (
                                urls_to_insert[0]
                                if urls_to_insert
                                else (file_urls[0] if file_urls else None)
                            )
                            if content_url:
                                await crud.update_message(
                                    db_message["id"], content=content_url
                                )
                        except Exception:
                            pass

                if sender_type == "OPERATOR":
                    try:
                        channel = await crud.get_channel(chat["channel_id"])
                        if (
                            channel
                            and channel.get("type") == "TELEGRAM"
                            and chat.get("external_chat_id")
                        ):
                            from api.chats.avito.avito_factory import (
                                _decrypt_credential,
                            )
                            from api.chats.telegram.telegram_client import (
                                send_document,
                                send_message,
                                send_photo,
                                send_video,
                            )

                            creds = await database.fetch_one(
                                channel_credentials.select().where(
                                    (channel_credentials.c.channel_id == channel["id"])
                                    & (
                                        channel_credentials.c.cashbox_id
                                        == user.cashbox_id
                                    )
                                    & (channel_credentials.c.is_active.is_(True))
                                )
                            )
                            if creds:
                                bot_token = _decrypt_credential(creds["api_key"])
                                chat_id_external = chat["external_chat_id"]
                                payload_url = file_urls[0] if file_urls else None

                                file_payload = payload_url
                                file_filename = None
                                if isinstance(
                                    payload_url, str
                                ) and payload_url.startswith("data:"):
                                    parsed = _parse_data_url(payload_url)
                                    if parsed:
                                        file_bytes, content_type = parsed
                                        file_payload = file_bytes
                                        if content_type and "png" in (
                                            content_type or ""
                                        ):
                                            file_filename = "image.png"
                                        elif content_type and "gif" in (
                                            content_type or ""
                                        ):
                                            file_filename = "image.gif"
                                        elif content_type and "pdf" in (
                                            content_type or ""
                                        ):
                                            file_filename = "document.pdf"
                                        else:
                                            file_filename = "image.jpg"
                                    else:
                                        file_payload = None

                                if message_type == "IMAGE" and file_payload:
                                    cap = message_data.get("content") or ""
                                    if cap.startswith("data:") or len(cap) > 1024:
                                        cap = None
                                    send_result = await send_photo(
                                        bot_token,
                                        chat_id_external,
                                        file_payload,
                                        caption=cap,
                                        filename=file_filename,
                                    )
                                elif message_type == "VIDEO" and file_payload:
                                    send_result = await send_video(
                                        bot_token,
                                        chat_id_external,
                                        file_payload,
                                        caption=message_data.get("content"),
                                        filename=file_filename,
                                    )
                                elif message_type == "DOCUMENT" and file_payload:
                                    send_result = await send_document(
                                        bot_token,
                                        chat_id_external,
                                        file_payload,
                                        caption=message_data.get("content"),
                                        filename=file_filename,
                                    )
                                else:
                                    send_result = await send_message(
                                        bot_token,
                                        chat_id_external,
                                        message_data.get("content", ""),
                                    )

                                if send_result and send_result.get("message_id"):
                                    await crud.update_message(
                                        db_message["id"],
                                        external_message_id=str(
                                            send_result.get("message_id")
                                        ),
                                        status="DELIVERED",
                                    )
                                else:
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )
                        elif (
                            channel
                            and channel.get("type") == "AVITO"
                            and chat.get("external_chat_id")
                        ):
                            from api.chats.avito.avito_factory import (
                                create_avito_client,
                                save_token_callback,
                            )

                            client = await create_avito_client(
                                channel_id=channel["id"],
                                cashbox_id=user.cashbox_id,
                                on_token_refresh=lambda token_data: save_token_callback(
                                    channel["id"], user.cashbox_id, token_data
                                ),
                            )
                            if client:
                                external_chat_id = str(chat["external_chat_id"])
                                content_str = (
                                    message_data.get("content") or ""
                                ).strip()
                                image_id = None
                                if message_type == "IMAGE":
                                    payload_url = (
                                        content_str
                                        if content_str.startswith("data:")
                                        else (file_urls[0] if file_urls else None)
                                    )
                                    if (
                                        payload_url
                                        and isinstance(payload_url, str)
                                        and payload_url.startswith("data:")
                                    ):
                                        parsed = _parse_data_url(payload_url)
                                        if parsed:
                                            image_data, content_type = parsed
                                            filename = "image.jpg"
                                            if content_type:
                                                if "png" in (content_type or ""):
                                                    filename = "image.png"
                                                elif "gif" in (content_type or ""):
                                                    filename = "image.gif"
                                                elif "webp" in (content_type or ""):
                                                    filename = "image.webp"
                                            try:
                                                upload_result = (
                                                    await client.upload_image(
                                                        image_data, filename=filename
                                                    )
                                                )
                                                if upload_result:
                                                    image_id = upload_result[0]
                                            except Exception as e:
                                                logger.warning(
                                                    "Avito upload_image in WS failed: %s",
                                                    e,
                                                )
                                send_text = content_str and not content_str.startswith(
                                    "data:"
                                )
                                try:
                                    if image_id and send_text:
                                        avito_image = await client.send_message(
                                            chat_id=external_chat_id,
                                            text=None,
                                            image_id=image_id,
                                        )
                                        await client.send_message(
                                            chat_id=external_chat_id,
                                            text=content_str,
                                            image_id=None,
                                        )
                                        if avito_image.get("id"):
                                            await crud.update_message(
                                                db_message["id"],
                                                external_message_id=str(
                                                    avito_image.get("id")
                                                ),
                                                status="DELIVERED",
                                            )
                                        else:
                                            await crud.update_message(
                                                db_message["id"], status="FAILED"
                                            )
                                    elif image_id:
                                        avito_resp = await client.send_message(
                                            chat_id=external_chat_id,
                                            text=None,
                                            image_id=image_id,
                                        )
                                        if avito_resp.get("id"):
                                            await crud.update_message(
                                                db_message["id"],
                                                external_message_id=str(
                                                    avito_resp.get("id")
                                                ),
                                                status="DELIVERED",
                                            )
                                        else:
                                            await crud.update_message(
                                                db_message["id"], status="FAILED"
                                            )
                                    elif send_text:
                                        avito_resp = await client.send_message(
                                            chat_id=external_chat_id,
                                            text=content_str or " ",
                                            image_id=None,
                                        )
                                        if avito_resp.get("id"):
                                            await crud.update_message(
                                                db_message["id"],
                                                external_message_id=str(
                                                    avito_resp.get("id")
                                                ),
                                                status="DELIVERED",
                                            )
                                        else:
                                            await crud.update_message(
                                                db_message["id"], status="FAILED"
                                            )
                                    else:
                                        await crud.update_message(
                                            db_message["id"], status="FAILED"
                                        )
                                except Exception as e:
                                    logger.warning(
                                        "Failed to send message to Avito via WS: %s",
                                        e,
                                    )
                                    try:
                                        await crud.update_message(
                                            db_message["id"], status="FAILED"
                                        )
                                    except Exception:
                                        pass
                            else:
                                logger.warning(
                                    "Could not create Avito client for channel %s (WS)",
                                    channel["id"],
                                )
                        elif (
                            channel
                            and channel.get("type") == "MAX"
                            and chat.get("external_chat_id")
                        ):
                            from api.chats.avito.avito_factory import (
                                _decrypt_credential,
                            )
                            from api.chats.max.max_handler import send_operator_message

                            creds = await database.fetch_one(
                                channel_credentials.select().where(
                                    (channel_credentials.c.channel_id == channel["id"])
                                    & (
                                        channel_credentials.c.cashbox_id
                                        == user.cashbox_id
                                    )
                                    & (channel_credentials.c.is_active.is_(True))
                                )
                            )
                            if creds:
                                bot_token = _decrypt_credential(creds["api_key"])

                                # Определяем тип сообщения и файлы
                                msg_type = message_type
                                files_to_send = file_urls if file_urls else None

                                # Если тип TEXT, но есть data:image, то меняем на IMAGE
                                if (
                                    msg_type == "TEXT"
                                    and files_to_send
                                    and any(
                                        f.startswith("data:image")
                                        for f in files_to_send
                                    )
                                ):
                                    msg_type = "IMAGE"
                                    print(
                                        "[WebSocket] Detected IMAGE from data URL, overriding type to IMAGE"
                                    )

                                # Для IMAGE создаём files_to_send из content, если нужно
                                if (
                                    msg_type == "IMAGE"
                                    and not files_to_send
                                    and message_data.get("content", "").startswith(
                                        "data:image"
                                    )
                                ):
                                    files_to_send = [message_data.get("content")]

                                external_id = await send_operator_message(
                                    chat=chat,
                                    text=(
                                        message_data.get("content")
                                        if msg_type == "TEXT"
                                        else None
                                    ),
                                    image_url=message_data.get("image_url")
                                    or (
                                        files_to_send[0]
                                        if files_to_send and msg_type == "IMAGE"
                                        else None
                                    ),
                                    cashbox_id=user.cashbox_id,
                                    bot_token=bot_token,
                                    files=(
                                        files_to_send
                                        if msg_type in ("DOCUMENT", "VIDEO", "VOICE")
                                        else None
                                    ),
                                    message_type=msg_type,
                                )
                                if external_id:
                                    await crud.update_message(
                                        db_message["id"],
                                        external_message_id=external_id,
                                        status="DELIVERED",
                                    )
                                else:
                                    await crud.update_message(
                                        db_message["id"], status="FAILED"
                                    )
                            else:
                                print(
                                    f"[WebSocket] No credentials for MAX channel {channel['id']}"
                                )
                    except Exception:
                        try:
                            await crud.update_message(db_message["id"], status="FAILED")
                        except Exception:
                            pass

                try:
                    await chat_producer.send_message(
                        chat_id,
                        {
                            "message_id": db_message["id"],
                            "sender_type": sender_type,
                            "content": message_data.get("content", ""),
                            "message_type": message_type,
                            "timestamp": datetime.utcnow().isoformat(),
                        },
                    )
                except Exception:
                    pass

                try:
                    ts = datetime.utcnow().isoformat()
                    content = (
                        db_message.get("content")
                        or message_data.get("content", "")
                        or ""
                    )
                    ws_message = {
                        "type": "message",
                        "message_id": db_message["id"],
                        "chat_id": chat_id,
                        "sender_type": sender_type,
                        "content": content,
                        "message_type": message_type,
                        "status": "DELIVERED",
                        "timestamp": ts,
                    }

                    attachment_type = None
                    if message_type == "IMAGE":
                        attachment_type = "image"
                    file_types = [
                        MessageType.VIDEO,
                        MessageType.DOCUMENT,
                        MessageType.VOICE,
                    ]
                    if message_type in file_types:
                        attachment_type = "file"

                    if attachment_type is not None and content:
                        content_str = content if isinstance(content, str) else ""
                        if (
                            content_str.startswith(f"data:{attachment_type}")
                            or content_str.startswith("http://")
                            or content_str.startswith("https://")
                        ):
                            ws_message[f"{attachment_type}_url"] = content_str
                        else:
                            full_url = _normalize_telegram_file_url(content_str)
                            if full_url:
                                ws_message[f"{attachment_type}_url"] = full_url

                    await chat_manager.broadcast_to_chat(chat_id, ws_message)
                    cashbox_payload = {
                        "type": "chat_message",
                        "event": "new_message",
                        "chat_id": chat_id,
                        "message_id": db_message["id"],
                        "sender_type": sender_type,
                        "content": content,
                        "message_type": message_type,
                        "timestamp": ts,
                    }
                    if ws_message.get("image_url"):
                        cashbox_payload["image_url"] = ws_message["image_url"]
                    if ws_message.get("file_url"):
                        cashbox_payload["file_url"] = ws_message["file_url"]
                    await cashbox_manager.broadcast_to_cashbox(
                        user.cashbox_id,
                        cashbox_payload,
                    )
                except Exception as e:
                    logger.warning(
                        "Failed to broadcast new message via WebSocket: %s", e
                    )

            elif event_type == "typing":
                is_typing = message_data.get("is_typing", False)
                operator_name = getattr(user, "name", None) or getattr(
                    user, "email", None
                )

                # FIX: прямой broadcast с исключением отправителя (без эхо).
                # Остальные операторы в этом чате мгновенно видят "Иван печатает..."
                await chat_manager.broadcast_to_chat(
                    chat_id,
                    {
                        "type": "typing",
                        "chat_id": chat_id,
                        "user_id": user.user,
                        "user_type": user_type,
                        "user_name": operator_name,
                        "is_typing": is_typing,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                    exclude_websocket=websocket,  # не возвращаем себе
                )
                # RabbitMQ для multi-worker (в single-worker можно отключить).
                # Примечание: consumer тоже вызовет broadcast_to_chat, что приведёт
                # к дублированию в single-worker. Для production с несколькими
                # воркерами раскомментируйте и добавьте дедупликацию через Redis.
                # try:
                #     await chat_producer.send_typing_event(
                #         chat_id, user.user, user_type, is_typing,
                #         operator_name=operator_name,
                #     )
                # except Exception:
                #     pass

            elif event_type == "get_users":
                users = chat_manager.get_connected_users(chat_id)
                await websocket.send_json(
                    {
                        "type": "users_list",
                        "chat_id": chat_id,
                        "users": users,
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

            else:
                await websocket.send_json(
                    {"error": "Unknown event type", "type": event_type}
                )

    except WebSocketDisconnect:
        connection_info = await chat_manager.disconnect(chat_id, websocket)
        if connection_info:
            try:
                await chat_producer.send_user_disconnected_event(
                    chat_id, connection_info.user_id, connection_info.user_type
                )
            except Exception as e:
                pass
    except Exception as e:
        try:
            connection_info = await chat_manager.disconnect(chat_id, websocket)
            if connection_info:
                try:
                    await chat_producer.send_user_disconnected_event(
                        chat_id, connection_info.user_id, connection_info.user_type
                    )
                except Exception as e2:
                    pass
        except Exception as disconnect_error:
            pass
