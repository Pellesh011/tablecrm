import io
import json
import logging
import socket
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
TELEGRAM_FILE_BASE = "https://api.telegram.org/file/bot{token}/{file_path}"


class TelegramAPIError(RuntimeError):
    pass


async def _request(
    token: str,
    method: str,
    data: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    form: Optional[aiohttp.FormData] = None,
) -> Dict[str, Any]:
    url = f"{TELEGRAM_API_BASE.format(token=token)}/{method}"

    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        if form is not None:
            async with session.post(url, data=form) as response:
                payload = await response.json()
        elif json_payload is not None:
            async with session.post(url, json=json_payload) as response:
                payload = await response.json()
        else:
            async with session.post(url, data=data or {}) as response:
                payload = await response.json()

    if not payload.get("ok"):
        raise TelegramAPIError(payload.get("description") or "Telegram API error")

    return payload


async def get_me(token: str) -> Dict[str, Any]:
    payload = await _request(token, "getMe")
    return payload.get("result", {})


async def get_webhook_info(token: str) -> Dict[str, Any]:
    payload = await _request(token, "getWebhookInfo")
    return payload.get("result", {})


async def set_webhook(token: str, url: str, secret_token: str) -> Dict[str, Any]:
    payload = await _request(
        token,
        "setWebhook",
        json_payload={"url": url, "secret_token": secret_token},
    )
    return payload.get("result", {})


async def delete_webhook(
    token: str, drop_pending_updates: bool = False
) -> Dict[str, Any]:
    payload = await _request(
        token,
        "deleteWebhook",
        data={"drop_pending_updates": str(drop_pending_updates).lower()},
    )
    return payload.get("result", {})


async def get_updates(
    token: str,
    offset: Optional[int] = None,
    timeout: int = 25,
    limit: int = 100,
    allowed_updates: Optional[list[str]] = None,
) -> list[Dict[str, Any]]:
    data: Dict[str, Any] = {"timeout": timeout, "limit": limit}
    if offset is not None:
        data["offset"] = offset
    if allowed_updates is not None:
        data["allowed_updates"] = json.dumps(allowed_updates)
    payload = await _request(token, "getUpdates", data=data)
    return payload.get("result", [])


async def send_message(
    token: str,
    chat_id: str,
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    data: Dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    payload = await _request(token, "sendMessage", data=data)
    return payload.get("result", {})


async def delete_message(token: str, chat_id: str, message_id: int) -> bool:
    """Удалить сообщение в Telegram-чате.

    Возвращает True при успехе, False если сообщение не найдено или уже удалено.
    """
    try:
        payload = await _request(
            token,
            "deleteMessage",
            data={"chat_id": chat_id, "message_id": str(message_id)},
        )
        return bool(payload.get("result"))
    except TelegramAPIError:
        return False


def _photo_content_type(filename: Optional[str]) -> str:
    if not filename:
        return "image/jpeg"
    fn = (filename or "").lower()
    if fn.endswith(".png"):
        return "image/png"
    if fn.endswith(".gif"):
        return "image/gif"
    if fn.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


async def send_photo(
    token: str,
    chat_id: str,
    photo: Any,
    caption: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    filename: Optional[str] = None,
) -> Dict[str, Any]:
    if isinstance(photo, (bytes, bytearray)):
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        if caption:
            form.add_field("caption", caption)
        if reply_markup:
            form.add_field("reply_markup", json.dumps(reply_markup))
        fname = filename or "photo.jpg"
        form.add_field(
            "photo",
            io.BytesIO(photo),
            filename=fname,
            content_type=_photo_content_type(fname),
        )
        payload = await _request(token, "sendPhoto", form=form)
    else:
        data: Dict[str, Any] = {"chat_id": chat_id, "photo": photo}
        if caption:
            data["caption"] = caption
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        payload = await _request(token, "sendPhoto", data=data)
    return payload.get("result", {})


async def send_document(
    token: str,
    chat_id: str,
    document: Any,
    caption: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    filename: Optional[str] = None,
) -> Dict[str, Any]:
    if isinstance(document, (bytes, bytearray)):
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        if caption:
            form.add_field("caption", caption)
        if reply_markup:
            form.add_field("reply_markup", json.dumps(reply_markup))
        form.add_field("document", document, filename=filename or "file")
        payload = await _request(token, "sendDocument", form=form)
    else:
        data: Dict[str, Any] = {"chat_id": chat_id, "document": document}
        if caption:
            data["caption"] = caption
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        payload = await _request(token, "sendDocument", data=data)
    return payload.get("result", {})


async def send_video(
    token: str,
    chat_id: str,
    video: Any,
    caption: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None,
    filename: Optional[str] = None,
) -> Dict[str, Any]:
    if isinstance(video, (bytes, bytearray)):
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat_id))
        if caption:
            form.add_field("caption", caption)
        if reply_markup:
            form.add_field("reply_markup", json.dumps(reply_markup))
        form.add_field("video", video, filename=filename or "video.mp4")
        payload = await _request(token, "sendVideo", form=form)
    else:
        data: Dict[str, Any] = {"chat_id": chat_id, "video": video}
        if caption:
            data["caption"] = caption
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        payload = await _request(token, "sendVideo", data=data)
    return payload.get("result", {})


async def send_media_group(
    token: str, chat_id: str, media: list[Dict[str, Any]]
) -> list[Dict[str, Any]]:
    payload = await _request(
        token, "sendMediaGroup", json_payload={"chat_id": chat_id, "media": media}
    )
    return payload.get("result", [])


async def answer_callback_query(
    token: str, callback_query_id: str, text: Optional[str] = None
) -> Dict[str, Any]:
    data: Dict[str, Any] = {"callback_query_id": callback_query_id}
    if text:
        data["text"] = text
    payload = await _request(token, "answerCallbackQuery", data=data)
    return payload.get("result", {})


async def get_file(token: str, file_id: str) -> Dict[str, Any]:
    payload = await _request(token, "getFile", data={"file_id": file_id})
    return payload.get("result", {})


async def get_user_profile_photos(
    token: str, user_id: int, limit: int = 1, offset: int = 0
) -> Dict[str, Any]:
    payload = await _request(
        token,
        "getUserProfilePhotos",
        data={"user_id": user_id, "limit": limit, "offset": offset},
    )
    return payload.get("result", {})


async def download_file(token: str, file_path: str) -> bytes:
    url = TELEGRAM_FILE_BASE.format(token=token, file_path=file_path)
    logger.debug(f"Downloading Telegram file from {url}")
    connector = aiohttp.TCPConnector(family=socket.AF_INET)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.read()
