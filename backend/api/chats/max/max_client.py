"""
api/chats/max/max_client.py

HTTP-клиент для Max Bot API (https://platform-api.max.ru).
Аутентификация — заголовок Authorization: <token> (без Bearer).
"""

import logging
import socket
from typing import Any, Dict, List, Optional
from uuid import uuid4

import aiohttp

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_BOT_API_BASE = "https://platform-api.max.ru"


class MaxAPIError(RuntimeError):
    pass


class MaxClient:
    """Клиент для работы с Max Bot API."""

    def __init__(self, token: str):
        self.token = token
        self._me: Optional[Dict[str, Any]] = None

    async def _request(
        self,
        path: str,
        http_method: str = "GET",
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{MAX_BOT_API_BASE}/{path.lstrip('/')}"
        headers = {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }
        query = params or {}

        connector = aiohttp.TCPConnector(family=socket.AF_INET)
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                kwargs: Dict[str, Any] = {
                    "headers": headers,
                    "params": query,
                    "timeout": aiohttp.ClientTimeout(total=35),
                }
                if http_method in ("POST", "PUT", "PATCH"):
                    kwargs["json"] = data or {}

                async with session.request(http_method, url, **kwargs) as resp:
                    if resp.content_type and "json" in resp.content_type:
                        payload = await resp.json()
                    else:
                        text = await resp.text()
                        logger.warning(
                            "Max API non-JSON response [%s] %s: %s",
                            resp.status,
                            path,
                            text[:300],
                        )
                        payload = {"_raw": text}

                    if resp.status >= 400:
                        error = (
                            payload.get("message")
                            or payload.get("description")
                            or payload.get("_raw")
                            or str(payload)
                        )
                        raise MaxAPIError(
                            f"Max API error {resp.status} on {path}: {error}"
                        )
                    return payload
        except aiohttp.ClientError as exc:
            raise MaxAPIError(f"Max API request failed ({path}): {exc}") from exc

    async def get_me(self) -> Dict[str, Any]:
        """Вернуть информацию о боте."""
        if self._me:
            return self._me
        result = await self._request("/me")
        self._me = result
        return result

    async def get_updates(
        self,
        marker: Optional[int] = None,
        limit: int = 100,
        timeout: int = 25,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit, "timeout": timeout}
        if marker is not None:
            params["marker"] = marker
        return await self._request("/updates", params=params)

    async def set_webhook(self, url: str) -> Dict[str, Any]:
        return await self._request("/subscriptions", "POST", data={"url": url})

    async def delete_webhook(self) -> Dict[str, Any]:
        try:
            return await self._request("/subscriptions/webhook", "DELETE")
        except MaxAPIError as exc:
            logger.warning("Max delete_webhook: %s", exc)
            return {}

    async def get_subscriptions(self) -> Dict[str, Any]:
        return await self._request("/subscriptions")

    async def get_chat(self, chat_id: str) -> Dict[str, Any]:
        return await self._request(f"/chats/{chat_id}")

    async def get_chat_members(
        self, chat_id: str, marker: Optional[int] = None, limit: int = 100
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": limit}
        if marker is not None:
            params["marker"] = marker
        return await self._request(f"/chats/{chat_id}/members", params=params)

    async def send_message(
        self,
        text: str,
        user_id: Optional[int] = None,
        chat_id: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not user_id and not chat_id:
            raise MaxAPIError("send_message: требуется user_id или chat_id")

        params: Dict[str, Any] = {}
        if user_id:
            params["user_id"] = user_id
        if chat_id:
            params["chat_id"] = chat_id

        body: Dict[str, Any] = {"text": text}
        final_attachments = list(attachments) if attachments else []

        if reply_markup:
            inline_keyboard = {
                "type": "inline_keyboard",
                "payload": reply_markup.get("inline_keyboard", reply_markup),
            }
            final_attachments.append(inline_keyboard)

        if final_attachments:
            body["attachments"] = final_attachments

        return await self._request("/messages", "POST", data=body, params=params)

    async def send_message_with_inline_keyboard(
        self,
        text: str,
        user_id: Optional[int] = None,
        chat_id: Optional[str] = None,
        buttons: Optional[List[List[Dict[str, Any]]]] = None,
        format: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Отправляет сообщение с inline-клавиатурой."""
        attachments = []
        if buttons:
            attachments.append(
                {"type": "inline_keyboard", "payload": {"buttons": buttons}}
            )
        body = {"text": text, "attachments": attachments}
        if format:
            body["format"] = format
        params = {}
        if user_id:
            params["user_id"] = user_id
        if chat_id:
            params["chat_id"] = chat_id
        return await self._request("/messages", "POST", data=body, params=params)

    async def delete_message(self, message_id: str) -> bool:
        """Удалить сообщение по его mid."""
        try:
            await self._request(
                "/messages", "DELETE", params={"message_id": message_id}
            )
            return True
        except MaxAPIError as exc:
            logger.warning("Max delete_message failed: %s", exc)
            return False

    async def get_video_info(self, video_token: str) -> Optional[Dict[str, Any]]:
        try:
            result = await self._request(f"/videos/{video_token}")
            return result
        except MaxAPIError as exc:
            logger.warning(
                "Max get_video_info failed for token %s: %s", video_token, exc
            )
            return None

    async def get_user_info(self, user_id: int) -> Dict[str, Any]:
        return await self._request(f"/users/{user_id}")

    async def answer_callback_query(
        self, callback_id: str, text: Optional[str] = None
    ) -> bool:
        """Подтверждает получение callback'а."""
        try:
            data = {"callback_id": callback_id}
            if text:
                data["text"] = text
            await self._request("/callback", "POST", data=data)
            return True
        except MaxAPIError as exc:
            logger.warning(f"answer_callback_query failed: {exc}")
            return False

    async def upload_file(
        self,
        file_bytes: bytes,
        file_type: str = "image",
        filename: Optional[str] = None,
    ) -> Optional[str]:
        content_type = "application/octet-stream"
        if file_type == "image":
            if len(file_bytes) > 4:
                if file_bytes[:4] == b"\x89PNG":
                    content_type = "image/png"
                elif file_bytes[:2] == b"\xff\xd8":
                    content_type = "image/jpeg"
                elif file_bytes[:3] == b"GIF":
                    content_type = "image/gif"
                elif file_bytes[:4] == b"RIFF" and file_bytes[8:12] == b"WEBP":
                    content_type = "image/webp"
            else:
                content_type = "image/jpeg"
        elif file_type == "video":
            content_type = "video/mp4"
        elif file_type == "audio":
            content_type = "audio/mpeg"

        ext = content_type.split("/")[-1] if "/" in content_type else "bin"
        if not filename:
            filename = f"upload_{uuid4().hex}.{ext}"

        logger.info(
            f"[Max] Uploading file: type={file_type}, content_type={content_type}, size={len(file_bytes)} bytes"
        )

        try:
            upload_url_data = await self._request(
                "/uploads", "POST", params={"type": file_type}
            )
            upload_url = upload_url_data.get("url")
            if not upload_url:
                logger.error("[Max] upload_file: No 'url' in response from /uploads")
                return None
            logger.info(f"[Max] Got upload URL: {upload_url}")
        except MaxAPIError as e:
            logger.error(f"[Max] upload_file: Failed to get upload URL: {e}")
            return None

        try:
            connector = aiohttp.TCPConnector(family=socket.AF_INET)
            async with aiohttp.ClientSession(connector=connector) as session:
                form_data = aiohttp.FormData()
                form_data.add_field(
                    "data",
                    file_bytes,
                    filename=filename,
                    content_type=content_type,
                )
                logger.info(f"[Max] Sending file to upload URL, filename={filename}")
                async with session.post(upload_url, data=form_data) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.error(
                            f"[Max] Upload failed with status {resp.status}: {text}"
                        )
                        return None
                    result = await resp.json()

                    token = None
                    if file_type == "image" and "photos" in result:
                        photos = result["photos"]
                        for photo_info in photos.values():
                            if isinstance(photo_info, dict) and "token" in photo_info:
                                token = photo_info["token"]
                                break
                    else:
                        token = result.get("token")

                    if not token:
                        logger.error(f"[Max] Upload response missing token: {result}")
                    else:
                        logger.info(f"[Max] Upload successful, token={token}")
                    return token
        except Exception as e:
            logger.error(f"[Max] Exception during file upload: {e}")
            return None


async def get_me(token: str) -> Dict[str, Any]:
    return await MaxClient(token).get_me()


async def set_webhook(token: str, url: str) -> Dict[str, Any]:
    return await MaxClient(token).set_webhook(url)


async def delete_webhook(token: str) -> Dict[str, Any]:
    return await MaxClient(token).delete_webhook()


async def get_updates(
    token: str,
    marker: Optional[int] = None,
    timeout: int = 25,
    limit: int = 100,
) -> Dict[str, Any]:
    return await MaxClient(token).get_updates(
        marker=marker, timeout=timeout, limit=limit
    )
