import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


class AvitoAPIError(Exception):
    """Avito API error"""

    pass


class AvitoTokenExpiredError(AvitoAPIError):
    """Token expired error - requires refresh"""

    pass


class AvitoClient:

    BASE_URL = "https://api.avito.ru"
    MESSENGER_API = f"{BASE_URL}/messenger"
    AUTH_API = f"{BASE_URL}"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        token_expires_at: Optional[datetime] = None,
        on_token_refresh: Optional[callable] = None,
        user_id: Optional[int] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expires_at = token_expires_at
        self.on_token_refresh = on_token_refresh
        self._user_id = user_id

    async def _ensure_token_valid(self) -> None:
        if not self.access_token:
            if self.refresh_token:
                await self.refresh_access_token()
            else:
                await self.get_access_token()
            return

        if not self.token_expires_at:
            return

        now = datetime.utcnow()
        expires_soon = now >= self.token_expires_at - timedelta(minutes=5)

        if expires_soon:
            if self.refresh_token:
                await self.refresh_access_token()
            else:
                logger.warning(
                    "No refresh token available, obtaining new token via client_credentials"
                )
                await self.get_access_token()

    async def refresh_access_token(self) -> Dict[str, Any]:
        if not self.refresh_token:
            raise AvitoTokenExpiredError("No refresh token available")

        try:
            async with aiohttp.ClientSession() as session:
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": self.api_key,
                    "client_secret": self.api_secret,
                }

                async with session.post(
                    f"{self.AUTH_API}/token/",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status != 200:
                        raise AvitoTokenExpiredError(
                            f"Token refresh failed: HTTP {response.status}"
                        )

                    result = await response.json()

                    self.access_token = result.get("access_token")
                    self.refresh_token = result.get("refresh_token", self.refresh_token)
                    expires_in = result.get("expires_in", 3600)
                    self.token_expires_at = datetime.utcnow() + timedelta(
                        seconds=expires_in
                    )

                    if self.on_token_refresh:
                        await self.on_token_refresh(
                            {
                                "access_token": self.access_token,
                                "refresh_token": self.refresh_token,
                                "expires_at": self.token_expires_at.isoformat(),
                            }
                        )

                    return {
                        "access_token": self.access_token,
                        "refresh_token": self.refresh_token,
                        "expires_at": self.token_expires_at.isoformat(),
                    }

        except aiohttp.ClientError as e:
            raise AvitoTokenExpiredError(f"Token refresh request failed: {str(e)}")

    async def get_access_token(self) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                data = {
                    "grant_type": "client_credentials",
                    "client_id": self.api_key,
                    "client_secret": self.api_secret,
                    "scope": "messenger:read messenger:write",
                }

                async with session.post(
                    f"{self.AUTH_API}/token/",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    result = await response.json()

                    if "error" in result:
                        error_code = result.get("error")
                        error_description = result.get(
                            "error_description", "Unknown error"
                        )
                        error_text = (
                            f"Avito API error: {error_code} - {error_description}"
                        )
                        logger.error(f"Token request failed: {error_text}")
                        raise AvitoTokenExpiredError(error_text)

                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            f"Token request failed: HTTP {response.status}, {error_text}"
                        )
                        raise AvitoTokenExpiredError(
                            f"Token request failed: HTTP {response.status}"
                        )

                    access_token = result.get("access_token")
                    refresh_token = result.get("refresh_token")
                    expires_in = result.get("expires_in", 3600)

                    if not access_token:
                        error_text = "No access_token in Avito API response"
                        logger.error(error_text)
                        raise AvitoTokenExpiredError(error_text)

                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

                    self.access_token = access_token
                    self.refresh_token = refresh_token
                    self.token_expires_at = expires_at

                    if self.on_token_refresh:
                        await self.on_token_refresh(
                            {
                                "access_token": access_token,
                                "refresh_token": refresh_token,
                                "expires_at": expires_at.isoformat(),
                            }
                        )

                    return {
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "expires_at": expires_at.isoformat(),
                    }

        except aiohttp.ClientError as e:
            logger.error(f"Token request failed: {str(e)}")
            raise AvitoTokenExpiredError(f"Token request failed: {str(e)}")

    @staticmethod
    async def exchange_authorization_code_for_tokens(
        client_id: str,
        client_secret: str,
        authorization_code: str,
        redirect_uri: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                data = {
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "client_id": client_id,
                    "client_secret": client_secret,
                }

                if redirect_uri:
                    data["redirect_uri"] = redirect_uri

                async with session.post(
                    "https://api.avito.ru/token/",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            f"OAuth token exchange failed: HTTP {response.status}, {error_text}"
                        )
                        raise AvitoAPIError(
                            f"OAuth token exchange failed: HTTP {response.status}"
                        )

                    result = await response.json()

                    access_token = result.get("access_token")
                    refresh_token = result.get("refresh_token")
                    expires_in = result.get("expires_in", 3600)
                    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)

                    return {
                        "access_token": access_token,
                        "refresh_token": refresh_token,
                        "expires_at": expires_at.isoformat(),
                        "expires_in": expires_in,
                    }

        except aiohttp.ClientError as e:
            logger.error(f"OAuth token exchange request failed: {str(e)}")
            raise AvitoAPIError(f"OAuth token exchange request failed: {str(e)}")

    async def _get_user_id(self) -> int:
        if self._user_id:
            return self._user_id

        await self._ensure_token_valid()

        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                }
                async with session.get(
                    f"{self.BASE_URL}/core/v1/accounts/self",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status == 200:
                        user_data = await response.json()
                        user_id = user_data.get("id")
                        if user_id:
                            self._user_id = user_id
                            return user_id

                    error_text = await response.text()
                    logger.warning(
                        f"Failed to get user_id from profile: HTTP {response.status}, response: {error_text}"
                    )
                    raise AvitoAPIError(
                        f"user_id is required but could not be retrieved from profile. HTTP {response.status}: {error_text}"
                    )
        except Exception as e:
            logger.error(f"Failed to get user_id: {e}")
            raise AvitoAPIError(f"user_id is required but not set. Error: {str(e)}")

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        base_url: str = None,
    ) -> Dict[str, Any]:
        await self._ensure_token_valid()

        base = base_url or self.MESSENGER_API
        url = f"{base}{endpoint}"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(
                    method,
                    url,
                    json=data,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    content_type = response.headers.get("Content-Type", "").lower()

                    if "application/json" in content_type:
                        response_data = await response.json()
                    else:
                        response_text = await response.text()
                        logger.warning(
                            f"Avito API returned non-JSON response (Content-Type: {content_type}): {response_text[:500]}"
                        )

                        if response.status < 400:
                            try:
                                response_data = (
                                    json.loads(response_text) if response_text else {}
                                )
                            except:
                                response_data = {"raw_response": response_text}
                        else:
                            response_data = {
                                "message": response_text,
                                "raw_response": response_text,
                            }

                    if response.status == 401:
                        await self.refresh_access_token()
                        return await self._make_request(
                            method, endpoint, data, params, base_url
                        )

                    if response.status >= 400:
                        error_msg = response_data.get(
                            "message",
                            response_data.get(
                                "raw_response", f"HTTP {response.status}"
                            ),
                        )
                        logger.error(f"Avito API error {response.status}: {error_msg}")
                        raise AvitoAPIError(
                            f"Avito API error: {error_msg} (HTTP {response.status})"
                        )

                    return response_data

            except aiohttp.ClientError as e:
                logger.error(f"Avito API request failed: {str(e)}")
                raise AvitoAPIError(f"Request failed: {str(e)}")

    async def get_chats(
        self,
        limit: int = 50,
        offset: int = 0,
        chat_types: Optional[List[str]] = None,
        unread_only: bool = False,
        item_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        limit = min(limit, 100)
        user_id = await self._get_user_id()

        params = {"limit": limit, "offset": offset}
        if chat_types:
            params["chat_types"] = ",".join(chat_types)
        if unread_only:
            params["unread_only"] = "true"
        if item_ids:
            params["item_ids"] = ",".join(map(str, item_ids))

        response = await self._make_request(
            "GET", f"/v2/accounts/{user_id}/chats", params=params
        )

        if response is None:
            return []

        if not isinstance(response, dict):
            return []

        chats = response.get("chats", [])
        return chats

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        user_id = await self._get_user_id()
        response = await self._make_request(
            "GET", f"/v2/accounts/{user_id}/chats/{chat_id}"
        )
        return response

    async def get_messages(
        self, chat_id: str, limit: int = 50, offset: int = 0
    ) -> List[Dict[str, Any]]:
        limit = min(limit, 100)
        user_id = await self._get_user_id()
        params = {"offset": offset}
        response = await self._make_request(
            "GET",
            f"/v3/accounts/{user_id}/chats/{chat_id}/messages",
            params=params,
        )
        if isinstance(response, list):
            return response
        elif isinstance(response, dict) and "messages" in response:
            return response.get("messages", [])
        elif isinstance(response, dict):
            return []
        else:
            return []

    async def upload_image(
        self, image_data: bytes, filename: str = "image.jpg"
    ) -> Optional[tuple]:
        try:
            max_size = 24 * 1024 * 1024
            if len(image_data) > max_size:
                raise AvitoAPIError(
                    f"Image size ({len(image_data)} bytes) exceeds maximum allowed size (24 MB)"
                )

            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/uploadImages"
            url = f"{self.MESSENGER_API}{endpoint}"

            await self._ensure_token_valid()

            from io import BytesIO

            form_data = aiohttp.FormData()

            content_type = None
            if filename.lower().endswith(".png"):
                content_type = "image/png"
            elif filename.lower().endswith(".gif"):
                content_type = "image/gif"
            elif filename.lower().endswith(".webp"):
                content_type = "image/webp"
            else:
                content_type = "image/jpeg"

            file_obj = BytesIO(image_data)
            file_obj.seek(0)

            form_data.add_field(
                "uploadfile[]",
                value=file_obj,
                filename=filename,
                content_type=content_type,
            )

            headers = {
                "Authorization": f"Bearer {self.access_token}",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=form_data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status == 401:
                        await self.refresh_access_token()
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        async with session.post(
                            url,
                            data=form_data,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=30),
                        ) as retry_response:
                            response = retry_response

                    if response.status >= 400:
                        error_text = await response.text()
                        raise AvitoAPIError(
                            f"Failed to upload image: HTTP {response.status}, {error_text}"
                        )

                    response_text = await response.text()

                    try:
                        response_data = json.loads(response_text)
                    except json.JSONDecodeError:
                        response_data = response_text

                    if isinstance(response_data, dict):
                        image_id = (
                            list(response_data.keys())[0] if response_data else None
                        )
                        if image_id:
                            image_urls = response_data[image_id]
                            if isinstance(image_urls, dict):
                                image_url = (
                                    image_urls.get("1280x960")
                                    or image_urls.get("640x480")
                                    or (
                                        list(image_urls.values())[0]
                                        if image_urls
                                        else None
                                    )
                                )
                                return (image_id, image_url)
                            else:
                                return (image_id, None)

                    return None
        except Exception as e:
            raise AvitoAPIError(f"Failed to upload image: {str(e)}")

    async def send_message(
        self, chat_id: str, text: Optional[str] = None, image_id: Optional[str] = None
    ) -> Dict[str, Any]:
        if not text and not image_id:
            raise AvitoAPIError("Either text or image_id must be provided")

        user_id = await self._get_user_id()

        if image_id and text:
            image_payload = {"image_id": image_id}
            image_response = await self._make_request(
                "POST",
                f"/v1/accounts/{user_id}/chats/{chat_id}/messages/image",
                data=image_payload,
            )

            text_payload = {"message": {"text": text}, "type": "text"}
            text_response = await self._make_request(
                "POST",
                f"/v1/accounts/{user_id}/chats/{chat_id}/messages",
                data=text_payload,
            )

            return image_response
        elif image_id:
            payload = {"image_id": image_id}
            response = await self._make_request(
                "POST",
                f"/v1/accounts/{user_id}/chats/{chat_id}/messages/image",
                data=payload,
            )
        else:
            payload = {"message": {"text": text}, "type": "text"}
            response = await self._make_request(
                "POST", f"/v1/accounts/{user_id}/chats/{chat_id}/messages", data=payload
            )

        return response

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        try:
            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/chats/{chat_id}/messages/{message_id}"

            try:
                await self._make_request(
                    "POST", endpoint, base_url=self.MESSENGER_API, data={}
                )
                return True
            except AvitoAPIError as e:
                if "404" in str(e) or "405" in str(e):
                    logger.warning(
                        f"Primary delete endpoint failed, trying alternatives: {e}"
                    )
                    try:
                        endpoint_with_delete = f"{endpoint}/delete"
                        await self._make_request(
                            "POST",
                            endpoint_with_delete,
                            base_url=self.MESSENGER_API,
                            data={},
                        )
                        return True
                    except AvitoAPIError:
                        try:
                            await self._make_request(
                                "DELETE",
                                endpoint,
                                base_url=self.MESSENGER_API,
                            )
                            return True
                        except AvitoAPIError as e3:
                            logger.warning(f"All delete methods failed: {e3}")
                            raise e3
                else:
                    raise

        except AvitoAPIError as e:
            logger.warning(
                f"Failed to delete message {message_id} in chat {chat_id}: {e}"
            )
            return False

    async def mark_chat_as_read(self, chat_id: str) -> bool:
        try:
            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/chats/{chat_id}/read"
            response = await self._make_request(
                "POST",
                endpoint,
                base_url=self.MESSENGER_API,
                data={},
            )
            return isinstance(response, dict)
        except AvitoAPIError as e:
            logger.error(f"Failed to mark chat {chat_id} as read: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error marking chat {chat_id} as read: {e}")
            return False

    async def get_user_profile(self) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                }
                async with session.get(
                    f"{self.BASE_URL}/core/v1/accounts/self",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status == 200:
                        user_data = await response.json()
                        return user_data
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to get user profile: HTTP {response.status}, {error_text}"
                        )
                        raise AvitoAPIError(
                            f"Failed to get user profile: HTTP {response.status}"
                        )
        except aiohttp.ClientError as e:
            logger.error(f"Error getting user profile: {str(e)}")
            raise AvitoAPIError(f"Error getting user profile: {str(e)}")

    async def validate_token(self) -> bool:
        try:
            await self.get_chats(limit=1)
            return True
        except AvitoAPIError as e:
            logger.error(f"Token validation failed: {e}")
            return False

    async def check_status(self) -> Dict[str, Any]:
        """
        Проверяет статус аккаунта Avito и возвращает статус-код и информацию о подключении.
        Возвращает: {'status_code': int, 'connection_status': str, 'success': bool}
        """
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                }
                async with session.get(
                    f"{self.BASE_URL}/core/v1/accounts/self",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    status_code = response.status

                    if status_code == 200:
                        user_data = await response.json()
                        return {
                            "status_code": status_code,
                            "connection_status": "connected",
                            "success": True,
                        }
                    else:
                        error_text = await response.text()
                        logger.warning(
                            f"Status check failed: HTTP {status_code}, {error_text}"
                        )

                        if status_code == 401:
                            connection_status = "unauthorized"
                        elif status_code == 403:
                            connection_status = "forbidden"
                        elif status_code == 404:
                            connection_status = "not_found"
                        else:
                            connection_status = "error"

                        return {
                            "status_code": status_code,
                            "connection_status": connection_status,
                            "success": False,
                            "error": error_text,
                        }
        except aiohttp.ClientError as e:
            logger.error(f"Status check error: {str(e)}")
            return {
                "status_code": 0,
                "connection_status": "error",
                "success": False,
                "error": str(e),
            }
        except Exception as e:
            logger.error(f"Unexpected error during status check: {str(e)}")
            return {
                "status_code": 0,
                "connection_status": "error",
                "success": False,
                "error": str(e),
            }

    async def sync_messages(
        self, chat_id: str, since_timestamp: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        all_messages = []
        offset = 0
        limit = 100
        consecutive_old_batches = 0
        max_consecutive_old = 3

        while True:
            messages = await self.get_messages(chat_id, limit=limit, offset=offset)

            if not messages:
                break

            if since_timestamp:
                filtered = [
                    m for m in messages if m.get("created", 0) >= since_timestamp
                ]

                all_messages.extend(filtered)

                if len(filtered) == 0:
                    consecutive_old_batches += 1
                    if (
                        consecutive_old_batches >= max_consecutive_old
                        and len(messages) < limit
                    ):
                        break
                else:
                    consecutive_old_batches = 0
            else:
                all_messages.extend(messages)
                consecutive_old_batches = 0

            offset += limit

            if len(messages) < limit:
                break

        return all_messages

    async def register_webhook(self, webhook_url: str) -> Dict[str, Any]:
        try:
            payload = {"url": webhook_url}
            response = await self._make_request(
                "POST", "/v3/webhook", data=payload, base_url=self.MESSENGER_API
            )
            return response
        except Exception as e:
            logger.error(f"Error registering webhook: {e}")
            raise AvitoAPIError(f"Failed to register webhook: {str(e)}")

    async def get_webhooks(self) -> List[Dict[str, Any]]:
        try:
            endpoints_to_try = [
                "/v1/subscriptions",
            ]

            for endpoint in endpoints_to_try:
                try:
                    response = await self._make_request(
                        "POST", endpoint, base_url=self.MESSENGER_API
                    )
                    if isinstance(response, dict) and "subscriptions" in response:
                        subscriptions = response.get("subscriptions", [])
                        return subscriptions
                    elif isinstance(response, list):
                        return response
                    elif isinstance(response, dict) and "webhooks" in response:
                        webhooks = response.get("webhooks", [])
                        return webhooks
                    elif isinstance(response, dict):
                        logger.warning(
                            f"Unexpected response format from {endpoint}: {response}"
                        )
                        return [response]
                    logger.warning(f"Empty response from {endpoint}")
                    return []
                except AvitoAPIError as e:
                    error_str = str(e)
                    if "402" in error_str or "подписку" in error_str.lower():
                        logger.warning(
                            f"Subscription required (402) for getting webhooks from {endpoint}: {e}"
                        )
                    elif (
                        "404" not in error_str and "not found" not in error_str.lower()
                    ):
                        logger.warning(f"Error getting webhooks from {endpoint}: {e}")
                    continue

            logger.warning(
                "Could not get webhooks list - all endpoints returned 404 or error"
            )
            return []

        except Exception as e:
            logger.error(f"Error getting webhooks: {e}")
            return []

    async def get_voice_file_url(self, voice_id: str) -> Optional[str]:
        try:
            user_id = await self._get_user_id()
            endpoint = f"/v1/accounts/{user_id}/getVoiceFiles"

            params = {"voice_ids": [voice_id]}
            response = await self._make_request(
                "GET", endpoint, params=params, base_url=self.MESSENGER_API
            )

            voices_urls = response.get("voices_urls", {})
            voice_url = voices_urls.get(voice_id)

            if voice_url:
                voice_url = voice_url.replace("\u0026", "&")
                return voice_url

            return None
        except Exception as e:
            logger.warning(f"Failed to get voice file URL for voice_id {voice_id}: {e}")
            return None
