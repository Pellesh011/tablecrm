import json
import logging
import os
from typing import Any, Dict

import aiohttp
from aiohttp import ClientError, ClientResponseError

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AiTunnelService:

    def __init__(self):

        self.api_key = os.getenv("AITUNNEL_TOKEN")
        self.base_url = "https://api.aitunnel.ru/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    async def sendBase64File(
        self,
        file_name: str,
        data_url: str,
        prompt_str: str,
        model: str = "gpt-4o",
        timeout: int = 60,
    ) -> Dict[str, Any]:

        if not file_name or not isinstance(file_name, str):
            raise ValueError("Имя файла должно быть непустой строкой.")
        if not data_url or not isinstance(data_url, str):
            raise ValueError("Data URL должен быть непустой строкой.")
        if not prompt_str or not isinstance(prompt_str, str):
            raise ValueError("Промпт должен быть непустой строкой.")

        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt_str},
                    {
                        "type": "file",
                        "file": {"filename": file_name, "file_data": data_url},
                    },
                ],
            }
        ]

        payload = {"model": model, "messages": messages}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
                    logger.info("Успешный запрос к API AiTunnel.")
                    return result

        except ClientResponseError as e:
            logger.error(f"Ошибка HTTP: {e.status} - {e.message}")
            raise
        except ClientError as e:
            logger.error(f"Ошибка клиента: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON: {e}")
            raise ValueError("Некорректный формат ответа от сервера.")
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e}")
            raise
