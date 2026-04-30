import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
from apps.cdek import schemas
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class CdekClient:
    BASE_URL = "https://api.cdek.ru/v2"  # прод - https://api.cdek.ru/v2 / https://api.edu.cdek.ru/v2 - тестовый

    def __init__(self, account: str, secure_password: str):
        self.account = account
        self.secure_password = secure_password
        self._access_token = None
        self._token_expires = None

    async def _get_access_token(self) -> str:
        if (
            self._access_token
            and self._token_expires
            and datetime.utcnow() < self._token_expires
        ):
            return self._access_token

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.account,
                    "client_secret": self.secure_password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Failed to get token: {resp.status} {text}")
                data = await resp.json()
                self._access_token = data["access_token"]
                expires_in = data.get("expires_in", 300)
                self._token_expires = datetime.utcnow() + timedelta(
                    seconds=expires_in - 60
                )
                return self._access_token

    async def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        token = await self._get_access_token()
        url = f"{self.BASE_URL}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, **kwargs) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    raise Exception(f"CDEK API error {resp.status}: {text}")
                return await resp.json()

    async def get_available_tariffs(self) -> List[int]:
        result = await self._request("GET", "/calculator/alltariffs")
        return result.get("tariff_codes", [])

    async def calculate_tariff(
        self,
        from_location: dict,
        to_location: dict,
        packages: list,
        tariff_code: Optional[int] = None,
        services: Optional[List[dict]] = None,
        **kwargs,
    ) -> Dict:
        if tariff_code:
            path = "/calculator/tariff"
            payload = {
                "from_location": from_location,
                "to_location": to_location,
                "packages": packages,
                "tariff_code": tariff_code,
                **kwargs,
            }
            if services:
                payload["services"] = services
        else:
            path = "/calculator/tarifflist"
            payload = {
                "from_location": from_location,
                "to_location": to_location,
                "packages": packages,
                **kwargs,
            }
        return await self._request("POST", path, json=payload)

    async def create_order(self, order_data: dict) -> Dict:
        return await self._request("POST", "/orders", json=order_data)

    async def get_order_by_uuid(self, order_uuid: str) -> Dict:
        return await self._request("GET", f"/orders/{order_uuid}")

    async def get_order_by_number(
        self, cdek_number: Optional[str] = None, im_number: Optional[str] = None
    ) -> Dict:
        params = {}
        if cdek_number:
            params["cdek_number"] = cdek_number
        if im_number:
            params["im_number"] = im_number
        if not params:
            raise ValueError("Provide at least one of cdek_number or im_number")
        return await self._request("GET", "/orders", params=params)

    async def delete_order(self, order_uuid: str) -> Dict:
        return await self._request("DELETE", f"/orders/{order_uuid}")

    async def get_delivery_points(self, **filters) -> List[Dict]:
        params = {k: v for k, v in filters.items() if v is not None}
        return await self._request("GET", "/deliverypoints", params=params)

    async def get_token_response(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.BASE_URL}/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.account,
                    "client_secret": self.secure_password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"Failed to get token: {resp.status} {text}")
                return await resp.json()

    async def calculate_tariff_and_service(
        self,
        from_location: dict,
        to_location: dict,
        packages: list,
        services: list = None,
        **kwargs,
    ) -> Dict:
        payload = {
            "from_location": from_location,
            "to_location": to_location,
            "packages": packages,
            "services": services or [],
            **kwargs,
        }
        return await self._request("POST", "/calculator/tariffAndService", json=payload)

    async def suggest_cities(self, name: str, country_code: str = "RU") -> List[Dict]:

        params = {"name": name, "country_code": country_code}
        return await self._request("GET", "/location/suggest/cities", params=params)

    async def print_waybill(self, data: dict) -> Dict:
        return await self._request("POST", "/print/orders", json=data)

    async def get_waybill_info(self, uuid: str) -> Dict:
        return await self._request("GET", f"/print/orders/{uuid}")

    async def download_waybill(self, uuid: str) -> bytes:
        token = await self._get_access_token()
        url = f"{self.BASE_URL}/print/orders/{uuid}.pdf"
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    raise Exception(f"CDEK API error {resp.status}: {text}")
                return await resp.read()

    async def print_barcode(self, data: dict) -> Dict:
        return await self._request("POST", "/print/barcodes", json=data)

    async def get_barcode_info(self, uuid: str) -> Dict:
        return await self._request("GET", f"/print/barcodes/{uuid}")

    async def download_barcode(self, uuid: str) -> bytes:
        token = await self._get_access_token()
        url = f"{self.BASE_URL}/print/barcodes/{uuid}.pdf"
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    raise Exception(f"CDEK API error {resp.status}: {text}")
                return await resp.read()

    async def _get_filtered_tariffs(
        self,
        items: List[schemas.ClientOrderItem],
        delivery_type: str,
        address: Optional[str],
        delivery_point: Optional[str],
        recipient_city_code: Optional[int],
        template: dict,
    ) -> List[dict]:
        sender_city_data = template.get("sender_city_data")
        if not sender_city_data or not sender_city_data.get("code"):
            raise HTTPException(400, "Sender city data missing in template")

        pickup_mode = template.get("pickup_mode", "door")

        total_weight = sum(item.weight * item.amount for item in items) or 1000
        packages = [
            {
                "weight": total_weight,
                "length": 23,
                "width": 19,
                "height": 10,
            }
        ]

        calc_params = {
            "packages": packages,
            "type": 1,
        }

        calc_params["from_location"] = {
            "code": sender_city_data["code"],
            "city": sender_city_data.get("name"),
        }
        if pickup_mode == "door" and template.get("pickup_address"):
            calc_params["from_location"]["address"] = template["pickup_address"]

        if delivery_type == "courier":
            if not recipient_city_code:
                raise HTTPException(
                    400, "Recipient city code required for courier delivery"
                )
            calc_params["to_location"] = {
                "code": recipient_city_code,
            }
            if address:
                calc_params["to_location"]["address"] = address
        else:
            if not recipient_city_code:
                raise HTTPException(
                    400, "Recipient city code required for PVZ/postamat"
                )
            calc_params["to_location"] = {
                "code": recipient_city_code,
            }

        try:
            result = await self.calculate_tariff(**calc_params)
        except Exception as e:
            logger.exception("CDEK calculate error")
            raise HTTPException(500, f"Calculation failed: {str(e)}")

        if result.get("errors"):
            return []

        all_tariffs = result.get("tariff_codes", [])

        sender_mode = "дверь" if pickup_mode == "door" else "склад"
        receiver_mode = "дверь" if delivery_type == "courier" else "склад"
        target_pattern = f"{sender_mode}-{receiver_mode}"

        filtered = [
            t for t in all_tariffs if target_pattern in t.get("tariff_name", "").lower()
        ]
        return filtered
