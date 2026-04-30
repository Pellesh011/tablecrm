import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

TINKOFF_TERMINAL_KEY = "1726726365845"
TINKOFF_PASSWORD = "k2Wj*bsl24&%n7AB"
TINKOFF_IS_TEST = True


class TinkoffApiService:

    def __init__(self):
        self.terminal_key = TINKOFF_TERMINAL_KEY
        self.password = TINKOFF_PASSWORD
        self.is_test = TINKOFF_IS_TEST
        self.base_url = "https://securepay.tinkoff.ru/v2/"

    def _generate_token(self, data: Dict[str, Any]) -> str:
        """Генерирует токен для запроса к API Tinkoff по алгоритму из документации."""
        root_params = {
            key: value
            for key, value in data.items()
            if not isinstance(value, (dict, list))
        }
        root_params["Password"] = self.password
        sorted_params = dict(sorted(root_params.items()))
        values_str = "".join(str(value) for value in sorted_params.values())
        token = hashlib.sha256(values_str.encode("utf-8")).hexdigest()
        return token

    async def init_payment(
        self,
        amount: int,
        order_id: str,
        description: str,
        return_url: str,
        data: Optional[Dict[str, str]] = None,
        receipt: Optional[Dict[str, Any]] = None,
        fail_url: Optional[str] = None,
        notification_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
            raise ValueError(f"Amount must be a positive number, got {amount}")

        request_data = {
            "TerminalKey": self.terminal_key,
            "Amount": amount,
            "OrderId": order_id,
            "Description": description,
        }

        if not self.is_test:
            try:
                tomorrow = datetime.now() + timedelta(hours=24)
                request_data["RedirectDueDate"] = tomorrow.strftime(
                    "%Y-%m-%dT%H:%M:%S+03:00"
                )
            except Exception:
                pass

        from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

        parsed = urlparse(return_url)
        base_path = parsed.path.rstrip("/")
        query_params = parsed.query

        use_as_is = False
        if "use_as_is=true" in return_url.lower():
            use_as_is = True
            if query_params:
                params = parse_qs(query_params)
                params.pop("use_as_is", None)
                query_params = urlencode(params, doseq=True)

        if use_as_is:
            success_url = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    base_path,
                    parsed.params,
                    query_params,
                    parsed.fragment,
                )
            )
            fail_url = success_url
        else:
            success_path = f"{base_path}/success" if base_path else "/success"
            fail_path = f"{base_path}/fail" if base_path else "/fail"

            success_url = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    success_path,
                    parsed.params,
                    query_params,
                    parsed.fragment,
                )
            )
            fail_url = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    fail_path,
                    parsed.params,
                    query_params,
                    parsed.fragment,
                )
            )

        if not notification_url:
            domain = f"{parsed.scheme}://{parsed.netloc}"
            notification_url = f"{domain}/api/v1/payments/tinkoff/callback"

        request_data["SuccessURL"] = success_url
        request_data["FailURL"] = fail_url
        request_data["NotificationURL"] = notification_url

        token = self._generate_token(request_data)
        request_data["Token"] = token

        if data:
            request_data["DATA"] = data
        if receipt:
            request_data["Receipt"] = receipt

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }

                response = await client.post(
                    f"{self.base_url}Init", json=request_data, headers=headers
                )

                if response.status_code != 200:
                    return {
                        "Success": False,
                        "ErrorCode": str(response.status_code),
                        "Message": "HTTP error from Tinkoff API",
                        "Details": response.text,
                    }

                if not response.text:
                    return {
                        "Success": False,
                        "ErrorCode": "9999",
                        "Message": "Empty response from Tinkoff API",
                    }

                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    return {
                        "Success": False,
                        "ErrorCode": "9998",
                        "Message": "Invalid JSON response from Tinkoff API",
                        "Details": str(e),
                    }

        except Exception as e:
            return {
                "Success": False,
                "ErrorCode": "9997",
                "Message": f"Error making request to Tinkoff API: {str(e)}",
            }

    async def get_payment_state(self, payment_id: str) -> Dict[str, Any]:
        request_data = {"PaymentId": payment_id}
        return await self._make_request("GetState", request_data)

    async def confirm_payment(
        self, payment_id: str, amount: Optional[int] = None
    ) -> Dict[str, Any]:
        request_data = {"PaymentId": payment_id}
        if amount is not None:
            request_data["Amount"] = amount
        return await self._make_request("Confirm", request_data)

    async def cancel_payment(
        self, payment_id: str, amount: Optional[int] = None
    ) -> Dict[str, Any]:
        request_data = {"PaymentId": payment_id}
        if amount is not None:
            request_data["Amount"] = amount
        return await self._make_request("Cancel", request_data)

    async def _make_request(
        self, method: str, request_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        request_data["TerminalKey"] = self.terminal_key
        token = self._generate_token(request_data)
        request_data["Token"] = token

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }

                response = await client.post(
                    f"{self.base_url}{method}", json=request_data, headers=headers
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    return {
                        "Success": False,
                        "ErrorCode": str(response.status_code),
                        "Message": "HTTP error from Tinkoff API",
                        "Details": response.text,
                    }
        except Exception as e:
            return {
                "Success": False,
                "ErrorCode": "9997",
                "Message": f"Error making request to Tinkoff API: {str(e)}",
            }

    def create_receipt(
        self,
        email: str = "",
        phone: str = "",
        items: Optional[List[Dict[str, Any]]] = None,
        taxation: str = "usn_income",
        ffd_version: str = "1.2",
        customer: Optional[str] = None,
    ) -> Dict[str, Any]:
        if items is None:
            items = []

        formatted_items = []
        for item in items:
            if "name" not in item:
                raise ValueError("Item must have 'name' field")

            price = int(item.get("price", 0) * 100)  # Конвертируем в копейки
            quantity = item.get("quantity", 1)
            if "amount" in item:
                amount = int(item["amount"] * 100)
            else:
                amount = price * quantity

            formatted_item = {
                "Name": item["name"],
                "Price": price,
                "Quantity": quantity,
                "Amount": amount,
                "Tax": item.get("tax", "none"),
                "PaymentMethod": item.get("payment_method", "full_prepayment"),
                "PaymentObject": item.get("payment_object", "service"),
                "MeasurementUnit": item.get("measurement_unit", "шт"),
            }
            formatted_items.append(formatted_item)

        receipt = {
            "EmailCompany": email if email else "noreply@tablecrm.com",
            "Taxation": taxation,
            "FfdVersion": ffd_version,
            "Items": formatted_items,
        }

        if phone:
            receipt["Phone"] = phone

        if customer:
            receipt["Customer"] = customer

        return receipt

    def map_tinkoff_status_to_internal(self, tinkoff_status: str) -> str:
        status_mapping = {
            "NEW": "pending",
            "FORM_SHOWED": "pending",
            "AUTHORIZED": "pending",
            "CONFIRMED": "success",
            "REJECTED": "failed",
            "CANCELED": "canceled",
            "REFUNDED": "refunded",
            "PARTIAL_REFUNDED": "refunded",
            "3DS_CHECKING": "pending",
            "3DS_CHECKED": "pending",
        }
        return status_mapping.get(tinkoff_status, "pending")
