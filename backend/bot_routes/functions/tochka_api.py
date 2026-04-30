import aiohttp
from database.db import database, tochka_bank_accounts, tochka_bank_credentials
from sqlalchemy import select


class TochkaBankError(Exception):
    def __init__(self, code: str, message: str, error_id: str, errors: list = []):
        self.code = code
        self.message = message
        self.error_id = error_id
        self.errors = errors
        super().__init__(self.message)


async def get_access_token(tochka_bank_accounts_id: int):
    query = (
        select([tochka_bank_credentials])
        .select_from(tochka_bank_credentials)
        .join(
            tochka_bank_accounts,
            tochka_bank_accounts.c.tochka_bank_credential_id
            == tochka_bank_credentials.c.id,
        )
        .where(tochka_bank_accounts.c.id == tochka_bank_accounts_id)
    )
    return await database.fetch_one(query)


async def refresh_token(integration_cashboxes: int) -> dict:

    url = "http://localhost/api/v1/bank/refresh_token"

    headers = {"Content-Type": "application/json"}
    payload = {
        "integration_cashboxes": integration_cashboxes,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            return await response.json()


async def send_payment_to_tochka(
    account_code: str,
    bank_code: str,
    counterparty_bank_bic: str,
    counterparty_account_number: str,
    payment_amount: float,
    payment_date: str,
    counterparty_name: str,
    payment_purpose: str,
    auth: str,
) -> dict:
    # url = 'https://enter.tochka.com/sandbox/v2/payment/v1.0/for-sign'
    url = "https://enter.tochka.com/uapi/payment/v1.0/for-sign"

    headers = {"Authorization": f"Bearer {auth}", "Content-Type": "application/json"}

    payload = {
        "Data": {
            "accountCode": account_code,
            "bankCode": bank_code,
            "paymentAmount": payment_amount,
            "paymentDate": payment_date,
            "counterpartyBankBic": counterparty_bank_bic,
            "counterpartyAccountNumber": counterparty_account_number,
            "counterpartyName": counterparty_name,
            "paymentPurpose": payment_purpose,
        }
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, headers=headers) as response:
            response_data = await response.json()

            if response.status == 200:
                return {
                    "success": True,
                    "request_id": response_data["Data"]["requestId"],
                    "status_code": 200,
                }

            error_code = response_data.get("code")
            error_message = response_data.get("message")
            error_id = response_data.get("id")

            error_mapping = {
                "400": "Bad Request - Invalid input parameters",
                "401": "Unauthorized - Invalid or expired token",
                "403": "Forbidden - Insufficient permissions",
                "404": "Not Found - Resource not found",
                "500": "Internal Server Error",
            }

            raise TochkaBankError(
                code=error_code,
                message=error_mapping.get(error_code, error_message),
                error_id=error_id,
                errors=response_data.get("Errors"),
            )
