import asyncio
import re
from datetime import datetime, timedelta

import aiohttp
from common.decorators import ensure_db_connection
from database.db import (
    contragents,
    database,
    docs_sales,
    integrations,
    payments,
    pboxes,
    tochka_bank_accounts,
    tochka_bank_credentials,
    tochka_bank_payments,
    users_cboxes_relation,
)
from functions.helpers import get_statement, init_statement
from sqlalchemy import and_, select


async def refresh_token(cred_id: int):
    integration = await database.fetch_one(
        integrations.select().where(integrations.c.id == 1)
    )
    credentials = await database.fetch_one(
        tochka_bank_credentials.select().where(tochka_bank_credentials.c.id == cred_id)
    )
    async with aiohttp.ClientSession(trust_env=True) as session:
        async with session.post(
            "https://enter.tochka.com/connect/token",
            data={
                "client_id": integration.get("client_app_id"),
                "client_secret": integration.get("client_secret"),
                "grant_type": "refresh_token",
                "refresh_token": credentials.get("refresh_token"),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            token_json = await resp.json()
            print(token_json)
            if token_json.get("access_token") and token_json.get("refresh_token"):
                await database.execute(
                    tochka_bank_credentials.update()
                    .where(tochka_bank_credentials.c.id == cred_id)
                    .values(
                        {
                            "access_token": token_json.get("access_token"),
                            "refresh_token": token_json.get("refresh_token"),
                        }
                    )
                )


async def extract_number(text):
    match = re.search(r"[№#]\s*(\d+)", text)
    if match:
        return int(match.group(1))
    return None


async def process_payment(contragent_id, description, amount, cashbox_id):
    if description:
        number_document = await extract_number(description)
        if number_document:
            query = docs_sales.select().where(
                and_(
                    docs_sales.c.number == str(number_document),
                    docs_sales.c.cashbox == cashbox_id,
                    docs_sales.c.contragent == contragent_id,
                    docs_sales.c.is_deleted == False,
                )
            )
            docs_sales_info = await database.fetch_one(query)
            if docs_sales_info:
                query = (
                    payments.update()
                    .where(payments.c.docs_sales_id == docs_sales_info.id)
                    .values({"status": True})
                )
                await database.execute(query)

                query = payments.select().where(
                    payments.c.docs_sales_id == docs_sales_info.id
                )
                payment_id = await database.fetch_one(query)

                if payment_id:

                    return True, payment_id.id

    query = docs_sales.select().where(
        and_(
            docs_sales.c.sum == float(amount),
            docs_sales.c.cashbox == cashbox_id,
            docs_sales.c.contragent == contragent_id,
            docs_sales.c.is_deleted == False,
        )
    )
    docs_sales_info = await database.fetch_one(query)
    if docs_sales_info:
        query = (
            payments.update()
            .where(payments.c.docs_sales_id == docs_sales_info.id)
            .values({"status": True})
        )
        await database.execute(query)

        query = payments.select().where(payments.c.docs_sales_id == docs_sales_info.id)
        payment_id = await database.fetch_one(query)

        if payment_id:
            return True, payment_id.id

    return False, 0


@ensure_db_connection
async def tochka_update_transaction():
    print("START TOCHKABANK")

    @database.transaction()
    async def _tochka_update():
        active_accounts_with_credentials = await database.fetch_all(
            select(
                tochka_bank_accounts.c.accountId,
                tochka_bank_accounts.c.registrationDate,
                tochka_bank_credentials.c.access_token,
                tochka_bank_credentials.c.id.label("cred_id"),
                pboxes.c.id.label("pbox_id"),
                users_cboxes_relation.c.token,
                pboxes.c.cashbox.label("cashbox_id"),
            )
            .where(
                and_(
                    tochka_bank_accounts.c.is_active == True,
                    tochka_bank_accounts.c.is_deleted == False,
                    pboxes.c.deleted_at.is_(None),
                )
            )
            .select_from(tochka_bank_accounts)
            .join(
                tochka_bank_credentials,
                tochka_bank_credentials.c.id
                == tochka_bank_accounts.c.tochka_bank_credential_id,
            )
            .join(pboxes, pboxes.c.id == tochka_bank_accounts.c.payboxes_id)
            .join(users_cboxes_relation, users_cboxes_relation.c.id == pboxes.c.cashbox)
        )
        if active_accounts_with_credentials:
            for account in active_accounts_with_credentials:
                async with aiohttp.ClientSession(trust_env=True) as session:
                    async with session.get(
                        f'https://enter.tochka.com/uapi/open-banking/v1.0/accounts/{account.get("accountId")}/balances',
                        headers={
                            "Authorization": f'Bearer {account.get("access_token")}',
                            "Content-type": "application/json",
                        },
                    ) as resp:
                        balance_json = await resp.json()
                    await session.close()

                if not balance_json.get("Data"):

                    await refresh_token(cred_id=account.get("cred_id"))

                    print(
                        "проблема с получением баланса (вероятно некорректный access_token), был произведён рефреш"
                    )

                    continue

                await database.execute(
                    pboxes.update()
                    .where(pboxes.c.id == account.get("pbox_id"))
                    .values(
                        {
                            "balance": balance_json.get("Data")
                            .get("Balance")[0]
                            .get("Amount")
                            .get("amount"),
                            "updated_at": int(datetime.utcnow().timestamp()),
                            "balance_date": int(datetime.utcnow().timestamp()),
                        }
                    )
                )

                statement = await init_statement(
                    {
                        "accountId": account.get("accountId"),
                        "startDateTime": account.get("registrationDate"),
                        "endDateTime": str(datetime.now().date() + timedelta(days=1)),
                    },
                    account.get("access_token"),
                )
                status_info = ""
                info_statement = None
                while status_info != "Ready":
                    await asyncio.sleep(2)
                    info_statement = await get_statement(
                        statement.get("Data")["Statement"].get("statementId"),
                        statement.get("Data")["Statement"].get("accountId"),
                        account.get("access_token"),
                    )
                    status_info = info_statement.get("Data")["Statement"][0].get(
                        "status"
                    )
                tochka_payments_db = await database.fetch_all(
                    select(*payments.columns, tochka_bank_payments.c.payment_id)
                    .where(
                        and_(
                            payments.c.paybox == account.get("pbox_id"),
                            payments.c.cashbox == account.get("cashbox_id"),
                        )
                    )
                    .select_from(payments)
                    .join(
                        tochka_bank_payments,
                        tochka_bank_payments.c.payment_crm_id == payments.c.id,
                    )
                )

                if len(tochka_payments_db) < 1:
                    for payment in info_statement.get("Data")["Statement"][0][
                        "Transaction"
                    ]:

                        if payment.get("CreditorParty"):
                            category_payment = "CreditorParty"
                        else:
                            category_payment = "DebtorParty"

                        contragent_db = await database.fetch_one(
                            contragents.select().where(
                                and_(
                                    contragents.c.inn
                                    == payment.get(category_payment).get("inn")
                                ),
                                contragents.c.cashbox == account.get("cashbox_id"),
                            )
                        )

                        result_process = False

                        if contragent_db:
                            contragent_id = contragent_db.get("id")
                            if payment.get("DebtorParty"):
                                result_process, payment_create_id = (
                                    await process_payment(
                                        contragent_id=contragent_db.id,
                                        description=payment.get("description"),
                                        amount=payment.get("Amount").get("amount"),
                                        cashbox_id=account.cashbox_id,
                                    )
                                )
                        else:
                            contragent_db = await database.execute(
                                contragents.insert().values(
                                    {
                                        "name": payment.get(category_payment).get(
                                            "name"
                                        ),
                                        "inn": payment.get(category_payment).get("inn"),
                                        "cashbox": account.get("cashbox_id"),
                                        "is_deleted": False,
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                    }
                                )
                            )
                            contragent_id = contragent_db

                        if not result_process:

                            payment_create_id = await database.execute(
                                payments.insert().values(
                                    {
                                        "name": payment.get("transactionTypeCode"),
                                        "description": payment.get("description"),
                                        "type": (
                                            "outgoing"
                                            if payment.get("creditDebitIndicator")
                                            == "Debit"
                                            else "incoming"
                                        ),
                                        "tags": f"TochkaBank,{account.get('accountId')}",
                                        "amount": payment.get("Amount").get("amount"),
                                        "cashbox": account.get("cashbox_id"),
                                        "paybox": account.get("pbox_id"),
                                        "date": datetime.strptime(
                                            payment.get("documentProcessDate"),
                                            "%Y-%m-%d",
                                        ).timestamp(),
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "is_deleted": False,
                                        "amount_without_tax": payment.get("Amount").get(
                                            "amount"
                                        ),
                                        "status": (
                                            True
                                            if payment.get("status") == "Booked"
                                            else False
                                        ),
                                        "stopped": True,
                                        "contragent": contragent_id,
                                    }
                                )
                            )

                        payment_data = {
                            "accountId": info_statement.get("Data")["Statement"][0].get(
                                "accountId"
                            ),
                            "payment_crm_id": payment_create_id,
                            "statementId": info_statement.get("Data")["Statement"][
                                0
                            ].get("statementId"),
                            "statement_creation_datetime": info_statement.get("Data")[
                                "Statement"
                            ][0].get("creationDateTime"),
                            "transactionTypeCode": payment.get("transactionTypeCode"),
                            "transactionId": payment.get("transactionId"),
                            "status": payment.get("status"),
                            "payment_id": payment.get("paymentId"),
                            "documentProcessDate": payment.get("documentProcessDate"),
                            "documentNumber": payment.get("documentNumber"),
                            "description": payment.get("description"),
                            "creditDebitIndicator": payment.get("creditDebitIndicator"),
                            "amount": (
                                payment.get("Amount").get("amount")
                                if payment.get("Amount")
                                else None
                            ),
                            "amountNat": (
                                payment.get("Amount").get("amountNat")
                                if payment.get("Amount")
                                else None
                            ),
                            "currency": (
                                payment.get("Amount").get("currency")
                                if payment.get("Amount")
                                else None
                            ),
                        }
                        if payment.get("CreditorParty"):
                            payment_data.update(
                                {
                                    "creditor_party_inn": payment.get(
                                        "CreditorParty"
                                    ).get("inn"),
                                    "creditor_party_name": payment.get(
                                        "CreditorParty"
                                    ).get("name"),
                                    "creditor_party_kpp": payment.get(
                                        "CreditorParty"
                                    ).get("kpp"),
                                    "creditor_account_identification": payment.get(
                                        "CreditorAccount"
                                    ).get("identification"),
                                    "creditor_account_schemeName": payment.get(
                                        "CreditorAccount"
                                    ).get("schemeName"),
                                    "creditor_agent_schemeName": payment.get(
                                        "CreditorAgent"
                                    ).get("schemeName"),
                                    "creditor_agent_name": payment.get(
                                        "CreditorAgent"
                                    ).get("name"),
                                    "creditor_agent_identification": payment.get(
                                        "CreditorAgent"
                                    ).get("identification"),
                                    "creditor_agent_accountIdentification": payment.get(
                                        "CreditorAgent"
                                    ).get("accountIdentification"),
                                }
                            )
                        elif payment.get("DebtorParty"):
                            payment_data.update(
                                {
                                    "debitor_party_inn": payment.get("DebtorParty").get(
                                        "inn"
                                    ),
                                    "debitor_party_name": payment.get(
                                        "DebtorParty"
                                    ).get("name"),
                                    "debitor_party_kpp": payment.get("DebtorParty").get(
                                        "kpp"
                                    ),
                                    "debitor_account_identification": payment.get(
                                        "DebtorAccount"
                                    ).get("identification"),
                                    "debitor_account_schemeName": payment.get(
                                        "DebtorAccount"
                                    ).get("schemeName"),
                                    "debitor_agent_schemeName": payment.get(
                                        "DebtorAgent"
                                    ).get("schemeName"),
                                    "debitor_agent_name": payment.get(
                                        "DebtorAgent"
                                    ).get("name"),
                                    "debitor_agent_identification": payment.get(
                                        "DebtorAgent"
                                    ).get("identification"),
                                    "debitor_agent_accountIdentification": payment.get(
                                        "DebtorAgent"
                                    ).get("accountIdentification"),
                                }
                            ),
                        else:
                            raise Exception(
                                "не вилидный формат транзакции от Точка банка"
                            )

                        await database.execute(
                            tochka_bank_payments.insert().values(payment_data)
                        )

                else:
                    set_tochka_payments_statement = set(
                        [
                            item.get("paymentId")
                            for item in info_statement.get("Data")["Statement"][0][
                                "Transaction"
                            ]
                        ]
                    )
                    set_tochka_payments_db = set(
                        [item.get("payment_id") for item in tochka_payments_db]
                    )
                    new_paymentsId = list(
                        set_tochka_payments_statement - set_tochka_payments_db
                    )
                    for payment in [
                        item
                        for item in info_statement.get("Data")["Statement"][0][
                            "Transaction"
                        ]
                        if item.get("paymentId") in new_paymentsId
                    ]:
                        if payment.get("CreditorParty"):
                            category_payment = "CreditorParty"
                        else:
                            category_payment = "DebtorParty"

                        contragent_db = await database.fetch_one(
                            contragents.select().where(
                                and_(
                                    contragents.c.inn
                                    == payment.get(category_payment).get("inn")
                                ),
                                contragents.c.cashbox == account.get("cashbox_id"),
                            )
                        )

                        result_process = False

                        if contragent_db:
                            contragent_id = contragent_db.get("id")
                            if payment.get("DebtorParty"):
                                result_process, payment_create_id = (
                                    await process_payment(
                                        contragent_id=contragent_db.id,
                                        description=payment.get("description"),
                                        amount=payment.get("Amount").get("amount"),
                                        cashbox_id=account.cashbox_id,
                                    )
                                )
                        else:
                            contragent_db = await database.execute(
                                contragents.insert().values(
                                    {
                                        "name": payment.get(category_payment).get(
                                            "name"
                                        ),
                                        "inn": payment.get(category_payment).get("inn"),
                                        "cashbox": account.get("cashbox_id"),
                                        "is_deleted": False,
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                    }
                                )
                            )
                            contragent_id = contragent_db

                        if not result_process:

                            payment_create_id = await database.execute(
                                payments.insert().values(
                                    {
                                        "name": payment.get("transactionTypeCode"),
                                        "description": payment.get("description"),
                                        "type": (
                                            "outgoing"
                                            if payment.get("creditDebitIndicator")
                                            == "Debit"
                                            else "incoming"
                                        ),
                                        "tags": f"TochkaBank,{account.get('accountId')}",
                                        "amount": payment.get("Amount").get("amount"),
                                        "cashbox": account.get("cashbox_id"),
                                        "paybox": account.get("pbox_id"),
                                        "date": datetime.strptime(
                                            payment.get("documentProcessDate"),
                                            "%Y-%m-%d",
                                        ).timestamp(),
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "is_deleted": False,
                                        "amount_without_tax": payment.get("Amount").get(
                                            "amount"
                                        ),
                                        "status": (
                                            True
                                            if payment.get("status") == "Booked"
                                            else False
                                        ),
                                        "stopped": True,
                                        "contragent": contragent_id,
                                    }
                                )
                            )

                        payment_data = {
                            "accountId": info_statement.get("Data")["Statement"][0].get(
                                "accountId"
                            ),
                            "payment_crm_id": payment_create_id,
                            "statementId": info_statement.get("Data")["Statement"][
                                0
                            ].get("statementId"),
                            "statement_creation_datetime": info_statement.get("Data")[
                                "Statement"
                            ][0].get("creationDateTime"),
                            "transactionTypeCode": payment.get("transactionTypeCode"),
                            "transactionId": payment.get("transactionId"),
                            "status": payment.get("status"),
                            "payment_id": payment.get("paymentId"),
                            "documentProcessDate": payment.get("documentProcessDate"),
                            "documentNumber": payment.get("documentNumber"),
                            "description": payment.get("description"),
                            "creditDebitIndicator": payment.get("creditDebitIndicator"),
                            "amount": (
                                payment.get("Amount").get("amount")
                                if payment.get("Amount")
                                else None
                            ),
                            "amountNat": (
                                payment.get("Amount").get("amountNat")
                                if payment.get("Amount")
                                else None
                            ),
                            "currency": (
                                payment.get("Amount").get("currency")
                                if payment.get("Amount")
                                else None
                            ),
                        }
                        if payment.get("CreditorParty"):
                            payment_data.update(
                                {
                                    "creditor_party_inn": payment.get(
                                        "CreditorParty"
                                    ).get("inn"),
                                    "creditor_party_name": payment.get(
                                        "CreditorParty"
                                    ).get("name"),
                                    "creditor_party_kpp": payment.get(
                                        "CreditorParty"
                                    ).get("kpp"),
                                    "creditor_account_identification": payment.get(
                                        "CreditorAccount"
                                    ).get("identification"),
                                    "creditor_account_schemeName": payment.get(
                                        "CreditorAccount"
                                    ).get("schemeName"),
                                    "creditor_agent_schemeName": payment.get(
                                        "CreditorAgent"
                                    ).get("schemeName"),
                                    "creditor_agent_name": payment.get(
                                        "CreditorAgent"
                                    ).get("name"),
                                    "creditor_agent_identification": payment.get(
                                        "CreditorAgent"
                                    ).get("identification"),
                                    "creditor_agent_accountIdentification": payment.get(
                                        "CreditorAgent"
                                    ).get("accountIdentification"),
                                }
                            )

                        elif payment.get("DebtorParty"):
                            payment_data.update(
                                {
                                    "debitor_party_inn": payment.get("DebtorParty").get(
                                        "inn"
                                    ),
                                    "debitor_party_name": payment.get(
                                        "DebtorParty"
                                    ).get("name"),
                                    "debitor_party_kpp": payment.get("DebtorParty").get(
                                        "kpp"
                                    ),
                                    "debitor_account_identification": payment.get(
                                        "DebtorAccount"
                                    ).get("identification"),
                                    "debitor_account_schemeName": payment.get(
                                        "DebtorAccount"
                                    ).get("schemeName"),
                                    "debitor_agent_schemeName": payment.get(
                                        "DebtorAgent"
                                    ).get("schemeName"),
                                    "debitor_agent_name": payment.get(
                                        "DebtorAgent"
                                    ).get("name"),
                                    "debitor_agent_identification": payment.get(
                                        "DebtorAgent"
                                    ).get("identification"),
                                    "debitor_agent_accountIdentification": payment.get(
                                        "DebtorAgent"
                                    ).get("accountIdentification"),
                                }
                            )
                        else:
                            raise Exception(
                                "не вилидный формат транзакции от Точка банка"
                            )

                        await database.execute(
                            tochka_bank_payments.insert().values(payment_data)
                        )

                    for payment in info_statement.get("Data")["Statement"][0][
                        "Transaction"
                    ]:
                        payment_data = {
                            "accountId": info_statement.get("Data")["Statement"][0].get(
                                "accountId"
                            ),
                            "statementId": info_statement.get("Data")["Statement"][
                                0
                            ].get("statementId"),
                            "statement_creation_datetime": info_statement.get("Data")[
                                "Statement"
                            ][0].get("creationDateTime"),
                            "transactionTypeCode": payment.get("transactionTypeCode"),
                            "transactionId": payment.get("transactionId"),
                            "status": payment.get("status"),
                            "payment_id": payment.get("paymentId"),
                            "documentProcessDate": payment.get("documentProcessDate"),
                            "documentNumber": payment.get("documentNumber"),
                            "description": payment.get("description"),
                            "creditDebitIndicator": payment.get("creditDebitIndicator"),
                            "amount": (
                                payment.get("Amount").get("amount")
                                if payment.get("Amount")
                                else None
                            ),
                            "amountNat": (
                                payment.get("Amount").get("amountNat")
                                if payment.get("Amount")
                                else None
                            ),
                            "currency": (
                                payment.get("Amount").get("currency")
                                if payment.get("Amount")
                                else None
                            ),
                        }
                        if payment.get("CreditorParty"):
                            payment_data.update(
                                {
                                    "creditor_party_inn": payment.get(
                                        "CreditorParty"
                                    ).get("inn"),
                                    "creditor_party_name": payment.get(
                                        "CreditorParty"
                                    ).get("name"),
                                    "creditor_party_kpp": payment.get(
                                        "CreditorParty"
                                    ).get("kpp"),
                                    "creditor_account_identification": payment.get(
                                        "CreditorAccount"
                                    ).get("identification"),
                                    "creditor_account_schemeName": payment.get(
                                        "CreditorAccount"
                                    ).get("schemeName"),
                                    "creditor_agent_schemeName": payment.get(
                                        "CreditorAgent"
                                    ).get("schemeName"),
                                    "creditor_agent_name": payment.get(
                                        "CreditorAgent"
                                    ).get("name"),
                                    "creditor_agent_identification": payment.get(
                                        "CreditorAgent"
                                    ).get("identification"),
                                    "creditor_agent_accountIdentification": payment.get(
                                        "CreditorAgent"
                                    ).get("accountIdentification"),
                                }
                            )
                        elif payment.get("DebtorParty"):
                            payment_data.update(
                                {
                                    "debitor_party_inn": payment.get("DebtorParty").get(
                                        "inn"
                                    ),
                                    "debitor_party_name": payment.get(
                                        "DebtorParty"
                                    ).get("name"),
                                    "debitor_party_kpp": payment.get("DebtorParty").get(
                                        "kpp"
                                    ),
                                    "debitor_account_identification": payment.get(
                                        "DebtorAccount"
                                    ).get("identification"),
                                    "debitor_account_schemeName": payment.get(
                                        "DebtorAccount"
                                    ).get("schemeName"),
                                    "debitor_agent_schemeName": payment.get(
                                        "DebtorAgent"
                                    ).get("schemeName"),
                                    "debitor_agent_name": payment.get(
                                        "DebtorAgent"
                                    ).get("name"),
                                    "debitor_agent_identification": payment.get(
                                        "DebtorAgent"
                                    ).get("identification"),
                                    "debitor_agent_accountIdentification": payment.get(
                                        "DebtorAgent"
                                    ).get("accountIdentification"),
                                }
                            ),
                        else:
                            raise Exception(
                                "не вилидный формат транзакции от Точка банка"
                            )

                        await database.execute(
                            tochka_bank_payments.update()
                            .where(
                                tochka_bank_payments.c.payment_id
                                == payment.get("paymentId")
                            )
                            .values(payment_data)
                        )
                        payment_update = await database.fetch_one(
                            tochka_bank_payments.select().where(
                                tochka_bank_payments.c.payment_id
                                == payment.get("paymentId")
                            )
                        )
                        await database.execute(
                            payments.update()
                            .where(
                                payments.c.id == payment_update.get("payment_crm_id")
                            )
                            .values(
                                {
                                    "name": payment.get("transactionTypeCode"),
                                    "description": payment.get("description"),
                                    "type": (
                                        "outgoing"
                                        if payment.get("creditDebitIndicator")
                                        == "Debit"
                                        else "incoming"
                                    ),
                                    "tags": f"TochkaBank,{account.get('accountId')}",
                                    "amount": payment.get("Amount").get("amount"),
                                    "cashbox": account.get("cashbox_id"),
                                    "paybox": account.get("pbox_id"),
                                    "date": datetime.strptime(
                                        payment.get("documentProcessDate"), "%Y-%m-%d"
                                    ).timestamp(),
                                    "updated_at": int(datetime.utcnow().timestamp()),
                                    "is_deleted": False,
                                    "amount_without_tax": payment.get("Amount").get(
                                        "amount"
                                    ),
                                    "status": (
                                        True
                                        if payment.get("status") == "Booked"
                                        else False
                                    ),
                                }
                            )
                        )

    await _tochka_update()
