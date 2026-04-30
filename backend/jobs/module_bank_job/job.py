import re
from datetime import datetime, timedelta

import aiohttp
from common.decorators import ensure_db_connection
from database.db import (
    async_session_maker,
    contragents,
    docs_sales,
    integrations_to_cashbox,
    module_bank_accounts,
    module_bank_credentials,
    module_bank_operations,
    payments,
    pboxes,
    users_cboxes_relation,
)
from jobs.module_bank_job.repositories.ContragentRepository import ContragentRepository
from sqlalchemy import and_, select


async def extract_number(text):
    match = re.search(r"[№#]\s*(\d+)", text)
    if match:
        return int(match.group(1))
    return None


async def process_payment(contragent_id, operation, cashbox_id, session):
    payment_purpose = operation.get("paymentPurpose")
    if payment_purpose:
        number_document = await extract_number(operation.get("paymentPurpose"))
        if number_document:
            query = docs_sales.select().where(
                and_(
                    docs_sales.c.number == str(number_document),
                    docs_sales.c.cashbox == cashbox_id,
                    docs_sales.c.contragent == contragent_id,
                    docs_sales.c.is_deleted == False,
                )
            )
            result = await session.execute(query)
            docs_sales_info = result.fetchone()
            if docs_sales_info:
                query = (
                    payments.update()
                    .where(payments.c.docs_sales_id == docs_sales_info.id)
                    .values({"status": True})
                )
                await session.execute(query)
                await session.commit()

                query = payments.select().where(
                    payments.c.docs_sales_id == docs_sales_info.id
                )
                result = await session.execute(query)
                payment_id = result.fetchone()

                if payment_id:

                    return True, payment_id.id

    query = docs_sales.select().where(
        and_(
            docs_sales.c.sum == float(operation.get("amount")),
            docs_sales.c.cashbox == cashbox_id,
            docs_sales.c.contragent == contragent_id,
            docs_sales.c.is_deleted == False,
        )
    )
    result = await session.execute(query)
    docs_sales_info = result.fetchone()
    if docs_sales_info:
        query = (
            payments.update()
            .where(payments.c.docs_sales_id == docs_sales_info.id)
            .values({"status": True})
        )
        await session.execute(query)
        await session.commit()

        query = payments.select().where(payments.c.docs_sales_id == docs_sales_info.id)
        result = await session.execute(query)
        payment_id = result.fetchone()

        if payment_id:

            return True, payment_id.id

    return False, 0


@ensure_db_connection
async def module_bank_update_transaction(
    contragent_repository: ContragentRepository = ContragentRepository(),
):
    async with async_session_maker() as session:
        query = (
            select(
                module_bank_credentials.c.id,
                module_bank_credentials.c.access_token,
                users_cboxes_relation.c.cashbox_id,
            )
            .select_from(module_bank_credentials)
            .join(
                integrations_to_cashbox,
                integrations_to_cashbox.c.id
                == module_bank_credentials.c.integration_cashboxes,
            )
            .join(
                users_cboxes_relation,
                users_cboxes_relation.c.id == integrations_to_cashbox.c.installed_by,
            )
        )
        result = await session.execute(query)
        accounts_credentials = result.fetchall()
        for account in accounts_credentials:
            async with aiohttp.ClientSession(trust_env=True) as session_http:
                async with session_http.post(
                    "https://api.modulbank.ru/v1/account-info",
                    headers={
                        "Authorization": f"Bearer {account.access_token}",
                        "Content-type": "application/json",
                    },
                ) as resp:
                    companies_json = await resp.json()
            for company in companies_json:
                for bank_account in company.get("bankAccounts"):
                    data = {
                        "name": f"{bank_account.get('accountName', 'Счёт')} банк Модуль №{bank_account.get('id')}",
                        "start_balance": 0,
                        "cashbox": account.cashbox_id,
                        "balance": bank_account.get("balance"),
                        "update_start_balance": int(datetime.utcnow().timestamp()),
                        "update_start_balance_date": int(datetime.utcnow().timestamp()),
                        "created_at": int(datetime.utcnow().timestamp()),
                        "updated_at": int(datetime.utcnow().timestamp()),
                        "balance_date": 0,
                    }
                    query = module_bank_accounts.select().where(
                        module_bank_accounts.c.accountId == bank_account.get("id")
                    )
                    result = await session.execute(query)
                    account_db = result.fetchone()
                    if not account_db:
                        query = pboxes.insert().values(data).returning(pboxes.c.id)
                        result = await session.execute(query)
                        id_paybox = result.scalar()

                        query = module_bank_accounts.insert().values(
                            {
                                "payboxes_id": id_paybox,
                                "module_bank_credential_id": account.id,
                                "accountName": bank_account.get("accountName"),
                                "bankBic": bank_account.get("bankBic"),
                                "bankInn": bank_account.get("bankInn"),
                                "bankKpp": bank_account.get("bankKpp"),
                                "bankCorrespondentAccount": bank_account.get(
                                    "bankCorrespondentAccount"
                                ),
                                "bankName": bank_account.get("bankName"),
                                "beginDate": bank_account.get("beginDate"),
                                "category": bank_account.get("category"),
                                "currency": bank_account.get("currency"),
                                "accountId": bank_account.get("id"),
                                "number": bank_account.get("number"),
                                "status": bank_account.get("status"),
                                "is_deleted": False,
                                "is_active": True,
                            }
                        )
                        await session.execute(query)
                        await session.commit()
                    else:
                        del data["created_at"]
                        query = (
                            pboxes.update()
                            .where(pboxes.c.id == account_db.payboxes_id)
                            .values(data)
                            .returning(pboxes.c.id)
                        )
                        result = await session.execute(query)
                        id_paybox = result.scalar()

                        query = (
                            module_bank_accounts.update()
                            .where(module_bank_accounts.c.id == account_db.id)
                            .values(
                                {
                                    "payboxes_id": id_paybox,
                                    "module_bank_credential_id": account.id,
                                    "accountName": bank_account.get("accountName"),
                                    "bankBic": bank_account.get("bankBic"),
                                    "bankInn": bank_account.get("bankInn"),
                                    "bankKpp": bank_account.get("bankKpp"),
                                    "bankCorrespondentAccount": bank_account.get(
                                        "bankCorrespondentAccount"
                                    ),
                                    "bankName": bank_account.get("bankName"),
                                    "beginDate": bank_account.get("beginDate"),
                                    "category": bank_account.get("category"),
                                    "currency": bank_account.get("currency"),
                                    "accountId": bank_account.get("id"),
                                    "number": bank_account.get("number"),
                                    "status": bank_account.get("status"),
                                    "is_deleted": False,
                                    "is_active": True,
                                }
                            )
                        )
                        await session.execute(query)
                        await session.commit()

    async with async_session_maker() as session:
        query = (
            select(
                module_bank_accounts.c.accountId,
                module_bank_accounts.c.beginDate,
                module_bank_credentials.c.access_token,
                pboxes.c.id.label("pbox_id"),
                users_cboxes_relation.c.token,
                pboxes.c.cashbox.label("cashbox_id"),
            )
            .where(
                and_(
                    module_bank_accounts.c.is_active == True,
                    module_bank_accounts.c.is_deleted == False,
                    pboxes.c.deleted_at.is_(None),
                )
            )
            .select_from(module_bank_accounts)
            .join(
                module_bank_credentials,
                module_bank_credentials.c.id
                == module_bank_accounts.c.module_bank_credential_id,
            )
            .join(pboxes, pboxes.c.id == module_bank_accounts.c.payboxes_id)
            .join(users_cboxes_relation, users_cboxes_relation.c.id == pboxes.c.cashbox)
        )
        result = await session.execute(query)
        active_accounts_with_credentials = result.fetchall()
        for account in active_accounts_with_credentials:
            async with aiohttp.ClientSession(trust_env=True) as http_session:
                async with http_session.post(
                    f"https://api.modulbank.ru/v1/account-info/balance/{account.accountId}",
                    headers={
                        "Authorization": f"Bearer {account.access_token}",
                        "Content-type": "application/json",
                    },
                ) as resp:
                    balance_json = await resp.json()

            if balance_json is None:
                raise Exception(
                    "проблема с получением баланса (вероятно некорректный access_token)"
                )

            query = (
                pboxes.update()
                .where(pboxes.c.id == account.pbox_id)
                .values(
                    {
                        "balance": balance_json,
                        "updated_at": int(datetime.utcnow().timestamp()),
                        "balance_date": int(datetime.utcnow().timestamp()),
                    }
                )
            )
            await session.execute(query)
            await session.commit()

            page = 1
            while True:
                async with aiohttp.ClientSession(trust_env=True) as http_session:
                    async with http_session.post(
                        f"https://api.modulbank.ru/v1/operation-history/{account.accountId}",
                        headers={
                            "Authorization": f"Bearer {account.access_token}",
                            "Content-type": "application/json",
                        },
                        json={
                            "statuses": ["Executed", "Received"],
                            "from": f"{account.beginDate}Z",
                            "till": (
                                datetime.now().date() + timedelta(days=1)
                            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "skip": 50 * (page - 1),
                            "records": (50 * page),
                        },
                    ) as resp:
                        operations_json = await resp.json()

                if not operations_json:
                    break

                page += 1

                query = (
                    select(*payments.columns, module_bank_operations.c.operationId)
                    .where(
                        and_(
                            payments.c.paybox == account.pbox_id,
                            payments.c.cashbox == account.cashbox_id,
                        )
                    )
                    .select_from(payments)
                    .join(
                        module_bank_operations,
                        module_bank_operations.c.payment_crm_id == payments.c.id,
                    )
                )
                result = await session.execute(query)
                module_operations_db = result.fetchall()

                if not module_operations_db:
                    for operation in operations_json:
                        contragent_db = (
                            await contragent_repository.get_contragent_by_inn(
                                inn=operation.get("contragentInn"),
                                cashbox_id=account.cashbox_id,
                            )
                        )

                        result_process = False

                        if contragent_db:
                            contragent_id = contragent_db.id
                            if operation.get("category") == "Debet":
                                result_process, payment_create_id = (
                                    await process_payment(
                                        contragent_id=contragent_db.id,
                                        operation=operation,
                                        cashbox_id=account.cashbox_id,
                                        session=session,
                                    )
                                )
                        else:
                            query = (
                                contragents.insert()
                                .values(
                                    {
                                        "name": operation.get("contragentName"),
                                        "inn": operation.get("contragentInn"),
                                        "cashbox": account.cashbox_id,
                                        "is_deleted": False,
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                    }
                                )
                                .returning(contragents.c.id)
                            )
                            result = await session.execute(query)
                            await session.commit()
                            contragent_id = result.scalar()

                        if not result_process:
                            query = (
                                payments.insert()
                                .values(
                                    {
                                        "name": operation.get("paymentPurpose"),
                                        "description": operation.get("paymentPurpose"),
                                        "type": (
                                            "outgoing"
                                            if operation.get("category") == "Credit"
                                            else "incoming"
                                        ),
                                        "tags": f"ModuleBank,{account.accountId}",
                                        "amount": operation.get("amount"),
                                        "cashbox": account.cashbox_id,
                                        "paybox": account.pbox_id,
                                        "date": (
                                            datetime.now().timestamp()
                                            if not operation.get("executed")
                                            else datetime.strptime(
                                                operation.get("executed"),
                                                "%Y-%m-%dT%H:%M:%S",
                                            ).timestamp()
                                        ),
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "is_deleted": False,
                                        "amount_without_tax": operation.get("amount"),
                                        "status": True,
                                        "stopped": True,
                                        "contragent": contragent_id,
                                    }
                                )
                                .returning(payments.c.id)
                            )
                            result = await session.execute(query)
                            payment_create_id = result.scalar()

                        payment_data = {
                            "accountId": account.accountId,
                            "payment_crm_id": payment_create_id,
                            "operationId": operation.get("id"),
                            "cardId": operation.get("cardId"),
                            "companyId": operation.get("companyId"),
                            "status": operation.get("status"),
                            "category": operation.get("category"),
                            "contragentName": operation.get("contragentName"),
                            "contragentInn": operation.get("contragentInn"),
                            "contragentKpp": operation.get("contragentKpp"),
                            "contragentBankAccountNumber": operation.get(
                                "contragentBankAccountNumber"
                            ),
                            "contragentBankName": operation.get("contragentBankName"),
                            "contragentBankBic": operation.get("contragentBankBic"),
                            "currency": operation.get("currency"),
                            "amount": operation.get("amount"),
                            "bankAccountNumber": operation.get("bankAccountNumber"),
                            "paymentPurpose": operation.get("paymentPurpose"),
                            "executed": operation.get("executed"),
                            "created": operation.get("created"),
                            "absId": operation.get("absId"),
                            "ibsoId": operation.get("ibsoId"),
                            "kbk": operation.get("kbk"),
                            "oktmo": operation.get("oktmo"),
                            "paymentBasis": operation.get("paymentBasis"),
                            "taxCode": operation.get("taxCode"),
                            "taxDocNum": operation.get("taxDocNum"),
                            "taxDocDate": operation.get("taxDocDate"),
                            "payerStatus": operation.get("payerStatus"),
                            "uin": operation.get("uin"),
                        }

                        query = module_bank_operations.insert().values(payment_data)
                        await session.execute(query)
                        await session.commit()
                else:
                    set_module_payments = set(
                        [item.get("id") for item in operations_json]
                    )
                    set_module_payments_db = set(
                        [item.operationId for item in module_operations_db]
                    )
                    new_paymentsId = list(set_module_payments - set_module_payments_db)
                    for operation in [
                        item
                        for item in operations_json
                        if item.get("id") in new_paymentsId
                    ]:

                        contragent_db = (
                            await contragent_repository.get_contragent_by_inn(
                                inn=operation.get("contragentInn"),
                                cashbox_id=account.cashbox_id,
                            )
                        )

                        result_process = False

                        if contragent_db:
                            contragent_id = contragent_db.id
                            result_process, payment_create_id = await process_payment(
                                contragent_id=contragent_db.id,
                                operation=operation,
                                cashbox_id=account.cashbox_id,
                                session=session,
                            )
                        else:
                            query = (
                                contragents.insert()
                                .values(
                                    {
                                        "name": operation.get("contragentName"),
                                        "inn": operation.get("contragentInn"),
                                        "cashbox": account.cashbox_id,
                                        "is_deleted": False,
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                    }
                                )
                                .returning(contragents.c.id)
                            )
                            result = await session.execute(query)
                            await session.commit()
                            contragent_id = result.scalar()

                        if not result_process:

                            query = (
                                payments.insert()
                                .values(
                                    {
                                        "name": operation.get("paymentPurpose"),
                                        "description": operation.get("paymentPurpose"),
                                        "type": (
                                            "outgoing"
                                            if operation.get("category") == "Credit"
                                            else "incoming"
                                        ),
                                        "tags": f"ModuleBank,{account.accountId}",
                                        "amount": operation.get("amount"),
                                        "cashbox": account.cashbox_id,
                                        "paybox": account.pbox_id,
                                        "date": (
                                            datetime.now().timestamp()
                                            if not operation.get("executed")
                                            else datetime.strptime(
                                                operation.get("executed"),
                                                "%Y-%m-%dT%H:%M:%S",
                                            ).timestamp()
                                        ),
                                        "created_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "updated_at": int(
                                            datetime.utcnow().timestamp()
                                        ),
                                        "is_deleted": False,
                                        "amount_without_tax": operation.get("amount"),
                                        "status": True,
                                        "stopped": True,
                                        "contragent": contragent_id,
                                    }
                                )
                                .returning(payments.c.id)
                            )
                            result = await session.execute(query)
                            payment_create_id = result.scalar()

                        payment_data = {
                            "accountId": account.accountId,
                            "payment_crm_id": payment_create_id,
                            "operationId": operation.get("id"),
                            "cardId": operation.get("cardId"),
                            "companyId": operation.get("companyId"),
                            "status": operation.get("status"),
                            "category": operation.get("category"),
                            "contragentName": operation.get("contragentName"),
                            "contragentInn": operation.get("contragentInn"),
                            "contragentKpp": operation.get("contragentKpp"),
                            "contragentBankAccountNumber": operation.get(
                                "contragentBankAccountNumber"
                            ),
                            "contragentBankName": operation.get("contragentBankName"),
                            "contragentBankBic": operation.get("contragentBankBic"),
                            "currency": operation.get("currency"),
                            "amount": operation.get("amount"),
                            "bankAccountNumber": operation.get("bankAccountNumber"),
                            "paymentPurpose": operation.get("paymentPurpose"),
                            "executed": operation.get("executed"),
                            "created": operation.get("created"),
                            "absId": operation.get("absId"),
                            "ibsoId": operation.get("ibsoId"),
                            "kbk": operation.get("kbk"),
                            "oktmo": operation.get("oktmo"),
                            "paymentBasis": operation.get("paymentBasis"),
                            "taxCode": operation.get("taxCode"),
                            "taxDocNum": operation.get("taxDocNum"),
                            "taxDocDate": operation.get("taxDocDate"),
                            "payerStatus": operation.get("payerStatus"),
                            "uin": operation.get("uin"),
                        }

                        query = module_bank_operations.insert().values(payment_data)
                        await session.execute(query)
                        await session.commit()

                    for operation in operations_json:
                        payment_data = {
                            "accountId": account.accountId,
                            "operationId": operation.get("id"),
                            "cardId": operation.get("cardId"),
                            "companyId": operation.get("companyId"),
                            "status": operation.get("status"),
                            "category": operation.get("category"),
                            "contragentName": operation.get("contragentName"),
                            "contragentInn": operation.get("contragentInn"),
                            "contragentKpp": operation.get("contragentKpp"),
                            "contragentBankAccountNumber": operation.get(
                                "contragentBankAccountNumber"
                            ),
                            "contragentBankName": operation.get("contragentBankName"),
                            "contragentBankBic": operation.get("contragentBankBic"),
                            "currency": operation.get("currency"),
                            "amount": operation.get("amount"),
                            "bankAccountNumber": operation.get("bankAccountNumber"),
                            "paymentPurpose": operation.get("paymentPurpose"),
                            "executed": operation.get("executed"),
                            "created": operation.get("created"),
                            "absId": operation.get("absId"),
                            "ibsoId": operation.get("ibsoId"),
                            "kbk": operation.get("kbk"),
                            "oktmo": operation.get("oktmo"),
                            "paymentBasis": operation.get("paymentBasis"),
                            "taxCode": operation.get("taxCode"),
                            "taxDocNum": operation.get("taxDocNum"),
                            "taxDocDate": operation.get("taxDocDate"),
                            "payerStatus": operation.get("payerStatus"),
                            "uin": operation.get("uin"),
                        }

                        query = (
                            module_bank_operations.update()
                            .where(
                                module_bank_operations.c.operationId
                                == operation.get("id")
                            )
                            .values(payment_data)
                        )
                        await session.execute(query)
                        await session.commit()

                        query = module_bank_operations.select().where(
                            module_bank_operations.c.operationId == operation.get("id")
                        )
                        result = await session.execute(query)
                        payment_update = result.fetchone()

                        query = (
                            payments.update()
                            .where(payments.c.id == payment_update.payment_crm_id)
                            .values(
                                {
                                    "name": operation.get("paymentPurpose"),
                                    "description": operation.get("paymentPurpose"),
                                    "type": (
                                        "outgoing"
                                        if operation.get("category") == "Credit"
                                        else "incoming"
                                    ),
                                    "tags": f"ModuleBank,{account.accountId}",
                                    "amount": operation.get("amount"),
                                    "cashbox": account.cashbox_id,
                                    "paybox": account.pbox_id,
                                    "date": (
                                        datetime.now().timestamp()
                                        if not operation.get("executed")
                                        else datetime.strptime(
                                            operation.get("executed"),
                                            "%Y-%m-%dT%H:%M:%S",
                                        ).timestamp()
                                    ),
                                    "updated_at": int(datetime.utcnow().timestamp()),
                                    "is_deleted": False,
                                    "amount_without_tax": operation.get("amount"),
                                    "status": True,
                                }
                            )
                        )
                        await session.execute(query)
                        await session.commit()
