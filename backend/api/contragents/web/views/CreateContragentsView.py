from datetime import datetime
from typing import List, Union

import api.contragents.schemas as ca_schemas
import phonenumbers
from database.db import (
    contragents,
    contragents_tags,
    database,
    tags,
    users_cboxes_relation,
)
from fastapi import HTTPException
from fastapi.responses import Response
from phonenumbers import geocoder
from sqlalchemy import select
from ws_manager import manager


class CreateContragentsView:

    async def __call__(
        self,
        token: str,
        body: Union[ca_schemas.ContragentCreate, List[ca_schemas.ContragentCreate]],
    ):
        user = await database.fetch_one(
            users_cboxes_relation.select(users_cboxes_relation.c.token == token)
        )
        if not user:
            raise HTTPException(403, "Неверный токен")
        if not user.status:
            raise HTTPException(403, "Неверный токен")

        items = body if isinstance(body, list) else [body]
        insert_values, phones_seen = [], set()
        all_tag_ids: set[int] = set()
        items_tags: list[list[int]] = []

        for item in items:
            data = item.dict(exclude_unset=True)

            phone_number = data.get("phone")
            phone_code = None
            is_phone_formatted = False
            tag_ids = data.pop("tags_id", None) or []

            items_tags.append(tag_ids)
            all_tag_ids.update(tag_ids)  # добавляем теги в множество для уникальности

            if phone_number:
                try:
                    phone_number_with_plus = (
                        f"+{phone_number}"
                        if not phone_number.startswith("+")
                        else phone_number
                    )
                    number_phone_parsed = phonenumbers.parse(
                        phone_number_with_plus, "RU"
                    )
                    phone_number = phonenumbers.format_number(
                        number_phone_parsed, phonenumbers.PhoneNumberFormat.E164
                    )
                    phone_code = geocoder.description_for_number(
                        number_phone_parsed, "en"
                    )
                    is_phone_formatted = True
                    if not phone_code:
                        phone_number = data["phone"]
                        is_phone_formatted = False
                except:
                    try:
                        number_phone_parsed = phonenumbers.parse(phone_number, "RU")
                        phone_number = phonenumbers.format_number(
                            number_phone_parsed, phonenumbers.PhoneNumberFormat.E164
                        )
                        phone_code = geocoder.description_for_number(
                            number_phone_parsed, "en"
                        )
                        is_phone_formatted = True
                        if not phone_code:
                            phone_number = data["phone"]
                            is_phone_formatted = False
                    except:
                        phone_number = data["phone"]
                        is_phone_formatted = False

            if phone_number is not None and phone_number in phones_seen:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": "Phone number already in use",
                        "phones": [phone_number],
                    },
                )

            if phone_number is not None:
                phones_seen.add(phone_number)

            insert_values.append(
                {
                    "name": data.get("name", ""),
                    "external_id": data.get("external_id", ""),
                    "inn": data.get("inn", ""),
                    "phone": phone_number,
                    "phone_code": phone_code,
                    "is_phone_formatted": is_phone_formatted,
                    "description": data.get("description"),
                    "contragent_type": data.get("contragent_type"),
                    "birth_date": data.get("birth_date"),
                    "data": data.get("data"),
                    "cashbox": user.cashbox_id,
                    "is_deleted": False,
                    "created_at": int(datetime.now().timestamp()),
                    "updated_at": int(datetime.now().timestamp()),
                    "email": data.get("email"),
                }
            )

        if not insert_values:
            return Response(status_code=204)

        # проверка на теги, если есть, то сохраняем
        if all_tag_ids:
            existing_tags_rows = await database.fetch_all(
                select(tags.c.id).where(
                    tags.c.cashbox_id == user.cashbox_id, tags.c.id.in_(all_tag_ids)
                )
            )

            existing_tag_ids = {r.id for r in existing_tags_rows}

            # проверяем на несуществующие теги
            missing_tag_ids = all_tag_ids - existing_tag_ids
            if missing_tag_ids:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "message": "Теги не найдены",
                        "tags_id": list(missing_tag_ids),
                    },
                )

        phones_to_check = {p["phone"] for p in insert_values if p["phone"]}
        existing_phones: set[str] = set()
        if phones_to_check:
            rows = await database.fetch_all(
                select(contragents.c.phone).where(
                    contragents.c.cashbox == user.cashbox_id,
                    contragents.c.phone.in_(phones_to_check),
                    contragents.c.is_deleted.is_(False),
                )
            )
            existing_phones = {r.phone for r in rows}

        duplicated_contragent_phones = [
            p for p in insert_values if p["phone"] in existing_phones
        ]

        if duplicated_contragent_phones:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Phone number already in use",
                    "phones": [p["phone"] for p in duplicated_contragent_phones],
                },
            )
        query = contragents.insert().values(insert_values).returning(contragents.c.id)
        ids = await database.fetch_all(query=query)

        # Сохраняем теги в отдельную бд contragent_tags
        contragent_ids = [k.id for k in ids]

        contragent_tags_values = []

        for contragent_id, tag_ids in zip(contragent_ids, items_tags):
            for tag_id in set(tag_ids):
                contragent_tags_values.append(
                    {
                        "tag_id": tag_id,
                        "contragent_id": contragent_id,
                        "cashbox_id": user.cashbox_id,
                    }
                )

        if contragent_tags_values:
            await database.execute(
                contragents_tags.insert().values(contragent_tags_values)
            )

        rows = await database.fetch_all(
            select(contragents).where(contragents.c.id.in_([k.id for k in ids]))
        )
        for r in rows:
            await manager.send_message(
                token, {"action": "create", "target": "contragents", "result": dict(r)}
            )

        return rows if isinstance(body, list) else rows[0]
