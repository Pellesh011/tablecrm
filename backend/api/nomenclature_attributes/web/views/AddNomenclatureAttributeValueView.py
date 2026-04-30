from collections import defaultdict

from api.nomenclature.infrastructure.readers.core.INomenclatureReader import (
    INomenclatureReader,
)
from api.nomenclature_attributes.infrastructure.readers.core.INomenclatureAttributesReader import (
    INomenclatureAttributesReader,
)
from api.nomenclature_attributes.web.models import schemas
from asyncpg import UniqueViolationError
from database.db import database, nomenclature_attributes_value
from fastapi import HTTPException
from functions.helpers import get_user_by_token
from sqlalchemy import select
from starlette import status


class AddNomenclatureAttributeValueView:

    def __init__(
        self,
        nomenclature_reader: INomenclatureReader,
        nomenclature_attributes_reader: INomenclatureAttributesReader,
    ):
        self.__nomenclature_reader = nomenclature_reader
        self.__nomenclature_attributes_reader = nomenclature_attributes_reader

    async def __call__(
        self, token: str, attribute_value_data: schemas.AttributeValueCreate
    ):
        user = await get_user_by_token(token)

        nomenclature_info = await self.__nomenclature_reader.get_by_id(
            id=attribute_value_data.nomenclature_id, cashbox_id=user.cashbox_id
        )

        if not nomenclature_info:
            raise HTTPException(
                status_code=404,
                detail=f"Номенклатура с ID '{attribute_value_data.nomenclature_id}' не найдена",
            )

        attribute_ids = [
            attribute.attribute_id for attribute in attribute_value_data.attributes
        ]

        existing_attribute_ids = (
            await self.__nomenclature_attributes_reader.get_ids_by_ids(
                ids=attribute_ids, cashbox_id=user.cashbox_id
            )
        )

        query = select(
            nomenclature_attributes_value.c.attribute_id,
            nomenclature_attributes_value.c.value,
        ).where(
            nomenclature_attributes_value.c.nomenclature_id
            == attribute_value_data.nomenclature_id
        )
        existing_values = await database.fetch_all(query)

        existing_values_map = {}
        for record in existing_values:
            if record["attribute_id"] not in existing_values_map:
                existing_values_map[record["attribute_id"]] = set()
            existing_values_map[record["attribute_id"]].add(record["value"])

        attributes_to_insert = []
        for attribute in attribute_value_data.attributes:
            if attribute.attribute_id not in existing_attribute_ids:
                raise HTTPException(
                    status_code=404,
                    detail=f"Атрибут с ID '{attribute.attribute_id}' не найден",
                )
            if attribute.value in existing_values_map.get(
                attribute.attribute_id, set()
            ):
                raise HTTPException(
                    status_code=400,
                    detail=f"Значение '{attribute.value}' для атрибута с ID '{attribute.attribute_id}' уже существует.",
                )
            attributes_to_insert.append(
                {
                    "nomenclature_id": attribute_value_data.nomenclature_id,
                    "attribute_id": attribute.attribute_id,
                    "value": attribute.value,
                }
            )

        try:
            query = (
                nomenclature_attributes_value.insert()
                .values(attributes_to_insert)
                .returning(nomenclature_attributes_value.c.id)
            )
            result = await database.fetch_all(query)
        except UniqueViolationError as error:
            if error.constraint_name == "uq_nomenclature_attributes_value_attribute_id_nomenclature_id":  # type: ignore
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Ошибка: Уже существует значение для этого атрибута и номенклатуры.",
                )
            else:
                raise error

        attributes = defaultdict(list)
        for record in zip(result, attributes_to_insert):
            attributes[record[1]["attribute_id"]].append(
                {"attribute_value_id": record[0].id, "value": record[1]["value"]}
            )

        return schemas.AttributeValueResponse(
            nomenclature_id=attribute_value_data.nomenclature_id,
            attributes=[
                {
                    "attribute_id": attribute.attribute_id,
                    "attribute_value": attributes.get(attribute.attribute_id, []),
                }
                for attribute in attribute_value_data.attributes
            ],
        )
