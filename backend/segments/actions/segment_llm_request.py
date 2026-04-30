import json
from dataclasses import dataclass
from typing import List

from api.segments.schema_actions import NomenclatureFields
from database.db import (
    categories,
    database,
    global_categories,
    units,
    users_cboxes_relation,
)
from openai import AsyncOpenAI
from sqlalchemy import insert, select

PROMPT = """
            You are an expert e-commerce product data specialist.
            Given only the product name below, fully populate all required fields for the accounting system.
            Be accurate, creative, and SEO-optimized.
            Answer Russian only.
            Use exactly those entities (like categories, global_categories, units and etc.) described in the "ADDITIONAL INFORMATION" section (or suggest a new suitable one if none of them fits perfectly).:
            USE OR CREATE ONLY THOSE ENTITIES THAT ARE MEANINGFULLY RELATED TO THE PRODUCT NAME:

            Product name: {0}

            ======ADDITIONAL INFORMATION=========
            {1}
            ====================================
        """

FIELDS_ADDITIONAL_DATA = {
    NomenclatureFields.CATEGORY.value: select(
        categories.c.id, categories.c.name
    ).select_from(categories),
    NomenclatureFields.GLOBAL_CATEGORY.value: select(
        global_categories.c.id, global_categories.c.name
    ).select_from(global_categories),
    NomenclatureFields.UNIT.value: select(units.c.id, units.c.name).select_from(units),
}

FIELDS_PROPERTIES = {
    NomenclatureFields.DESCRIPTION_SHORT.value: {
        "type": "string",
        "description": "Короткое описание товара",
    },
    NomenclatureFields.DESCRIPTION_LONG.value: {
        "type": "string",
        "description": "Подробное описание товара",
    },
    NomenclatureFields.CATEGORY.value: {
        "type": "object",
        "properties": {
            "id": {"type": "integer", "description": "Id категории"},
            "name": {"type": "string", "description": "Название категории"},
            "description": {
                "type": "string",
                "description": "Описание глобальной категории",
            },
        },
        "required": ["id", "name", "description"],
        "additionalProperties": False,
    },
    NomenclatureFields.GLOBAL_CATEGORY.value: {
        "type": "object",
        "properties": {
            "id": {"type": "integer", "description": "Id глобальной категории"},
            "name": {"type": "string", "description": "Название глобальной категории"},
            "description": {
                "type": "string",
                "description": "Описание глобальной категории",
            },
        },
        "required": ["id", "name", "description"],
        "additionalProperties": False,
    },
    NomenclatureFields.SEO_TITLE.value: {
        "type": "string",
        "description": "SEO заголовок",
    },
    NomenclatureFields.SEO_DESCRIPTION.value: {
        "type": "string",
        "description": "SEO описание",
    },
    NomenclatureFields.SEO_KEYWORDS.value: {
        "type": "array",
        "items": {"type": "string"},
        "description": "SEO ключевые слова",
    },
    NomenclatureFields.ADDRESS.value: {
        "type": "string",
        "description": "Точный адрес товара в России",
    },
    NomenclatureFields.UNIT.value: {
        "type": "object",
        "properties": {
            "id": {"type": "integer", "description": "Id единицы измерения"},
            "name": {"type": "string", "description": "Название единицы измерения"},
        },
        "required": ["id", "name"],
        "additionalProperties": False,
    },
}


@dataclass
class LLMRequestConfig:
    nomenclature_name: str
    fields: List[str]


async def send_llm_request(
    model: str, api_key: str, base_url: str, cashbox_id: int, config: LLMRequestConfig
) -> dict:
    """
    Отправляет запрос для генерации полей товара

    Args:
        model: Название модели
        api_key: API ключ
        base_url: URL сервиса
        cashbox_id: cashbox id
        config (type[LLMRequestConfig]): Конфиг запроса

    Returns:
        dict: json ответ от llm
    """
    additional_data = {
        field: await get_additional_data(
            FIELDS_ADDITIONAL_DATA[field], field, cashbox_id
        )
        for field in config.fields
        if field in FIELDS_ADDITIONAL_DATA
    }

    additional_data_str = "\n".join(
        [
            f"{field.upper()}: {[(row[0], row[1]) for row in rows]}"
            for field, rows in additional_data.items()
        ]
    )

    properties = {
        field: FIELDS_PROPERTIES[field]
        for field in config.fields
        if field in FIELDS_PROPERTIES
    }

    client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    response = await client.chat.completions.create(
        model=model,
        messages=[
            dict(
                role="system",
                content=PROMPT.format(config.nomenclature_name, additional_data_str),
            ),
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "product",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": properties,
                    "required": config.fields,
                    "additionalProperties": False,
                },
            },
        },
    )

    response = await process_response(response.choices[0].message.content, cashbox_id)
    return response


async def process_response(response: str, cashbox_id: int) -> dict:
    response_json = json.loads(response)

    if category := response_json.get("category"):
        # Создаем новую категорию, если такой нет
        id = await create_entity_if_not_exists(categories, category, cashbox_id)
        if id is None:
            id = category["id"]

        del response_json["category"]
        response_json["category"] = id

    if global_category := response_json.get("global_category"):
        id = await create_entity_if_not_exists(
            global_categories, global_category, cashbox_id
        )
        if id is None:
            id = global_category["id"]
        del response_json["global_category"]
        response_json["global_category_id"] = id

    if unit := response_json.get("unit"):
        id = await create_entity_if_not_exists(units, unit, cashbox_id)
        if id is None:
            id = unit["id"]
        del response_json["unit"]
        response_json["unit"] = id

    return response_json


async def create_entity_if_not_exists(table, obj, cashbox_id):
    exists_query = select(table.c.id).where(table.c.id == obj["id"])
    existing = await database.fetch_one(exists_query)

    if existing is not None:
        return existing.id

    insert_data = {}

    for col in table.columns:
        if col.name == "id":
            continue

        if col.name == "owner":
            owner_row = await database.fetch_one(
                select(users_cboxes_relation.c.id).where(
                    users_cboxes_relation.c.cashbox_id == cashbox_id
                )
            )
            insert_data["owner"] = owner_row.id

        elif col.name == "cashbox":
            insert_data["cashbox"] = cashbox_id

        elif col.name == "status":
            insert_data["status"] = True

        elif col.name == "description" and "description" in obj:
            insert_data["description"] = obj["description"]
        else:
            if col.name in obj:
                insert_data[col.name] = obj[col.name]

    stmt = insert(table).values(insert_data).returning(table.c.id)
    new_id = await database.execute(stmt)
    return new_id


async def get_additional_data(query, field, cashbox_id):
    if NomenclatureFields.CATEGORY.value == field:
        query = query.where(categories.c.cashbox == cashbox_id)

    return await database.fetch_all(query)
