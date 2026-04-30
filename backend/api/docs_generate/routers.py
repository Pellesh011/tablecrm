import base64
import datetime
from io import BytesIO
from os import environ
from typing import Any, Dict
from uuid import uuid4

import aioboto3
import pdfkit
import qrcode
from api.docs_generate.schemas import ReGenerateList, TypeDoc
from database.db import database, doc_generated, doc_templates
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import Response
from functions.helpers import get_user_by_token
from jinja2 import Template
from jinja2.filters import FILTERS
from sqlalchemy import desc


def render_qrcode(value):
    qr_image = qrcode.make(value, box_size=15)
    qr_image_pil = qr_image.get_image()
    stream = BytesIO()
    qr_image_pil.save(stream, format="PNG")
    qr_image_data = stream.getvalue()
    qr_image_base64 = base64.b64encode(qr_image_data).decode("utf-8")
    return f"data:image/png;base64,{qr_image_base64}"


FILTERS["render_qrcode"] = render_qrcode

s3_session = aioboto3.Session()

s3_data = {
    "service_name": "s3",
    "endpoint_url": environ.get("S3_URL"),
    "aws_access_key_id": environ.get("S3_ACCESS"),
    "aws_secret_access_key": environ.get("S3_SECRET"),
}

bucket_name = "5075293c-docs_generated"

router = APIRouter(tags=["docgenerated"])


def generate_doc(template, *kwargs) -> Any:
    try:
        tm = Template(template)
        return tm.render(*kwargs)
    except Exception as error:
        return error


def convert_html_to_pdf(doc_render):
    return pdfkit.from_string(doc_render, options={"enable-local-file-access": ""})


@router.post("/docgenerated/")
async def doc_generate(
    token: str,
    template_id: int,
    variable: Dict,
    type_doc: TypeDoc,
    entity: str = None,
    entity_id: int = None,
    tags: str = None,
):
    """Генерирование документа с загрузкой в S3 и фиксацией записи генерации"""

    user = await get_user_by_token(token)
    query = doc_templates.select().where(
        doc_templates.c.id == template_id, doc_templates.c.cashbox == user.cashbox_id
    )
    template = await database.fetch_one(query)
    data = generate_doc(template["template_data"], variable)

    file_link = f"docsgenerate/{entity}_{entity_id}_{uuid4().hex[:8]}"
    if type_doc is TypeDoc.html:
        file_link += ".html"
    if type_doc is TypeDoc.pdf:
        data = convert_html_to_pdf(data)
        file_link += ".pdf"

    async with s3_session.client(**s3_data) as s3:
        await s3.put_object(Body=data, Bucket=bucket_name, Key=file_link)

    file_dict = {
        "cashbox_id": user.cashbox_id,
        "doc_link": file_link,
        "created_at": datetime.datetime.now(),
        "tags": None if not tags else tags.lower(),
        "template_id": template_id,
        "entity": entity,
        "entity_id": entity_id,
        "type_doc": type_doc,
    }
    query = doc_generated.insert().values(file_dict)
    result_file_dict_id = await database.execute(query)
    query = doc_generated.select().where(
        doc_generated.c.id == result_file_dict_id,
        doc_generated.c.cashbox_id == user.cashbox_id,
    )
    result = await database.fetch_one(query)
    return result


@router.get("/docgenerated/{idx}", status_code=status.HTTP_200_OK)
async def get_doc_generate_by_idx(token: str, idx: int):
    """Получение реквизитов документа по ID"""

    user = await get_user_by_token(token)
    query = doc_generated.select().where(
        doc_generated.c.id == idx, doc_generated.c.cashbox_id == user.cashbox_id
    )
    result = await database.fetch_one(query)
    return result


@router.get("/docgenerated/file/{filename}")
async def get_generate_docs_by_filename(filename: str, type_doc: TypeDoc):
    """Получение документа по имени файла"""

    async with s3_session.client(**s3_data) as s3:
        try:
            file_key = f"docsgenerate/{filename}"
            s3_ob = await s3.get_object(Bucket=bucket_name, Key=file_key)
            body = await s3_ob["Body"].read()
            if type_doc is TypeDoc.html:

                return Response(
                    content=body,
                    media_type="text/html",
                    headers={"Content-Disposition": f'inline; filename="{filename}"'},
                )
            if type_doc is TypeDoc.pdf:

                return Response(
                    content=body,
                    media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="{filename}"'},
                )

        except Exception as err:
            return HTTPException(
                status_code=404, detail="Такого документа не существует"
            )


@router.get("/docgenerated/", status_code=status.HTTP_200_OK)
async def get_doc_generate_list(
    token: str, tags: str = None, limit: int = 100, offset: int = 0
):
    """Получение списка генераций"""
    user = await get_user_by_token(token)
    if tags:
        tags = list(
            map(lambda x: x.strip().lower(), tags.replace(" ", "").strip().split(","))
        )
        filter_tags = list(map(lambda x: doc_generated.c.tags.like(f"%{x}%"), tags))
        query = (
            doc_generated.select()
            .where(doc_generated.c.cashbox_id == user.cashbox_id, *filter_tags)
            .order_by(desc(doc_generated.c.created_at))
            .limit(limit)
            .offset(offset)
        )
        result = await database.fetch_all(query)
        return {"results": result}
    else:
        query = (
            doc_generated.select()
            .where(doc_generated.c.cashbox_id == user.cashbox_id)
            .limit(limit)
            .offset(offset)
        )
        result = await database.fetch_all(query)
        return {"results": result}


@router.post("/regenerated/", status_code=status.HTTP_200_OK)
async def regenerated(token: str, generateList: ReGenerateList):
    pass
