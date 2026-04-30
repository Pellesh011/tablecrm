import json
import uuid
from os import environ

from api.apple_wallet_card_settings.schemas import (
    WalletCardSettings,
    WalletCardSettingsCreate,
    WalletCardSettingsUpdate,
)
from api.apple_wallet_card_settings.utils import create_default_apple_wallet_setting
from common.s3_service.impl.S3Client import S3Client
from common.s3_service.models.S3SettingsModel import S3SettingsModel
from database.db import (
    apple_wallet_card_settings,
    database,
    loyality_cards,
    users_cboxes_relation,
)
from fastapi import APIRouter, File, HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse
from producer import publish_apple_wallet_pass_update
from sqlalchemy import select
from starlette.staticfiles import StaticFiles

router = APIRouter(
    prefix="/apple_wallet_card_settings", tags=["apple_wallet_card_settings"]
)
router.mount(
    "/backend/static_files",
    StaticFiles(directory="/backend/static_files"),
    name="static_files",
)

BUCKET_NAME = "5075293c-docs_generated"
S3_FOLDER = "photos"


def get_s3_client() -> S3Client:
    return S3Client(
        S3SettingsModel(
            aws_access_key_id=environ.get("S3_ACCESS"),
            aws_secret_access_key=environ.get("S3_SECRET"),
            endpoint_url=environ.get("S3_URL"),
        )
    )


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Простая валидация типа (при необходимости расширите)
    if file.content_type not in (
        "image/png",
        "image/jpeg",
        "image/svg+xml",
        "image/webp",
        "application/octet-stream",
    ):
        # допускаем generic binary если нужно
        # при желании вернуть 415 Unsupported Media Type
        return Response(status_code=415)

    print(1)
    # Генерируем уникальное имя файла с сохранением расширения
    unique_name = f"{uuid.uuid4().hex}-{file.filename.split('.')[0]}.{file.filename.split('.')[-1]}"
    s3_key = f"{S3_FOLDER}/{unique_name}"

    try:
        # Читаем файл в байты
        file_bytes = await file.read()
        print(2)
        # Загружаем в S3
        s3_client = get_s3_client()
        await s3_client.upload_file_object(BUCKET_NAME, s3_key, file_bytes)
        print(3)
        # Возвращаем путь в S3
        return JSONResponse(content={"path": s3_key})
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to upload file to S3: {str(e)}"
        )


@router.get("", response_model=WalletCardSettings)
async def get_apple_wallet_card_settings(token: str):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    settings_query = select(apple_wallet_card_settings.c.data).where(
        apple_wallet_card_settings.c.cashbox_id == user.cashbox_id
    )
    settings = await database.fetch_one(settings_query)

    if not settings:
        return await create_default_apple_wallet_setting(user.cashbox_id)

    return WalletCardSettings(**json.loads(settings.data))


@router.post("", response_model=WalletCardSettings)
async def create_apple_wallet_card_settings(token: str, settings: WalletCardSettings):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    setting_stmt = (
        apple_wallet_card_settings.insert()
        .values(
            WalletCardSettingsCreate(cashbox_id=user.cashbox_id, data=settings).dict()
        )
        .returning(apple_wallet_card_settings.c.data)
    )
    settings = await database.execute(setting_stmt)

    return WalletCardSettings(**json.loads(settings))


@router.patch("", response_model=WalletCardSettings)
async def update_apple_wallet_card_settings(token: str, settings: WalletCardSettings):
    # Найти пользователя по токену
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    # Подготовить обновлённые данные
    update_data = WalletCardSettingsUpdate(
        cashbox_id=user.cashbox_id, data=settings.dict(exclude_unset=True)
    ).dict()

    # Обновить настройки в БД
    settings_stmt = (
        apple_wallet_card_settings.update()
        .values(**update_data)
        .where(apple_wallet_card_settings.c.cashbox_id == user.cashbox_id)
        .returning(apple_wallet_card_settings.c.data)
    )
    updated_data = await database.fetch_one(settings_stmt)

    # Получить все карты пользователя
    cards_query = select(loyality_cards.c.id).where(
        loyality_cards.c.cashbox_id == user.cashbox_id
    )
    cards = [row.id for row in await database.fetch_all(cards_query)]

    # Опубликовать обновление пассов
    await publish_apple_wallet_pass_update(cards)

    # Вернуть обновлённые настройки
    return WalletCardSettings(**json.loads(updated_data.data))
