import os
from datetime import datetime

from api.apple_wallet.schemas import DeviceRegistration, SerialNumbersList
from common.apple_wallet_service.impl.WalletNotificationService import (
    WalletNotificationService,
)
from common.apple_wallet_service.impl.WalletPassService import (
    WalletPassGeneratorService,
)
from database.db import (
    apple_push_tokens,
    database,
    loyality_cards,
    users_cboxes_relation,
)
from fastapi import APIRouter, HTTPException, Request, Response
from sqlalchemy import String, cast, select

router = APIRouter(tags=["apple-wallet"])

# serial_number = card_id. Помни это


@router.post("/create_apple_wallet_card")
async def create_apple_wallet_card(token: str, card_id: int):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    pass_generator = WalletPassGeneratorService()
    s3_key, filename = await pass_generator.update_pass(card_id)

    # Получаем файл из S3
    pkpass_bytes = await pass_generator.get_pkpass_from_s3(
        s3_key.split("/")[-1].replace(".pkpass", "")
    )

    return Response(
        content=pkpass_bytes,
        media_type="application/vnd.apple.pkpass",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/version")
async def get_version():
    """Эндпоинт для проверки версии веб-сервиса (обязательный)."""
    return {"version": 1}


@router.post(
    "/v1/devices/{device_library_identifier}/registrations/{pass_type_identifier}/{serial_number}"
)
async def register_device(
    device_library_identifier: str,
    pass_type_identifier: str,
    serial_number: str,
    registration: DeviceRegistration,
):
    """
    Регистрирует устройство для получения обновлений пасса.
    Apple вызывает этот эндпоинт, когда пользователь добавляет пасс в Wallet.
    """
    serial_number = int(serial_number)
    # Нормализуем токен (удаляем пробелы и приводим к нижнему регистру, если нужно)
    clean_token = (
        registration.pushToken.replace(" ", "").replace("<", "").replace(">", "")
    )

    is_token_exist_query = apple_push_tokens.select().where(
        apple_push_tokens.c.device_library_identifier == device_library_identifier,
        apple_push_tokens.c.pass_type_id == pass_type_identifier,
        apple_push_tokens.c.serial_number == str(serial_number),
        apple_push_tokens.c.push_token == clean_token,
    )
    is_token_exist = await database.fetch_one(is_token_exist_query)

    if not is_token_exist:
        token_query = apple_push_tokens.insert().values(
            {
                "device_library_identifier": device_library_identifier,
                "pass_type_id": pass_type_identifier,
                "serial_number": str(serial_number),
                "push_token": clean_token,
                "card_id": serial_number,
            }
        )

        await database.execute(token_query)
        return {"status": "success"}, 201

    return {"status": "already_registered"}, 200


@router.delete(
    "/v1/devices/{device_library_identifier}/registrations/{pass_type_identifier}/{serial_number}"
)
async def unregister_device(
    device_library_identifier: str, pass_type_identifier: str, serial_number: str
):
    """
    Отменяет регистрацию устройства для пасса.
    Apple вызывает этот эндпоинт, когда пользователь удаляет пасс из Wallet.
    """
    is_pass_exist = apple_push_tokens.select().where(
        apple_push_tokens.c.device_library_identifier == device_library_identifier,
        apple_push_tokens.c.pass_type_id == pass_type_identifier,
        apple_push_tokens.c.serial_number == serial_number,
    )
    is_pass_exist = await database.fetch_one(is_pass_exist)
    if is_pass_exist:
        token_query = apple_push_tokens.delete().where(
            apple_push_tokens.c.device_library_identifier == device_library_identifier,
            apple_push_tokens.c.pass_type_id == pass_type_identifier,
            apple_push_tokens.c.serial_number == serial_number,
        )
        await database.execute(token_query)
        return {"status": "success"}, 200
    return Response(status_code=401)


@router.get("/v1/passes/{pass_type_identifier}/{serial_number}")
async def get_pass(pass_type_identifier: str, serial_number: str, request: Request):
    """
    Возвращает последнюю версию пасса.
    Устройство вызывает этот эндпоинт после получения push-уведомления.
    """
    pass_service = WalletPassGeneratorService()

    # # Проверяем существование файла в S3
    # exists = await pass_service.pkpass_exists_in_s3(serial_number)
    #
    # if not exists:
    await pass_service.update_pass(int(serial_number))

    # Получаем файл из S3
    pkpass_bytes = await pass_service.get_pkpass_from_s3(serial_number)
    filename = f"{serial_number}.pkpass"

    return Response(
        content=pkpass_bytes,
        media_type="application/vnd.apple.pkpass",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/v1/devices/{device_library_identifier}/registrations/{pass_type_identifier}",
    response_model=SerialNumbersList,
)
async def get_passes_for_device(
    device_library_identifier: str,
    pass_type_identifier: str,
    passesUpdatedSince: str = None,
):
    """
    Возвращает список серийных номеров пассов, которые были обновлены.
    Устройство может вызывать этот эндпоинт для проверки обновлений.
    """
    query = (
        select(loyality_cards.c.id)
        .select_from(
            loyality_cards.join(
                apple_push_tokens,
                apple_push_tokens.c.serial_number == cast(loyality_cards.c.id, String),
            )
        )
        .where(
            apple_push_tokens.c.device_library_identifier == device_library_identifier,
            apple_push_tokens.c.pass_type_id == pass_type_identifier,
        )
    )

    if passesUpdatedSince is not None:
        query = query.where(
            loyality_cards.c.updated_at >= datetime.fromisoformat(passesUpdatedSince)
        )

    res = [i.id for i in await database.fetch_all(query)]

    return SerialNumbersList(
        serialNumbers=list(map(str, res)), lastUpdated=datetime.now().isoformat()
    )


@router.post("/ask_update_pass")
async def renew_pass(token: str, card_id: int):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    service = WalletNotificationService()
    await service.ask_update_pass(card_id)

    return Response(status_code=200)


@router.get("/link_to_card")
async def link_to_card(token: str, card_id: int):
    user_query = users_cboxes_relation.select().where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(user_query)

    if not user:
        raise HTTPException(status_code=401, detail="Неверный токен")

    return (
        f'https://{os.getenv("APP_URL")}/get_apple_wallet_card/?card_number={card_id}'
    )
