from database.db import database, nomenclature
from fastapi import APIRouter, HTTPException, status
from functions.helpers import get_user_by_token
from sqlalchemy import and_, select

from .models import nomenclature_videos
from .schemas import (
    NomenclatureVideoCreate,
    NomenclatureVideoList,
    NomenclatureVideoPatch,
    NomenclatureVideoRead,
)

router = APIRouter(tags=["nomenclature_videos"])


async def _check_nomenclature_access(nomenclature_id: int, cashbox_id: int) -> None:
    """Проверяет, что номенклатура принадлежит кассе и не удалена."""
    row = await database.fetch_one(
        select(nomenclature.c.id).where(
            and_(
                nomenclature.c.id == nomenclature_id,
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
            )
        )
    )
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Номенклатура с ID {nomenclature_id} не найдена",
        )


async def _check_video_access(video_id: int, cashbox_id: int) -> dict:
    """Проверяет доступ к видео через номенклатуру и возвращает запись."""
    row = await database.fetch_one(
        select(nomenclature_videos)
        .select_from(
            nomenclature_videos.join(
                nomenclature,
                nomenclature_videos.c.nomenclature_id == nomenclature.c.id,
            )
        )
        .where(
            and_(
                nomenclature_videos.c.id == video_id,
                nomenclature.c.cashbox == cashbox_id,
                nomenclature.c.is_deleted.is_not(True),
            )
        )
    )
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Видео с ID {video_id} не найдено",
        )
    return dict(row)


@router.get(
    "/nomenclature/{nomenclature_id}/videos",
    response_model=NomenclatureVideoList,
    summary="Получить список видео номенклатуры",
)
async def get_nomenclature_videos(nomenclature_id: int, token: str):
    user = await get_user_by_token(token)
    await _check_nomenclature_access(nomenclature_id, user.cashbox_id)

    rows = await database.fetch_all(
        select(nomenclature_videos)
        .where(nomenclature_videos.c.nomenclature_id == nomenclature_id)
        .order_by(nomenclature_videos.c.id.asc())
    )
    items = [NomenclatureVideoRead(**dict(r)) for r in rows]
    return NomenclatureVideoList(result=items, count=len(items))


@router.post(
    "/nomenclature/{nomenclature_id}/videos",
    response_model=NomenclatureVideoRead,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить видео к номенклатуре",
)
async def create_nomenclature_video(
    nomenclature_id: int,
    token: str,
    body: NomenclatureVideoCreate,
):
    user = await get_user_by_token(token)
    await _check_nomenclature_access(nomenclature_id, user.cashbox_id)

    new_id = await database.execute(
        nomenclature_videos.insert().values(
            nomenclature_id=nomenclature_id,
            url=body.url,
            description=body.description,
            tags=body.tags or [],
        )
    )

    row = await database.fetch_one(
        select(nomenclature_videos).where(nomenclature_videos.c.id == new_id)
    )
    return NomenclatureVideoRead(**dict(row))


@router.patch(
    "/nomenclature/videos/{video_id}",
    response_model=NomenclatureVideoRead,
    summary="Обновить видео номенклатуры",
)
async def patch_nomenclature_video(
    video_id: int,
    token: str,
    body: NomenclatureVideoPatch,
):
    user = await get_user_by_token(token)
    await _check_video_access(video_id, user.cashbox_id)

    update_data = body.dict(exclude_none=True)
    if not update_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Нет полей для обновления",
        )

    await database.execute(
        nomenclature_videos.update()
        .where(nomenclature_videos.c.id == video_id)
        .values(**update_data)
    )

    row = await database.fetch_one(
        select(nomenclature_videos).where(nomenclature_videos.c.id == video_id)
    )
    return NomenclatureVideoRead(**dict(row))


@router.delete(
    "/nomenclature/videos/{video_id}",
    status_code=status.HTTP_200_OK,
    summary="Удалить видео номенклатуры",
)
async def delete_nomenclature_video(video_id: int, token: str):
    user = await get_user_by_token(token)
    await _check_video_access(video_id, user.cashbox_id)

    await database.execute(
        nomenclature_videos.delete().where(nomenclature_videos.c.id == video_id)
    )
    return {"detail": "Видео удалено", "id": video_id}
