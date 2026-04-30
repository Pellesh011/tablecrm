from api.docs_sales_utm_tags import schemas
from api.docs_sales_utm_tags.service import DocsSalesUTMTagsService
from fastapi import APIRouter, Depends
from starlette import status

router = APIRouter(tags=["docs_sales"])


@router.get("/docs_sales/{idx}/utm", response_model=schemas.UtmTag)
async def get_utm_tag(
    token: str, idx: int, service: DocsSalesUTMTagsService = Depends()
):
    return await service.get_utm_tag(token, idx)


@router.post("/docs_sales/{idx}/utm", response_model=schemas.UtmTag)
async def create_utm_tag(
    token: str,
    idx: int,
    utm_tags_data: schemas.CreateUTMTag,
    service: DocsSalesUTMTagsService = Depends(),
):
    return await service.create_utm_tag(token, idx, utm_tags_data)


@router.put("/docs_sales/{idx}/utm", response_model=schemas.UtmTag)
async def update_utm_tag(
    idx: int,
    token: str,
    data: schemas.CreateUTMTag,
    service: DocsSalesUTMTagsService = Depends(),
):
    return await service.update_utm_tag(token, idx, data)


@router.delete("/docs_sales/{idx}/utm", status_code=status.HTTP_204_NO_CONTENT)
async def delete_utm_tag(
    idx: int, token: str, service: DocsSalesUTMTagsService = Depends()
):
    return await service.delete_utm_tag(token, idx)
