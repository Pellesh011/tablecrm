from api.pictures.routers import get_picture_link_by_id
from apps.booking.schemas import (
    Booking,
    BookingCreate,
    BookingCreateList,
    BookingEdit,
    BookingEditList,
    BookingFiltersList,
    BookingList,
    NomenclatureBookingCreate,
    NomenclatureBookingEdit,
)
from database.db import (
    booking,
    booking_events,
    booking_events_photo,
    booking_nomenclature,
    booking_tags,
    contragents,
    database,
    nomenclature,
    pictures,
    users,
)
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from functions.helpers import get_user_by_token
from sqlalchemy import and_, desc, func, or_, select, update
from ws_manager import manager

router = APIRouter(tags=["booking"])


async def create_filters_list(filters: BookingFiltersList):
    result = []
    result_join = []

    if filters.dict().get("title"):
        result.append(
            booking.c.title.ilike(f'%{filters.dict().get("title").strip().lower()}%')
        )

    if filters.dict().get("contragent"):
        result.append(booking.c.contragent == filters.dict().get("contragent"))

    if filters.dict().get("start_booking") and filters.dict().get("end_booking"):
        result.append(
            and_(
                booking.c.start_booking >= filters.dict().get("start_booking"),
                booking.c.end_booking <= filters.dict().get("end_booking"),
            )
        )
    elif filters.dict().get("start_booking") and not filters.dict().get("end_booking"):
        result.append(booking.c.start_booking >= filters.dict().get("start_booking"))
    elif filters.dict().get("end_booking") and not filters.dict().get("start_booking"):
        result.append(booking.c.end_booking <= filters.dict().get("end_booking"))

    if filters.dict().get("status_doc_sales"):
        result.append(
            booking.c.status_doc_sales == filters.dict().get("status_doc_sales")
        )

    if filters.dict().get("status_booking"):
        result.append(booking.c.status_booking == filters.dict().get("status_booking"))

    if filters.dict().get("status_booking") and filters.dict().get("status_doc_sales"):
        result.append(
            or_(
                booking.c.status_booking == filters.dict().get("status_booking"),
                booking.c.status_doc_sales == filters.dict().get("status_doc_sales"),
            )
        )

    if filters.dict().get("status_booking") and not filters.dict().get(
        "status_doc_sales"
    ):
        result.append(booking.c.status_booking == filters.dict().get("status_booking"))

    if not filters.dict().get("status_booking") and filters.dict().get(
        "status_doc_sales"
    ):
        result.append(
            booking.c.status_doc_sales == filters.dict().get("status_doc_sales")
        )

    if filters.dict().get("tags"):
        tags_list = filters.dict().get("tags").split(",")
        filters_query = []
        for tag in tags_list:
            filters_query.append(booking_tags.c.name == tag)
        result.append(or_(*filters_query))
        result_join.append((booking_tags, booking_tags.c.booking_id == booking.c.id))

    return result, result_join


@router.get("/booking/events/nomenclature/{idx}")
async def get_events_by_nomenclature(
    token: str, idx: int, limit: int = 5, offset: int = 0
):
    await get_user_by_token(token)

    query = (
        select(booking_events, users.c.first_name, users.c.last_name)
        .select_from(booking_events)
        .join(
            booking_nomenclature,
            booking_nomenclature.c.id == booking_events.c.booking_nomenclature_id,
        )
        .join(booking, booking.c.id == booking_nomenclature.c.booking_id)
        .select_from(booking)
        .join(users, users.c.id == booking.c.booking_driver_id)
        .where(booking_nomenclature.c.nomenclature_id == idx)
        .order_by(desc(booking_events.c.created_at))
    )
    events_list = await database.fetch_all(query.limit(limit).offset(offset))

    photo_event = await database.fetch_all(
        select(pictures.c.id, pictures.c.url, booking_events_photo.c.booking_event_id)
        .join(pictures, booking_events_photo.c.photo_id == pictures.c.id)
        .where(
            booking_events_photo.c.booking_event_id.in_(
                [event.id for event in events_list]
            )
        )
    )
    events_list_photo = []
    for event in events_list:
        driver = {
            "first_name": event.get("first_name"),
            "last_name": event.get("last_name"),
        }
        event = dict(event)
        del event["first_name"]
        del event["last_name"]
        events_list_photo.append(
            {
                "driver": driver,
                "photo": [
                    {
                        "id": photo.get("id"),
                        "name": photo.get("url").split("/")[1],
                        "url": (
                            await get_picture_link_by_id(photo.get("url").split("/")[1])
                        )
                        .get("data")
                        .get("url"),
                    }
                    for photo in photo_event
                    if photo.get("booking_event_id") == event.get("id")
                ],
                **event,
            }
        )
    total = await database.fetch_val(select(func.count()).select_from(query))
    return {"items": events_list_photo, "pageSize": limit, "total": total}


@router.get("/booking/list", response_model=BookingList)
async def get_list_booking(token: str, filters: BookingFiltersList = Depends()):
    filter_result, join_results = await create_filters_list(filters)
    user = await get_user_by_token(token)
    try:
        tags_subquery = (
            select(
                booking_tags.c.booking_id,
                func.coalesce(
                    func.array_agg(booking_tags.c.name).filter(
                        booking_tags.c.name.is_not(None)
                    ),
                    [],
                ).label("tags"),
            )
            .group_by(booking_tags.c.booking_id)
            .subquery()
        )

        query = (
            select(
                booking,
                contragents.c.name.label("contragent_name"),
                func.coalesce(tags_subquery.c.tags, []).label("tags"),
            )
            .select_from(booking)
            .outerjoin(
                contragents, booking.c.contragent == contragents.c.id
            )  # Присоединяем контрагента
            .outerjoin(
                tags_subquery, tags_subquery.c.booking_id == booking.c.id
            )  # Левое соединение с тегами
            .where(
                booking.c.cashbox == user.cashbox_id,
                booking.c.is_deleted.is_not(True),
                *filter_result,
            )
        )
        for join_result in join_results:
            query = query.join(join_result[0], join_result[1])

        list_db = await database.fetch_all(query)
        list_result = []
        for item in list(map(dict, list_db)):
            goods = await database.fetch_all(
                select(
                    booking_nomenclature.c.id,
                    booking_nomenclature.c.is_deleted,
                    booking_nomenclature.c.nomenclature_id,
                    booking_nomenclature.c.tariff,
                    nomenclature.c.name,
                    nomenclature.c.category,
                )
                .where(and_(booking_nomenclature.c.booking_id == item.get("id")))
                .select_from(booking_nomenclature)
                .join(
                    nomenclature,
                    nomenclature.c.id == booking_nomenclature.c.nomenclature_id,
                )
            )
            list_result.append({**item, "goods": list(map(dict, goods))})
        return list_result
    except Exception as e:
        raise HTTPException(status_code=432, detail=str(e))


@router.get("/booking/{idx}", response_model=Booking)
async def get_booking_by_idx(token: str, idx: int):
    user = await get_user_by_token(token)
    result = await database.fetch_one(
        booking.select().where(
            and_(
                booking.c.cashbox == user.cashbox_id,
                booking.c.id == idx,
                booking.c.is_deleted.is_not(True),
            )
        )
    )
    if result:
        goods = await database.fetch_all(
            select(
                booking_nomenclature.c.id,
                booking_nomenclature.c.is_deleted,
                booking_nomenclature.c.nomenclature_id,
                booking_nomenclature.c.tariff,
                nomenclature.c.name,
                nomenclature.c.category,
            )
            .where(and_(booking_nomenclature.c.booking_id == idx))
            .select_from(booking_nomenclature)
            .join(
                nomenclature,
                nomenclature.c.id == booking_nomenclature.c.nomenclature_id,
            )
        )
        dict_result = dict(result)
        dict_result["goods"] = list(map(dict, goods))
        return dict_result
    else:
        raise HTTPException(status_code=404, detail="not found")


@database.transaction()
@router.post(
    "/booking/create",
    status_code=201,
    # response_model = ResponseCreate,
    # responses={201: {"model": ResponseCreate}}
)
async def create_booking(token: str, bookings: BookingCreateList):
    user = await get_user_by_token(token)

    insert_booking_list = []
    prepare_booking_goods_list = []
    prepare_booking_tags_list = []
    exception_list = []
    request_id = 0

    try:
        for bookingItem in bookings.dict()["__root__"]:
            request_id += 1

            skip_iteration_outer = False

            good_info_list = []

            exception = {}

            for good_info in bookingItem.pop("goods"):
                query = select(nomenclature.c.id).where(
                    and_(
                        nomenclature.c.id == good_info["nomenclature_id"],
                        nomenclature.c.cashbox == user.get("cashbox_id"),
                    )
                )
                nomenclature_info = await database.fetch_one(query)

                if not nomenclature_info:
                    skip_iteration_outer = True
                    exception["request_id"] = request_id
                    exception["error"] = "Nomenclature not found"
                    break

                query = (
                    select(booking.c.id)
                    .join(
                        booking_nomenclature,
                        booking.c.id == booking_nomenclature.c.booking_id,
                    )
                    .where(
                        and_(
                            booking_nomenclature.c.nomenclature_id
                            == good_info["nomenclature_id"],
                            booking.c.cashbox == user.get("cashbox_id"),
                            booking.c.start_booking < bookingItem.get("end_booking"),
                            booking.c.end_booking > bookingItem.get("start_booking"),
                            booking.c.is_deleted == False,
                        )
                    )
                )
                booking_find = await database.fetch_one(query)

                if booking_find:
                    skip_iteration_outer = True
                    exception["request_id"] = request_id
                    exception["error"] = "Conflict booking date with another booking"
                    break

                good_info_list.append(
                    {
                        **good_info,
                        "is_deleted": False,
                    }
                )

            if skip_iteration_outer:
                exception_list.append(exception)
                continue

            tags = bookingItem.pop("tags")
            if tags:
                prepare_booking_tags_list.append(
                    [{"name": tag} for tag in tags.split(",")]
                )
            prepare_booking_goods_list.append(good_info_list)
            insert_booking_list.append(
                {**bookingItem, "cashbox": user.get("cashbox_id"), "is_deleted": False}
            )

        create_booking_ids = []
        if insert_booking_list:
            query = booking.insert().values(insert_booking_list).returning(booking.c.id)
            create_booking_ids = await database.fetch_all(query)

            booking_goods_insert = []
            booking_tags_insert = []

            for index, create_booking_id in enumerate(create_booking_ids):
                # была ошибка обращение к индексу который не существует массива prepare_booking_goods_list. Сделал проверку
                if len(prepare_booking_goods_list) > 0:
                    booking_goods = prepare_booking_goods_list[index]
                else:
                    booking_goods = []
                # была ошибка обращение к индексу который не существует prepare_booking_tags_list. Сделал проверку
                if len(prepare_booking_tags_list) > 0:
                    booking_tags_list = prepare_booking_tags_list[index]
                else:
                    booking_tags_list = []

                for booking_good_info in booking_goods:
                    booking_good_info["booking_id"] = create_booking_id.id
                    booking_goods_insert.append(booking_good_info)

                for booking_tag_info in booking_tags_list:
                    booking_tag_info["booking_id"] = create_booking_id.id
                    booking_tags_insert.append(booking_tag_info)

            if booking_goods_insert:
                await database.execute(
                    booking_nomenclature.insert().values(booking_goods_insert)
                )

            if booking_tags_insert:
                await database.execute(
                    booking_tags.insert().values(booking_tags_insert)
                )

        response = {
            "data": [
                BookingCreate(**booking_element, goods=good_elements)
                for booking_element, good_elements in zip(
                    insert_booking_list, prepare_booking_goods_list
                )
            ],
            "errors": exception_list,
        }
        await manager.send_message(
            token,
            {
                "action": "create",
                "target": "booking",
                "result": jsonable_encoder(response),
            },
        )
        return JSONResponse(status_code=201, content=jsonable_encoder(response))
    except Exception as e:
        raise HTTPException(status_code=432, detail=str(e))


@database.transaction()
@router.patch(
    "/booking/edit",
    description="""
              <p><div style="color:red">Важно!</div>
              <ul>
              <li>Если товары не редактируются то в запросе ключ goods не отправляется.</li>
              <li>Если отправить goods: [] - из бронирования будут удалены все товары товары.</li>
              <li>Добавление товаров без ID ведет к их добавлению в бронирование.</li>
              <li>Если отправить goods с изменным полем - произойдет изменение поля. Для этого в элементе goods
              указывается id и с ним те поля которые хотите изменить в бронировании товара</li>
              </ul>
              </p>

              """,
)
async def create_booking(token: str, bookings: BookingEditList):
    user = await get_user_by_token(token)

    try:
        bookings = bookings.dict(exclude_unset=True)
        for bookingItem in bookings["__root__"]:
            if not bookingItem.get("id"):
                raise Exception("не указан id бронирования")
            goods = None
            if bookingItem.get("goods"):
                goods = bookingItem.get("goods")
                del bookingItem["goods"]

            bookingItem_db = await database.fetch_one(
                booking.select().where(booking.c.id == bookingItem.get("id"))
            )
            if not bookingItem_db:
                raise Exception("не найден id бронирования")
            bookingItem_db_model = BookingEdit(**bookingItem_db)
            update_data = bookingItem
            updated_item = bookingItem_db_model.copy(update=update_data)
            await database.execute(
                booking.update()
                .where(booking.c.id == bookingItem_db.get("id"))
                .values(updated_item.dict())
            )
            if goods is not None:
                goods_db = await database.fetch_all(
                    booking_nomenclature.select().where(
                        booking_nomenclature.c.booking_id == bookingItem.get("id")
                    )
                )
                for good in goods:
                    if good.get("id"):
                        goodItem_db = await database.fetch_one(
                            booking_nomenclature.select().where(
                                booking_nomenclature.c.id == good.get("id")
                            )
                        )
                        goodItem_db_model = NomenclatureBookingEdit(**goodItem_db)
                        updated_goodItem = goodItem_db_model.copy(update=good)
                        await database.execute(
                            booking_nomenclature.update()
                            .where(booking_nomenclature.c.id == good.get("id"))
                            .values(updated_goodItem.dict())
                        )
                    else:
                        await database.execute(
                            booking_nomenclature.insert().values(
                                {
                                    **NomenclatureBookingCreate(**good).dict(),
                                    "booking_id": bookingItem.get("id"),
                                    "is_deleted": False,
                                }
                            )
                        )
                for good_db in goods_db:
                    if good_db.get("id") not in [good.get("id") for good in goods]:
                        print(good_db.get("id"), [good.get("id") for good in goods])
                        await database.execute(
                            booking_nomenclature.delete().where(
                                booking_nomenclature.c.id == good_db.get("id")
                            )
                        )
        response = {
            "status": "success updated",
            "data": [
                await get_booking_by_idx(token=token, idx=bookingItem.get("id"))
                for bookingItem in bookings["__root__"]
            ],
        }
        await manager.send_message(
            token,
            {
                "action": "edit",
                "target": "booking",
                "result": jsonable_encoder(response),
            },
        )
        return JSONResponse(status_code=200, content=jsonable_encoder(response))

    except Exception as e:
        raise HTTPException(status_code=432, detail=str(e))


@database.transaction()
@router.delete("/booking/{idx}")
async def delete_booking(token: str, idx: int):
    user = await get_user_by_token(token)
    booking_db = await database.fetch_one(
        select(booking).where(
            and_(booking.c.id == idx, booking.c.cashbox == user.get("cashbox_id"))
        )
    )
    query_delete = (
        update(booking)
        .where(and_(booking.c.id == idx, booking.c.cashbox == user.get("cashbox_id")))
        .values({"is_deleted": True})
        .returning(booking)
    )

    await database.execute(query_delete)
    return JSONResponse(
        status_code=200,
        content=jsonable_encoder(
            {
                "status": "success delete",
                "item": {**dict(booking_db), "is_deleted": True},
            }
        ),
    )
