from database.db import SegmentObjectType, database, segment_objects, segments
from segments.constants import SegmentChangeType
from sqlalchemy import and_, or_, select


async def collect_objects(segment_id, obj_type: SegmentObjectType, mode: str):
    """
    Получение списка id объектов в сегменте.
    """

    base_condition = [
        segment_objects.c.segment_id == segment_id,
        segment_objects.c.object_type == obj_type,
    ]

    if mode == SegmentChangeType.new.value:
        base_condition.append(
            or_(
                segments.c.updated_at.is_(None),
                segment_objects.c.valid_from >= segments.c.updated_at,
            )
        )

    elif mode == SegmentChangeType.active.value:
        base_condition.append(segment_objects.c.valid_to.is_(None))

    elif mode == SegmentChangeType.removed.value:
        base_condition.append(segment_objects.c.valid_to.isnot(None))
        base_condition.append(segment_objects.c.valid_to >= segments.c.updated_at)

    query = (
        select(segment_objects.c.object_id)
        .join(segments, segment_objects.c.segment_id == segments.c.id)
        .where(and_(*base_condition))
    )
    rows = await database.fetch_all(query)
    obj_ids = [row["object_id"] for row in rows]
    return obj_ids
