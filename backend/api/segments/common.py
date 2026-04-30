import hashlib
import secrets
import time

from database.db import database, segments_tags, tags


def generate_unix_salt_md5() -> str:
    """unix_timestamp + salt + MD5"""
    unix_ts = str(int(time.time()))
    salt = secrets.token_hex(8)
    data = f"{unix_ts}{salt}".encode()
    md5_hash = hashlib.md5(data).hexdigest()

    return f"{unix_ts}{salt}{md5_hash}"


async def get_segments_tags(segment_id: int):
    tags_query = (
        tags.select()
        .join(segments_tags, tags.c.id == segments_tags.c.tag_id)
        .where(segments_tags.c.segment_id == segment_id)
    )
    tags_db = await database.fetch_all(tags_query)
    return tags_db
