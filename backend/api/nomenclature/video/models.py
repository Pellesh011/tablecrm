"""
api/nomenclature/video/models.py
"""

import sqlalchemy
from database.db import metadata
from sqlalchemy import (
    ARRAY,
    DateTime,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.sql import func

nomenclature_videos = sqlalchemy.Table(
    "nomenclature_videos",
    metadata,
    sqlalchemy.Column("id", Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column(
        "nomenclature_id",
        Integer,
        ForeignKey("nomenclature.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    ),
    sqlalchemy.Column("url", String, nullable=False),
    sqlalchemy.Column("description", String, nullable=True),
    sqlalchemy.Column(
        "tags",
        ARRAY(item_type=String),
        nullable=True,
        server_default="{}",
    ),
    sqlalchemy.Column(
        "created_at",
        DateTime(timezone=True),
        server_default=func.now(),
    ),
    sqlalchemy.Column(
        "updated_at",
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    ),
)
