"""added tags for segments

Revision ID: 6dcd82b23035
Revises: ded27e5f734b
Create Date: 2025-08-13 20:24:07.393189

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6dcd82b23035"
down_revision = "ded27e5f734b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "segments_tags",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("tag_id", sa.Integer(), nullable=False),
        sa.Column("segment_id", sa.Integer(), nullable=False),
        sa.Column("cashbox_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(["cashbox_id"], ["cashboxes.id"]),
        sa.ForeignKeyConstraint(["segment_id"], ["segments.id"]),
        sa.ForeignKeyConstraint(["tag_id"], ["tags.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tag_id", "segment_id", name="unique_tag_id_segment_id"),
    )


def downgrade() -> None:
    op.drop_table("segments_tags")
