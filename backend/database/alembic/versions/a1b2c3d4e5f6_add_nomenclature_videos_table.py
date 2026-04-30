"""add_nomenclature_videos_table

Revision ID: a3b2c3d4e5f6
Revises: 920f89a5a654
Create Date: 2026-03-11 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a3b2c3d4e5f6"
down_revision = "920f89a5a654"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()

    if "nomenclature_videos" not in tables:
        op.create_table(
            "nomenclature_videos",
            sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
            sa.Column(
                "nomenclature_id",
                sa.Integer(),
                sa.ForeignKey("nomenclature.id", ondelete="CASCADE"),
                nullable=False,
                index=True,
            ),
            sa.Column("url", sa.String(), nullable=False),
            sa.Column("description", sa.String(), nullable=True),
            sa.Column(
                "tags",
                postgresql.ARRAY(sa.String()),
                nullable=True,
                server_default="{}",
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                onupdate=sa.func.now(),
            ),
        )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()

    if "nomenclature_videos" in tables:
        op.drop_table("nomenclature_videos")
