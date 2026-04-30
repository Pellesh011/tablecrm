"""add tilda fields to feeds

Revision ID: add_tilda_fields
Revises: merge_heads_duplicates_76d4216e5d87
Create Date: 2026-02-01 21:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_tilda_fields"
down_revision = "merge_heads_duplicates_76d4216e5d87"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поля для синхронизации с Tilda
    op.add_column("feeds", sa.Column("tilda_url", sa.String(), nullable=True))
    op.add_column("feeds", sa.Column("tilda_username", sa.String(), nullable=True))
    op.add_column("feeds", sa.Column("tilda_password", sa.String(), nullable=True))
    op.add_column(
        "feeds",
        sa.Column(
            "tilda_sync_enabled", sa.Boolean(), nullable=False, server_default="false"
        ),
    )
    op.add_column(
        "feeds", sa.Column("tilda_sync_interval", sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("feeds", "tilda_sync_interval")
    op.drop_column("feeds", "tilda_sync_enabled")
    op.drop_column("feeds", "tilda_password")
    op.drop_column("feeds", "tilda_username")
    op.drop_column("feeds", "tilda_url")
