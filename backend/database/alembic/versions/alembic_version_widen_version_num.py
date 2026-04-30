"""Widen alembic_version.version_num column

Revision ID: alembic_version_widen
Revises: fix_payboxes_deleted_at_safe
Create Date: 2026-01-18 01:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "alembic_version_widen"
down_revision = "fix_payboxes_deleted_at_safe"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Increase length of alembic_version.version_num to allow long revision ids."""

    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=32),
        type_=sa.String(length=128),
        existing_nullable=False,
    )


def downgrade() -> None:
    """Shrink version_num length back to 32 characters.

    Откат безопасен, так как все текущие revision id короче 32 символов
    (включая alembic_version_widen), а более длинные мы не будем
    использовать после применения этой миграции.
    """

    op.alter_column(
        "alembic_version",
        "version_num",
        existing_type=sa.String(length=128),
        type_=sa.String(length=32),
        existing_nullable=False,
    )
