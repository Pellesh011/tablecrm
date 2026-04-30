"""add nomenclature duplicates settings

Revision ID: add_cashbox_settings_nomenclature_duplicates
Revises: optimize_segments_003
Create Date: 2026-02-01 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_cashbox_settings_nomenclature_duplicates"
down_revision = "optimize_segments_003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "cashbox_settings",
        sa.Column(
            "check_nomenclature_duplicates_by_name",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.add_column(
        "cashbox_settings",
        sa.Column(
            "check_nomenclature_duplicates_by_code",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("cashbox_settings", "check_nomenclature_duplicates_by_code")
    op.drop_column("cashbox_settings", "check_nomenclature_duplicates_by_name")
