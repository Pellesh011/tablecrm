"""add tariff_paid_status and tariff_last_check_at to channel_credentials

Revision ID: add_tariff_cc
Revises: 67138537de70
Create Date: 2026-01-30

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "add_tariff_cc"
down_revision = "67138537de70"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "tariff_paid_status" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column("tariff_paid_status", sa.Boolean(), nullable=True),
        )

    if "tariff_last_check_at" not in columns:
        op.add_column(
            "channel_credentials",
            sa.Column("tariff_last_check_at", sa.DateTime(), nullable=True),
        )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("channel_credentials")]

    if "tariff_last_check_at" in columns:
        op.drop_column("channel_credentials", "tariff_last_check_at")

    if "tariff_paid_status" in columns:
        op.drop_column("channel_credentials", "tariff_paid_status")
