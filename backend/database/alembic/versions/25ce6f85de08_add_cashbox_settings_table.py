"""add cashbox_settings table

Revision ID: 25ce6f85de08
Revises: 7f34773f5e73
Create Date: 2025-08-09 13:22:41.735735

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "25ce6f85de08"
down_revision = "7f34773f5e73"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cashbox_settings",
        sa.Column("cashbox_id", sa.Integer(), nullable=False),
        sa.Column(
            "require_photo_for_writeoff",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
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
        sa.Column(
            "is_deleted", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.ForeignKeyConstraint(["cashbox_id"], ["cashboxes.id"]),
    )

    op.execute(
        """
        INSERT INTO cashbox_settings (cashbox_id, require_photo_for_writeoff, created_at, updated_at, is_deleted)
        SELECT id, false, now(), now(), false
        FROM cashboxes
    """
    )


def downgrade() -> None:
    op.drop_table("cashbox_settings")
