"""create user permissions table

Revision ID: 202505310001
Revises:
Create Date: 2025-05-31 10:01:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a2bb93ae9762"
down_revision = "b8cb133ee445"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "user_permissions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column("section", sa.String(), nullable=False),
        sa.Column("can_view", sa.Boolean(), server_default="true", nullable=True),
        sa.Column("can_edit", sa.Boolean(), server_default="false", nullable=True),
        sa.Column("paybox_id", sa.Integer(), nullable=True),
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
        sa.PrimaryKeyConstraint("id"),
        sa.Index("ix_user_permissions_id", "id"),
    )


def downgrade():
    op.drop_table("user_permissions")
