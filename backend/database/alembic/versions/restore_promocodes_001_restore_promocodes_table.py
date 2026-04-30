"""restore_promocodes_table

Revision ID: restore_promocodes_001
Revises: 76d4216e5d87
Create Date: 2026-02-07 00:36:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "restore_promocodes_001"
down_revision = "4a8f2f6c0c21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create promocodetype enum if it doesn't exist
    promocodetype = postgresql.ENUM(
        "ONE_TIME", "PERMANENT", name="promocodetype", create_type=False
    )
    promocodetype.create(op.get_bind(), checkfirst=True)

    # Create promocodes table
    op.create_table(
        "promocodes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(), nullable=False),
        sa.Column("points_amount", sa.Float(), nullable=False),
        sa.Column(
            "type",
            postgresql.ENUM(
                "ONE_TIME", "PERMANENT", name="promocodetype", create_type=False
            ),
            nullable=False,
        ),
        sa.Column("max_usages", sa.Integer(), nullable=True),
        sa.Column("current_usages", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("organization_id", sa.Integer(), nullable=False),
        sa.Column("distributor_id", sa.Integer(), nullable=True),
        sa.Column("creator_id", sa.Integer(), nullable=False),
        sa.Column("valid_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["creator_id"],
            ["tg_accounts.id"],
        ),
        sa.ForeignKeyConstraint(
            ["distributor_id"],
            ["contragents.id"],
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_promocodes_id"), "promocodes", ["id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_promocodes_id"), table_name="promocodes")
    op.drop_table("promocodes")
    sa.Enum(name="promocodetype").drop(op.get_bind(), checkfirst=True)
