"""add_nomenclature_groups

Revision ID: 854e54846d57
Revises: 38093d9b0377
Create Date: 2025-03-21 00:50:36.099846

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "854e54846d57"
down_revision = "38093d9b0377"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nomenclature_groups",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("cashbox", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["cashbox"],
            ["cashboxes.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "nomenclature_groups_value",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("nomenclature_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.Column("is_main", sa.Boolean(), server_default="false", nullable=False),
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
            ["group_id"],
            ["nomenclature_groups.id"],
        ),
        sa.ForeignKeyConstraint(
            ["nomenclature_id"],
            ["nomenclature.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "nomenclature_id", name="uq_nomenclature_groups_value_nomenclature_id"
        ),
    )
    op.create_index(
        "uq_nomenclature_groups_value_group_id_is_main",
        "nomenclature_groups_value",
        ["group_id"],
        unique=True,
        postgresql_where=sa.text("is_main IS TRUE"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_nomenclature_groups_value_group_id_is_main",
        table_name="nomenclature_groups_value",
        postgresql_where=sa.text("is_main IS TRUE"),
    )
    op.drop_table("nomenclature_groups_value")
    op.drop_table("nomenclature_groups")
