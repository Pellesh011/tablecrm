"""add_nomenclature_attributes

Revision ID: 38093d9b0377
Revises: 99a5e266349c
Create Date: 2025-03-19 23:45:48.976087

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "38093d9b0377"
down_revision = "99a5e266349c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nomenclature_attributes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("alias", sa.String(), nullable=True),
        sa.Column("cashbox", sa.Integer(), nullable=False),
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
            ["cashbox"],
            ["cashboxes.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "name", "cashbox", name="uq_nomenclature_attributes_name_cashbox"
        ),
    )
    op.create_table(
        "nomenclature_attributes_value",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("attribute_id", sa.Integer(), nullable=False),
        sa.Column("nomenclature_id", sa.Integer(), nullable=False),
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
        sa.Column("value", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(
            ["attribute_id"],
            ["nomenclature_attributes.id"],
        ),
        sa.ForeignKeyConstraint(
            ["nomenclature_id"],
            ["nomenclature.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "attribute_id",
            "nomenclature_id",
            name="uq_nomenclature_attributes_value_attribute_id_nomenclature_id",
        ),
    )


def downgrade() -> None:
    op.drop_table("nomenclature_attributes_value")
    op.drop_table("nomenclature_attributes")
