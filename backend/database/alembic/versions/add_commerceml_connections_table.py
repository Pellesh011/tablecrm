"""add_commerceml_connections_table

Revision ID: commerceml_connections_001
Revises: restore_promocodes_001
Create Date: 2025-02-13 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "commerceml_connections_001"
down_revision = (
    "4a8f2f6c0c21"  # Используем последнюю известную ревизию из restore_promocodes
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "commerceml_connections",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("cashbox_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.Column("username", sa.String(), nullable=False),
        sa.Column("password", sa.String(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column(
            "import_products", sa.Boolean(), nullable=False, server_default="true"
        ),
        sa.Column(
            "export_products", sa.Boolean(), nullable=False, server_default="true"
        ),
        sa.Column("import_orders", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("export_orders", sa.Boolean(), nullable=False, server_default="true"),
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
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default="false"),
        sa.ForeignKeyConstraint(
            ["cashbox_id"],
            ["cashboxes.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_commerceml_connections_id"),
        "commerceml_connections",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_commerceml_connections_cashbox_id"),
        "commerceml_connections",
        ["cashbox_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_commerceml_connections_cashbox_id"),
        table_name="commerceml_connections",
    )
    op.drop_index(
        op.f("ix_commerceml_connections_id"), table_name="commerceml_connections"
    )
    op.drop_table("commerceml_connections")
