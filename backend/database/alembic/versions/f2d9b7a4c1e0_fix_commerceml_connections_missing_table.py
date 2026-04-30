"""Fix missing commerceml_connections table

Revision ID: f2d9b7a4c1e0
Revises: merge_heads_20260216_raschet_docs_sales_tilda
Create Date: 2026-02-16

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "f2d9b7a4c1e0"
down_revision = "merge_heads_20260216_raschet_docs_sales_tilda"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    table_name = "commerceml_connections"

    if table_name not in inspector.get_table_names():
        op.create_table(
            table_name,
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
            sa.Column(
                "import_orders", sa.Boolean(), nullable=False, server_default="true"
            ),
            sa.Column(
                "export_orders", sa.Boolean(), nullable=False, server_default="true"
            ),
            sa.Column(
                "products_loaded_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
            sa.Column(
                "orders_exported_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
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
                "is_deleted", sa.Boolean(), nullable=False, server_default="false"
            ),
            sa.ForeignKeyConstraint(["cashbox_id"], ["cashboxes.id"]),
            sa.PrimaryKeyConstraint("id"),
        )

        op.create_index(
            op.f("ix_commerceml_connections_id"), table_name, ["id"], unique=False
        )
        op.create_index(
            op.f("ix_commerceml_connections_cashbox_id"),
            table_name,
            ["cashbox_id"],
            unique=False,
        )
        return

    existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
    if "products_loaded_count" not in existing_columns:
        op.add_column(
            table_name,
            sa.Column(
                "products_loaded_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
        )
    if "orders_exported_count" not in existing_columns:
        op.add_column(
            table_name,
            sa.Column(
                "orders_exported_count",
                sa.Integer(),
                nullable=False,
                server_default="0",
            ),
        )

    existing_indexes = {idx.get("name") for idx in inspector.get_indexes(table_name)}
    idx_id = op.f("ix_commerceml_connections_id")
    idx_cashbox = op.f("ix_commerceml_connections_cashbox_id")
    if idx_id not in existing_indexes:
        op.create_index(idx_id, table_name, ["id"], unique=False)
    if idx_cashbox not in existing_indexes:
        op.create_index(idx_cashbox, table_name, ["cashbox_id"], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    table_name = "commerceml_connections"

    if table_name not in inspector.get_table_names():
        return

    existing_indexes = {idx.get("name") for idx in inspector.get_indexes(table_name)}
    idx_id = op.f("ix_commerceml_connections_id")
    idx_cashbox = op.f("ix_commerceml_connections_cashbox_id")
    if idx_cashbox in existing_indexes:
        op.drop_index(idx_cashbox, table_name=table_name)
    if idx_id in existing_indexes:
        op.drop_index(idx_id, table_name=table_name)

    op.drop_table(table_name)
