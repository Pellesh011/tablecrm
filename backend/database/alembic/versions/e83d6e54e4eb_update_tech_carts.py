"""update_tech_carts

Revision ID: e83d6e54e4eb
Revises: 7ec45cb92dce
Create Date: 2026-03-26 04:36:47.681848

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "e83d6e54e4eb"
down_revision = "7ec45cb92dce"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tech_cards",
        sa.Column(
            "card_mode", sa.String(20), nullable=False, server_default="reference"
        ),
    )
    op.add_column(
        "tech_cards",
        sa.Column(
            "cashbox_id", sa.Integer, sa.ForeignKey("cashboxes.id"), nullable=True
        ),
    )
    op.add_column(
        "tech_cards",
        sa.Column(
            "warehouse_from_id",
            sa.Integer,
            sa.ForeignKey("warehouses.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "tech_cards",
        sa.Column(
            "warehouse_to_id", sa.Integer, sa.ForeignKey("warehouses.id"), nullable=True
        ),
    )

    op.add_column(
        "tech_operations",
        sa.Column(
            "production_doc_id",
            sa.Integer,
            sa.ForeignKey("docs_warehouse.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "tech_operations",
        sa.Column(
            "consumption_doc_id",
            sa.Integer,
            sa.ForeignKey("docs_warehouse.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "tech_operations",
        sa.Column(
            "docs_sales_id", sa.Integer, sa.ForeignKey("docs_sales.id"), nullable=True
        ),
    )
    op.add_column(
        "tech_operations",
        sa.Column(
            "cashbox_id", sa.Integer, sa.ForeignKey("cashboxes.id"), nullable=True
        ),
    )

    op.create_table(
        "tech_card_output_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "tech_card_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("tech_cards.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "nomenclature_id",
            sa.Integer,
            sa.ForeignKey("nomenclature.id"),
            nullable=False,
        ),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("unit_id", sa.Integer, sa.ForeignKey("units.id"), nullable=True),
        sa.Column("created_at", sa.DateTime, server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    )


def downgrade():
    op.drop_table("tech_card_output_items")
    op.drop_column("tech_operations", "status")
    op.drop_column("tech_operations", "cashbox_id")
    op.drop_column("tech_operations", "docs_sales_id")
    op.drop_column("tech_operations", "consumption_doc_id")
    op.drop_column("tech_operations", "production_doc_id")
    op.drop_column("tech_cards", "warehouse_to_id")
    op.drop_column("tech_cards", "warehouse_from_id")
    op.drop_column("tech_cards", "cashbox_id")
    op.drop_column("tech_cards", "card_mode")
