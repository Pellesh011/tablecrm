"""add_tech_cards

Revision ID: c9625a6c9c31
Revises: 3df9a9052a94
Create Date: 2025-09-09 14:26:26.463112

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "c9625a6c9c31"
down_revision = "3df9a9052a94"
branch_labels = None
depends_on = None

ENUM_NAMES = ["card_type", "status"]


def upgrade() -> None:
    # Удаляем ENUM-ы, если они есть
    for enum_name in ENUM_NAMES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_type WHERE typname = '{enum_name}') THEN
                    DROP TYPE {enum_name};
                END IF;
            END$$;
        """
        )

    op.create_table(
        "tech_cards",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=1000), nullable=True),
        sa.Column(
            "card_type",
            sa.Enum("reference", "automatic", name="card_type", create_type=False),
            nullable=False,
        ),
        sa.Column("auto_produce", sa.Boolean(), nullable=True),
        sa.Column("user_id", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("active", "canceled", "deleted", name="status", create_type=False),
            nullable=True,
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
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["relation_tg_cashboxes.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "tech_card_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tech_card_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("nomenclature_id", sa.Integer(), nullable=True),
        sa.Column("type_of_processing", sa.String(length=255), nullable=False),
        sa.Column("waste_from_cold_processing", sa.Float(), nullable=False),
        sa.Column("waste_from_heat_processing", sa.Float(), nullable=False),
        sa.Column("net_weight", sa.Float(), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("gross_weight", sa.Float(), nullable=False),
        sa.Column("output", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["nomenclature_id"],
            ["nomenclature.id"],
        ),
        sa.ForeignKeyConstraint(
            ["tech_card_id"],
            ["tech_cards.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "tech_operations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tech_card_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("output_quantity", sa.Float(), nullable=False),
        sa.Column("from_warehouse_id", sa.Integer(), nullable=False),
        sa.Column("to_warehouse_id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("nomenclature_id", sa.Integer(), nullable=True),
        sa.Column(
            "status",
            sa.Enum("active", "canceled", "deleted", name="status", create_type=False),
            nullable=True,
        ),
        sa.Column("production_order_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("consumption_order_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["nomenclature_id"],
            ["nomenclature.id"],
        ),
        sa.ForeignKeyConstraint(
            ["tech_card_id"],
            ["tech_cards.id"],
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["relation_tg_cashboxes.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "tech_operation_components",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("nomeclature_id", sa.Integer(), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("gross_weight", sa.Float(), nullable=True),
        sa.Column("net_weight", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["tech_operations.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "tech_operation_payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["tech_operations.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("tech_operation_payments")
    op.drop_table("tech_operation_components")
    op.drop_table("tech_operations")
    op.drop_table("tech_card_items")
    op.drop_table("tech_cards")
    # Удаляем ENUM-ы, если они есть
    for enum_name in ENUM_NAMES:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM pg_type WHERE typname = '{enum_name}') THEN
                    DROP TYPE {enum_name};
                END IF;
            END$$;
        """
        )
