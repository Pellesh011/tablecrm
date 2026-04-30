"""add tech-cards & tech-operations & tech_operation_payments & tech_operation_components & tech-card-items

Revision ID: bb88229e9ffd
Revises: eb6139bd2209
Create Date: 2025-12-02 14:10:41.304447

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "bb88229e9ffd"
down_revision = "eb6139bd2209"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # tech_cards
    if "tech_cards" not in inspector.get_table_names():
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
                sa.Enum(
                    "active", "canceled", "deleted", name="status", create_type=False
                ),
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
            sa.ForeignKeyConstraint(["user_id"], ["relation_tg_cashboxes.id"]),
            sa.PrimaryKeyConstraint("id"),
        )

    # tech_card_items
    if "tech_card_items" not in inspector.get_table_names():
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
            sa.ForeignKeyConstraint(["nomenclature_id"], ["nomenclature.id"]),
            sa.ForeignKeyConstraint(["tech_card_id"], ["tech_cards.id"]),
            sa.PrimaryKeyConstraint("id"),
        )

    # tech_operations
    if "tech_operations" not in inspector.get_table_names():
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
                sa.Enum(
                    "active", "canceled", "deleted", name="status", create_type=False
                ),
                nullable=True,
            ),
            sa.Column(
                "production_order_id", postgresql.UUID(as_uuid=True), nullable=True
            ),
            sa.Column(
                "consumption_order_id", postgresql.UUID(as_uuid=True), nullable=True
            ),
            sa.ForeignKeyConstraint(["nomenclature_id"], ["nomenclature.id"]),
            sa.ForeignKeyConstraint(["tech_card_id"], ["tech_cards.id"]),
            sa.ForeignKeyConstraint(["user_id"], ["relation_tg_cashboxes.id"]),
            sa.PrimaryKeyConstraint("id"),
        )

    # tech_operation_components
    if "tech_operation_components" not in inspector.get_table_names():
        op.create_table(
            "tech_operation_components",
            sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("nomeclature_id", sa.Integer(), nullable=True),
            sa.Column("name", sa.String(length=255), nullable=False),
            sa.Column("quantity", sa.Float(), nullable=False),
            sa.Column("gross_weight", sa.Float(), nullable=True),
            sa.Column("net_weight", sa.Float(), nullable=True),
            sa.ForeignKeyConstraint(["operation_id"], ["tech_operations.id"]),
            sa.PrimaryKeyConstraint("id"),
        )

    # tech_operation_payments
    if "tech_operation_payments" not in inspector.get_table_names():
        op.create_table(
            "tech_operation_payments",
            sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.ForeignKeyConstraint(["operation_id"], ["tech_operations.id"]),
            sa.PrimaryKeyConstraint("id"),
        )


def downgrade() -> None:
    op.drop_table("tech_operation_payments")
    op.drop_table("tech_operation_components")
    op.drop_table("tech_operations")
    op.drop_table("tech_card_items")
    op.drop_table("tech_cards")
    # Удаляем ENUM-ы, если они есть
    for enum_name in ["card_type", "status"]:
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
