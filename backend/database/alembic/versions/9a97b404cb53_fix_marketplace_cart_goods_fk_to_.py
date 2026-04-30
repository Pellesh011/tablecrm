"""Fix marketplace_cart_goods FK to marketplace_carts

Revision ID: 9a97b404cb53
Revises: 56a291842ddc
Create Date: 2026-01-19 10:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9a97b404cb53"
down_revision = "56a291842ddc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Исправляем foreign key в marketplace_cart_goods: с marketplace_contragent_cart на marketplace_carts"""

    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # Проверяем текущий foreign key
    cart_goods_fks = inspector.get_foreign_keys("marketplace_cart_goods")
    for fk in cart_goods_fks:
        if "cart_id" in fk.get("constrained_columns", []):
            referred_table = fk.get("referred_table", "")
            if "marketplace_contragent_cart" in str(referred_table):
                # Удаляем старый foreign key
                op.drop_constraint(
                    fk["name"], "marketplace_cart_goods", type_="foreignkey"
                )
                # Создаем новый foreign key на marketplace_carts
                op.create_foreign_key(
                    "marketplace_cart_goods_cart_id_fkey",
                    "marketplace_cart_goods",
                    "marketplace_carts",
                    ["cart_id"],
                    ["id"],
                    ondelete="CASCADE",
                )
                break


def downgrade() -> None:
    """Откатываем изменения: возвращаем foreign key на marketplace_contragent_cart"""

    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # Проверяем текущий foreign key
    cart_goods_fks = inspector.get_foreign_keys("marketplace_cart_goods")
    for fk in cart_goods_fks:
        if "cart_id" in fk.get("constrained_columns", []):
            referred_table = fk.get("referred_table", "")
            if "marketplace_carts" in str(referred_table):
                # Удаляем новый foreign key
                op.drop_constraint(
                    fk["name"], "marketplace_cart_goods", type_="foreignkey"
                )
                # Восстанавливаем старый foreign key на marketplace_contragent_cart
                op.create_foreign_key(
                    "marketplace_cart_goods_cart_id_fkey",
                    "marketplace_cart_goods",
                    "marketplace_contragent_cart",
                    ["cart_id"],
                    ["id"],
                    ondelete="CASCADE",
                )
                break
