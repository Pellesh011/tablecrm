"""Fix marketplace clients: add names and change FK from phone to id

Revision ID: fix_marketplace_clients_001
Revises: optimize_marketplace_002
Create Date: 2026-01-19 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fix_marketplace_clients_001"
down_revision = "optimize_marketplace_002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add first_name and last_name to marketplace_clients_list, change FK from phone to id"""

    # 1. Добавляем поля first_name, last_name и UTM метки в marketplace_clients_list
    op.add_column(
        "marketplace_clients_list",
        sa.Column("first_name", sa.String(), nullable=True),
    )
    op.add_column(
        "marketplace_clients_list",
        sa.Column("last_name", sa.String(), nullable=True),
    )
    op.add_column(
        "marketplace_clients_list",
        sa.Column("utm_source", sa.String(), nullable=True),
    )
    op.add_column(
        "marketplace_clients_list",
        sa.Column("utm_medium", sa.String(), nullable=True),
    )
    op.add_column(
        "marketplace_clients_list",
        sa.Column("utm_campaign", sa.String(), nullable=True),
    )
    op.add_column(
        "marketplace_clients_list",
        sa.Column("referer_id", sa.String(), nullable=True),
    )

    # 2. Добавляем колонку client_id в связанные таблицы
    # marketplace_carts
    op.add_column(
        "marketplace_carts",
        sa.Column("client_id", sa.BigInteger(), nullable=True),
    )

    # marketplace_favorites
    op.add_column(
        "marketplace_favorites",
        sa.Column("client_id", sa.BigInteger(), nullable=True),
    )

    # marketplace_orders
    op.add_column(
        "marketplace_orders",
        sa.Column("client_id", sa.BigInteger(), nullable=True),
    )

    # marketplace_searches
    op.add_column(
        "marketplace_searches",
        sa.Column("client_id", sa.BigInteger(), nullable=True),
    )

    # 3. Заполняем client_id данными из marketplace_clients_list по phone
    conn = op.get_bind()

    # marketplace_carts
    conn.execute(
        sa.text(
            """
        UPDATE marketplace_carts mc
        SET client_id = mcl.id
        FROM marketplace_clients_list mcl
        WHERE mc.phone = mcl.phone
    """
        )
    )

    # marketplace_favorites
    conn.execute(
        sa.text(
            """
        UPDATE marketplace_favorites mf
        SET client_id = mcl.id
        FROM marketplace_clients_list mcl
        WHERE mf.phone = mcl.phone
    """
        )
    )

    # marketplace_orders
    conn.execute(
        sa.text(
            """
        UPDATE marketplace_orders mo
        SET client_id = mcl.id
        FROM marketplace_clients_list mcl
        WHERE mo.phone = mcl.phone
    """
        )
    )

    # marketplace_searches (phone может быть NULL)
    conn.execute(
        sa.text(
            """
        UPDATE marketplace_searches ms
        SET client_id = mcl.id
        FROM marketplace_clients_list mcl
        WHERE ms.phone = mcl.phone AND ms.phone IS NOT NULL
    """
        )
    )

    # 4. Удаляем старые foreign key constraints на phone (если они существуют)
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    # marketplace_carts
    fk_name = "marketplace_carts_phone_fkey"
    if any(
        c["name"] == fk_name for c in inspector.get_foreign_keys("marketplace_carts")
    ):
        op.drop_constraint(fk_name, "marketplace_carts", type_="foreignkey")

    # marketplace_favorites
    fk_name = "marketplace_favorites_phone_fkey"
    if any(
        c["name"] == fk_name
        for c in inspector.get_foreign_keys("marketplace_favorites")
    ):
        op.drop_constraint(fk_name, "marketplace_favorites", type_="foreignkey")

    # marketplace_orders (может не существовать)
    fk_name = "marketplace_orders_phone_fkey"
    if any(
        c["name"] == fk_name for c in inspector.get_foreign_keys("marketplace_orders")
    ):
        op.drop_constraint(fk_name, "marketplace_orders", type_="foreignkey")

    # marketplace_searches
    fk_name = "marketplace_searches_phone_fkey"
    if any(
        c["name"] == fk_name for c in inspector.get_foreign_keys("marketplace_searches")
    ):
        op.drop_constraint(fk_name, "marketplace_searches", type_="foreignkey")

    # 5. Создаем новые foreign key constraints на client_id -> id
    # marketplace_carts
    op.create_foreign_key(
        "marketplace_carts_client_id_fkey",
        "marketplace_carts",
        "marketplace_clients_list",
        ["client_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )

    # marketplace_favorites
    op.create_foreign_key(
        "marketplace_favorites_client_id_fkey",
        "marketplace_favorites",
        "marketplace_clients_list",
        ["client_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )

    # marketplace_orders
    op.create_foreign_key(
        "marketplace_orders_client_id_fkey",
        "marketplace_orders",
        "marketplace_clients_list",
        ["client_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="RESTRICT",
    )

    # marketplace_searches
    op.create_foreign_key(
        "marketplace_searches_client_id_fkey",
        "marketplace_searches",
        "marketplace_clients_list",
        ["client_id"],
        ["id"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )

    # 6. Создаем индексы на client_id
    op.create_index(
        "ix_marketplace_carts_client_id",
        "marketplace_carts",
        ["client_id"],
        unique=False,
    )
    op.create_index(
        "ix_marketplace_favorites_client_id",
        "marketplace_favorites",
        ["client_id"],
        unique=False,
    )
    op.create_index(
        "ix_marketplace_orders_client_id",
        "marketplace_orders",
        ["client_id"],
        unique=False,
    )
    op.create_index(
        "ix_marketplace_searches_client_id",
        "marketplace_searches",
        ["client_id"],
        unique=False,
    )

    # 7. Удаляем записи без соответствующего клиента (если такие есть)
    conn.execute(sa.text("DELETE FROM marketplace_carts WHERE client_id IS NULL"))
    conn.execute(sa.text("DELETE FROM marketplace_favorites WHERE client_id IS NULL"))
    conn.execute(sa.text("DELETE FROM marketplace_orders WHERE client_id IS NULL"))
    # marketplace_searches не удаляем, так как phone там может быть NULL

    # 8. Делаем client_id NOT NULL (после заполнения данных и очистки)
    op.alter_column("marketplace_carts", "client_id", nullable=False)
    op.alter_column("marketplace_favorites", "client_id", nullable=False)
    op.alter_column("marketplace_orders", "client_id", nullable=False)
    # marketplace_searches.client_id остается nullable (так как в оригинале phone был nullable)

    # 9. Делаем phone nullable в связанных таблицах (для обратной совместимости, но не используем)
    op.alter_column("marketplace_carts", "phone", nullable=True)
    op.alter_column("marketplace_favorites", "phone", nullable=True)
    op.alter_column("marketplace_orders", "phone", nullable=True)
    # marketplace_searches.phone уже был nullable

    # 10. Исправляем foreign key в marketplace_cart_goods (если он ссылается на старую таблицу)
    # Проверяем, существует ли constraint на старую таблицу
    inspector = sa.inspect(conn)
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
    """Revert changes"""

    # Восстанавливаем phone как NOT NULL
    op.alter_column("marketplace_orders", "phone", nullable=False)
    op.alter_column("marketplace_favorites", "phone", nullable=False)
    op.alter_column("marketplace_carts", "phone", nullable=False)

    # Делаем client_id nullable
    op.alter_column("marketplace_orders", "client_id", nullable=True)
    op.alter_column("marketplace_favorites", "client_id", nullable=True)
    op.alter_column("marketplace_carts", "client_id", nullable=True)

    # Удаляем индексы на client_id
    op.drop_index("ix_marketplace_searches_client_id", "marketplace_searches")
    op.drop_index("ix_marketplace_orders_client_id", "marketplace_orders")
    op.drop_index("ix_marketplace_favorites_client_id", "marketplace_favorites")
    op.drop_index("ix_marketplace_carts_client_id", "marketplace_carts")

    # Удаляем новые foreign key constraints
    op.drop_constraint(
        "marketplace_searches_client_id_fkey",
        "marketplace_searches",
        type_="foreignkey",
    )
    op.drop_constraint(
        "marketplace_orders_client_id_fkey",
        "marketplace_orders",
        type_="foreignkey",
    )
    op.drop_constraint(
        "marketplace_favorites_client_id_fkey",
        "marketplace_favorites",
        type_="foreignkey",
    )
    op.drop_constraint(
        "marketplace_carts_client_id_fkey",
        "marketplace_carts",
        type_="foreignkey",
    )

    # Восстанавливаем старые foreign key constraints
    op.create_foreign_key(
        "marketplace_searches_phone_fkey",
        "marketplace_searches",
        "marketplace_clients_list",
        ["phone"],
        ["phone"],
        onupdate="CASCADE",
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "marketplace_orders_phone_fkey",
        "marketplace_orders",
        "marketplace_clients_list",
        ["phone"],
        ["phone"],
        onupdate="CASCADE",
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "marketplace_favorites_phone_fkey",
        "marketplace_favorites",
        "marketplace_clients_list",
        ["phone"],
        ["phone"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "marketplace_carts_phone_fkey",
        "marketplace_carts",
        "marketplace_clients_list",
        ["phone"],
        ["phone"],
        onupdate="CASCADE",
        ondelete="CASCADE",
    )

    # Удаляем колонки client_id
    op.drop_column("marketplace_searches", "client_id")
    op.drop_column("marketplace_orders", "client_id")
    op.drop_column("marketplace_favorites", "client_id")
    op.drop_column("marketplace_carts", "client_id")

    # Удаляем поля first_name, last_name и UTM метки
    op.drop_column("marketplace_clients_list", "referer_id")
    op.drop_column("marketplace_clients_list", "utm_campaign")
    op.drop_column("marketplace_clients_list", "utm_medium")
    op.drop_column("marketplace_clients_list", "utm_source")
    op.drop_column("marketplace_clients_list", "last_name")
    op.drop_column("marketplace_clients_list", "first_name")
