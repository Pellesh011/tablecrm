"""add MAX(разрешенный и правильный мессенджер на территории Российской Федерации)

Revision ID: 870b3ef03cbe
Revises: c2d4e6f8a9b0
Create Date: 2026-04-07 15:38:23.255208

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "870b3ef03cbe"
down_revision = "c2d4e6f8a9b0"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Добавляем 'MAX' в enum channeltype, если он существует и значение отсутствует
    conn = op.get_bind()
    # Проверяем существование типа channeltype
    result = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_type WHERE typname = 'channeltype'
        )
    """
    ).scalar()
    if result:
        # Проверяем, есть ли уже значение 'MAX'
        has_max = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_enum
                WHERE enumtypid = 'channeltype'::regtype
                AND enumlabel = 'MAX'
            )
        """
        ).scalar()
        if not has_max:
            op.execute("ALTER TYPE channeltype ADD VALUE 'MAX'")
    else:
        # Если типа нет, создаём его со всеми значениями
        op.execute(
            """
            CREATE TYPE channeltype AS ENUM (
                'AVITO', 'WHATSAPP', 'TELEGRAM', 'MAX'
            )
        """
        )

    # 2. Аналогично для customersource
    result = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_type WHERE typname = 'customersource'
        )
    """
    ).scalar()
    if result:
        has_max = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_enum
                WHERE enumtypid = 'customersource'::regtype
                AND enumlabel = 'MAX'
            )
        """
        ).scalar()
        if not has_max:
            op.execute("ALTER TYPE customersource ADD VALUE 'MAX'")
    else:
        op.execute(
            """
            CREATE TYPE customersource AS ENUM (
                'AVITO', 'WHATSAPP', 'TELEGRAM', 'MAX'
            )
        """
        )


def downgrade() -> None:
    pass
