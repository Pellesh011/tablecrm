"""add_channel_credentials_table

Revision ID: 8e1ae4c26a37
Revises: ac2ecd128db4
Create Date: 2025-11-12 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8e1ae4c26a37"
down_revision = "ac2ecd128db4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Создаем таблицу channel_credentials для хранения креденшалов каналов
    # Поддерживает мультитенантность - каждый cashbox_id имеет свои креденшалы
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    tables = inspector.get_table_names()

    if "channel_credentials" not in tables:
        op.create_table(
            "channel_credentials",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("channel_id", sa.Integer(), nullable=False),
            sa.Column("cashbox_id", sa.Integer(), nullable=False),
            sa.Column(
                "api_key", sa.String(length=500), nullable=False
            ),  # Зашифрованный
            sa.Column(
                "api_secret", sa.String(length=500), nullable=False
            ),  # Зашифрованный
            sa.Column(
                "access_token", sa.String(length=1000), nullable=True
            ),  # Зашифрованный
            sa.Column(
                "refresh_token", sa.String(length=1000), nullable=True
            ),  # Зашифрованный
            sa.Column("token_expires_at", sa.DateTime(), nullable=True),
            sa.Column(
                "avito_user_id", sa.Integer(), nullable=True
            ),  # ID пользователя Avito для API запросов
            sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(),
                server_default=sa.text("now()"),
                nullable=False,
            ),
            sa.ForeignKeyConstraint(
                ["channel_id"], ["channels.id"], ondelete="CASCADE"
            ),
            sa.ForeignKeyConstraint(
                ["cashbox_id"], ["cashboxes.id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint(
                "channel_id",
                "cashbox_id",
                name="uq_channel_credentials_channel_cashbox",
            ),
        )
        op.create_index(
            "ix_channel_credentials_channel_cashbox",
            "channel_credentials",
            ["channel_id", "cashbox_id"],
            unique=False,
        )
        op.create_index(
            "ix_channel_credentials_is_active",
            "channel_credentials",
            ["is_active"],
            unique=False,
        )
    else:
        columns = [col["name"] for col in inspector.get_columns("channel_credentials")]
        if "avito_user_id" not in columns:
            op.add_column(
                "channel_credentials",
                sa.Column("avito_user_id", sa.Integer(), nullable=True),
            )

        indexes = [idx["name"] for idx in inspector.get_indexes("channel_credentials")]
        if "ix_channel_credentials_channel_cashbox" not in indexes:
            op.create_index(
                "ix_channel_credentials_channel_cashbox",
                "channel_credentials",
                ["channel_id", "cashbox_id"],
                unique=False,
            )
        if "ix_channel_credentials_is_active" not in indexes:
            op.create_index(
                "ix_channel_credentials_is_active",
                "channel_credentials",
                ["is_active"],
                unique=False,
            )


def downgrade() -> None:
    op.drop_index("ix_channel_credentials_is_active", table_name="channel_credentials")
    op.drop_index(
        "ix_channel_credentials_channel_cashbox", table_name="channel_credentials"
    )
    op.drop_table("channel_credentials")
