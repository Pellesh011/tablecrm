import sqlalchemy as sa
from alembic import op

revision = "49e948206f88"
down_revision = "8e1ae4c26a37"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    tables = inspector.get_table_names()
    if "chat_messages" not in tables:
        return

    columns = [col["name"] for col in inspector.get_columns("chat_messages")]
    if "source" not in columns:
        op.add_column(
            "chat_messages",
            sa.Column(
                "source",
                sa.String(length=50),
                nullable=True,
                comment="Источник отправки сообщения: ios, android, api, web, avito, etc.",
            ),
        )


def downgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)

    tables = inspector.get_table_names()
    if "chat_messages" not in tables:
        return

    columns = [col["name"] for col in inspector.get_columns("chat_messages")]
    if "source" in columns:
        op.drop_column("chat_messages", "source")
