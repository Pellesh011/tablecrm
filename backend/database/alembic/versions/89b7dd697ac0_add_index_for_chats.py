"""add index for chats

Revision ID: 89b7dd697ac0
Revises: merge_videos_mp_orders
Create Date: 2026-03-15 07:04:43.234035

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "89b7dd697ac0"
down_revision = "merge_videos_mp_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_chats_cashbox_updated ON chats (cashbox_id, updated_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_chats_cashbox_status ON chats (cashbox_id, status)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_created ON chat_messages (chat_id, created_at DESC)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_messages_unread ON chat_messages (chat_id, sender_type, status) WHERE status != 'READ'"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_chat_contacts_channel_external ON chat_contacts (channel_id, external_contact_id)"
    )
    pass


def downgrade() -> None:
    pass
