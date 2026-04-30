"""fix marketplace_view_events: restore event column

Revision ID: ebd3d4fd346e
Revises: 813d08d49474
Create Date: 2025-12-02 01:39:54.930704

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ebd3d4fd346e"
down_revision = "813d08d49474"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col["name"] for col in inspector.get_columns("marketplace_view_events")]

    if "view_type" in columns:
        op.drop_column("marketplace_view_events", "view_type")


def downgrade():
    op.drop_column("marketplace_view_events", "event")
