import sqlalchemy as sa
from alembic import op

revision = "86c2d7aba466"
down_revision = "b7f1f1b0aaf7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE pay_transactions SET is_manual_deposit = false WHERE is_manual_deposit IS NULL"
    )

    op.alter_column(
        "pay_transactions",
        "is_manual_deposit",
        existing_type=sa.Boolean(),
        nullable=False,
        server_default="false",
    )

    op.add_column(
        "pay_transactions", sa.Column("external_id", sa.String(), nullable=True)
    )


def downgrade() -> None:
    try:
        op.drop_column("pay_transactions", "external_id")
    except Exception:
        pass

    op.alter_column(
        "pay_transactions",
        "is_manual_deposit",
        existing_type=sa.Boolean(),
        nullable=True,
        server_default="false",
    )
