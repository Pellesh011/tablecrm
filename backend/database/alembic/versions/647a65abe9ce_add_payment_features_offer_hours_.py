import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "647a65abe9ce"
down_revision = "51a676700853"
branch_labels = None
depends_on = None


def upgrade() -> None:
    transaction_type_enum = postgresql.ENUM(
        "incoming", "outgoing", "transfer", name="transactiontype", create_type=True
    )
    transaction_type_enum.create(op.get_bind(), checkfirst=True)

    op.add_column(
        "pay_tariffs",
        sa.Column("offer_hours", sa.Integer(), nullable=True, server_default="0"),
    )
    op.add_column(
        "pay_tariffs",
        sa.Column("discount_percent", sa.Float(), nullable=True, server_default="0"),
    )

    op.add_column(
        "pay_transactions", sa.Column("type", transaction_type_enum, nullable=True)
    )
    op.add_column(
        "pay_transactions",
        sa.Column(
            "is_manual_deposit", sa.Boolean(), nullable=True, server_default="false"
        ),
    )

    op.execute("UPDATE pay_transactions SET type = 'outgoing' WHERE type IS NULL")

    op.alter_column("pay_transactions", "type", nullable=False)


def downgrade() -> None:
    op.drop_column("pay_transactions", "is_manual_deposit")
    op.drop_column("pay_transactions", "type")

    op.drop_column("pay_tariffs", "discount_percent")
    op.drop_column("pay_tariffs", "offer_hours")

    transaction_type_enum = postgresql.ENUM(
        "incoming", "outgoing", "transfer", name="transactiontype"
    )
    transaction_type_enum.drop(op.get_bind(), checkfirst=True)
