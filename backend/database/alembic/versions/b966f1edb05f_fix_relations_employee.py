"""fix relations employee

Revision ID: b966f1edb05f
Revises: c9625a6c9c31
Create Date: 2025-09-28 20:11:38.365774

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b966f1edb05f"
down_revision = "c9625a6c9c31"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "employee_shifts_user_id_fkey", "employee_shifts", type_="foreignkey"
    )
    op.create_foreign_key(None, "employee_shifts", "tg_accounts", ["user_id"], ["id"])


def downgrade() -> None:
    op.drop_constraint(None, "employee_shifts", type_="foreignkey")
    op.create_foreign_key(
        "employee_shifts_user_id_fkey",
        "employee_shifts",
        "relation_tg_cashboxes",
        ["user_id"],
        ["id"],
    )
