"""fix relations employee

Revision ID: 30ddca6ff7e1
Revises: b966f1edb05f
Create Date: 2025-09-29 21:26:20.908521

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "30ddca6ff7e1"
down_revision = "b966f1edb05f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "employee_shifts_user_id_fkey", "employee_shifts", type_="foreignkey"
    )
    op.create_foreign_key(
        None, "employee_shifts", "relation_tg_cashboxes", ["user_id"], ["id"]
    )


def downgrade() -> None:
    op.drop_constraint(None, "employee_shifts", type_="foreignkey")
    op.create_foreign_key(
        "employee_shifts_user_id_fkey",
        "employee_shifts",
        "tg_accounts",
        ["user_id"],
        ["id"],
    )
