"""add apscheduler_jobs table

Таблица для APScheduler SQLAlchemyJobStore. Без неё при старте приложения
(импорт jobs.jobs) выполняется DELETE по несуществующей таблице и падает CI/тесты.

Revision ID: apscheduler_jobs_001
Revises: restore_promocodes_001
Create Date: 2026-02-09

"""

import sqlalchemy as sa
from alembic import op

revision = "apscheduler_jobs_001"
down_revision = "restore_promocodes_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "apscheduler_jobs",
        sa.Column("id", sa.Unicode(191), nullable=False),
        sa.Column("next_run_time", sa.Float(25), nullable=True),
        sa.Column("job_state", sa.LargeBinary(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_apscheduler_jobs_next_run_time"),
        "apscheduler_jobs",
        ["next_run_time"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_apscheduler_jobs_next_run_time"),
        table_name="apscheduler_jobs",
    )
    op.drop_table("apscheduler_jobs")
