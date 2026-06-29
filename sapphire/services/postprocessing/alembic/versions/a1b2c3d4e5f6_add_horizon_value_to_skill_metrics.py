"""Add horizon_value to skill_metrics (PP-038 Phase 1)

Staged migration:
  1. Add nullable column with server_default=0 so in-flight writes during
     the migration window do not 500.
  2. Backfill existing rows to 0 (sentinel for all non-month horizons).
  3. Promote column to NOT NULL.
  4. Swap the unique constraint to include horizon_value.

Lock / downtime notes (Postgres):
  - ALTER COLUMN … NOT NULL and CREATE UNIQUE CONSTRAINT both take
    ACCESS EXCLUSIVE on skill_metrics.  Schedule a maintenance window;
    the table may be large after historical recalc.
  - Pause the operational and recalc cron during migrate + backfill.
    Reference: doc/prod/historical_backfill_runbook.md

Backfill value for existing MONTH rows:
  All existing skill_metrics rows (including MONTH rows written before
  Phase 2) are set to 0.  After Phase 2 ships, the apps writer will emit
  the real lead (0–3) for new month rows.  The first post-Phase-2 recalc
  will upsert those rows, updating the sentinel-0 rows to the correct lead.
  Run the month-skill recalc on each environment after deploying Phase 2.

Downgrade note:
  downgrade() drops the new constraint and column only.  It does NOT
  recreate the old narrower unique constraint
  (uq_skill_metrics_horizon_code_model_date_horizon) because per-lead rows
  written after Phase 2 would violate it.

Revision ID: a1b2c3d4e5f6
Revises: 34b227f37299
Create Date: 2026-06-29
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '34b227f37299'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Step 1: add nullable column with server_default so in-flight writes
    # during the migration window do not 500.
    op.add_column(
        "skill_metrics",
        sa.Column(
            "horizon_value",
            sa.Integer(),
            nullable=True,
            server_default="0",
        ),
    )

    # Step 2: backfill — set all existing rows to sentinel 0.
    # Run as a single UPDATE to avoid lock contention on large tables.
    op.execute(
        "UPDATE skill_metrics SET horizon_value = 0 WHERE horizon_value IS NULL"
    )

    # Step 3: promote to NOT NULL after backfill is complete.
    op.alter_column("skill_metrics", "horizon_value", nullable=False)

    # Step 4: swap unique constraint.
    # Alembic cannot extend a constraint in-place — must DROP old and CREATE new.
    op.drop_constraint(
        "uq_skill_metrics_horizon_code_model_date_horizon",
        "skill_metrics",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_skill_metrics_horizon_code_model_date_horizon_value",
        "skill_metrics",
        [
            "horizon_type",
            "code",
            "model_type",
            "date",
            "horizon_in_year",
            "horizon_value",
        ],
    )


def downgrade() -> None:
    # Drop new constraint and column only.
    # Do NOT recreate the old narrower unique constraint — it would raise
    # IntegrityError if per-lead rows (distinct horizon_value) already exist
    # for the same (horizon_type, code, model_type, date, horizon_in_year).
    op.drop_constraint(
        "uq_skill_metrics_horizon_code_model_date_horizon_value",
        "skill_metrics",
        type_="unique",
    )
    op.drop_column("skill_metrics", "horizon_value")
