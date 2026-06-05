"""add QUARTER to horizontype enum

Revision ID: d4e5f6a7b8c9
Revises: 9f1e72108f01
Create Date: 2026-06-06 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, Sequence[str], None] = '9f1e72108f01'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add the QUARTER label to the existing PostgreSQL `horizontype` enum,
    # positioned after MONTH (before SEASON) to mirror the postprocessing
    # service. IF NOT EXISTS makes it idempotent; ADD VALUE is allowed inside
    # a transaction on PostgreSQL 12+.
    op.execute("ALTER TYPE horizontype ADD VALUE IF NOT EXISTS 'QUARTER' BEFORE 'SEASON'")


def downgrade() -> None:
    # PostgreSQL cannot remove a value from an enum type, so this is a no-op.
    pass
