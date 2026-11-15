"""Add quarterly_snapshot to bulletins (PP-075)

Adds a nullable JSON column `quarterly_snapshot` to the existing `bulletins`
table so that a monthly bulletin row can store a frozen copy of the
supplementary quarterly reservoir section captured at save time.

Existing rows remain NULL. No backfill is performed: historical quarterly
bounds cannot be reconstructed reliably. Display/export must treat NULL as
"no saved quarterly section" until the operator explicitly re-adds the entry.

No new unique constraint, endpoint, or table is created.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-11-15
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("bulletins", sa.Column("quarterly_snapshot", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("bulletins", "quarterly_snapshot")
