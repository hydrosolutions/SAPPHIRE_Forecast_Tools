"""Add bulletin_share table (PP-039)

Adds the `bulletin_share` table backing the "share a frozen bulletin
snapshot via a public capability-URL token" feature. The service stores
the dashboard-assembled JSON payload verbatim, keyed by a high-entropy
`token`, with a server-side `expires_at` used to gate the public read
endpoint (`GET /public/bulletin/{token}`).

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-07-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "bulletin_share",
        sa.Column("id", sa.Integer(), nullable=False, autoincrement=True),
        sa.Column("token", sa.String(length=64), nullable=False),
        sa.Column(
            "horizon_type",
            postgresql.ENUM(
                "DAY", "PENTAD", "DECADE", "MONTH", "QUARTER", "SEASON",
                name="horizontype",
                create_type=False,
            ),
            nullable=False,
        ),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("horizon_value", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("station_codes", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_bulletin_share_id"), "bulletin_share", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_bulletin_share_token"), "bulletin_share", ["token"], unique=True
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_bulletin_share_token"), table_name="bulletin_share")
    op.drop_index(op.f("ix_bulletin_share_id"), table_name="bulletin_share")
    op.drop_table("bulletin_share")
