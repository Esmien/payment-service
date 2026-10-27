"""add webhook delivery state

Revision ID: 4f2c9a7d1b6e
Revises: 9c1f4b7e2a3d
Create Date: 2026-10-26 22:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4f2c9a7d1b6e"
down_revision: Union[str, Sequence[str], None] = "9c1f4b7e2a3d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Добавляет состояние доставки webhook в payments."""
    op.add_column(
        "payments",
        sa.Column("webhook_status", sa.String(length=20), server_default="pending", nullable=False),
    )
    op.add_column(
        "payments",
        sa.Column("webhook_attempts", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "payments",
        sa.Column("webhook_last_error", sa.String(length=1024), nullable=True),
    )


def downgrade() -> None:
    """Удаляет состояние доставки webhook из payments."""
    op.drop_column("payments", "webhook_last_error")
    op.drop_column("payments", "webhook_attempts")
    op.drop_column("payments", "webhook_status")
