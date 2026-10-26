"""add outbox status created_at index

Revision ID: 9c1f4b7e2a3d
Revises: 130235ef5f74
Create Date: 2026-10-26 19:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9c1f4b7e2a3d"
down_revision: Union[str, Sequence[str], None] = "130235ef5f74"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Создает композитный индекс под выборку pending-событий."""
    op.create_index(
        "ix_outbox_status_created_at",
        "outbox",
        ["status", "created_at"],
        unique=False,
    )
    op.drop_index("ix_outbox_status", table_name="outbox")


def downgrade() -> None:
    """Возвращает индекс только по status."""
    op.create_index("ix_outbox_status", "outbox", ["status"], unique=False)
    op.drop_index("ix_outbox_status_created_at", table_name="outbox")
