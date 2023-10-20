"""Update tpch refactor from #1094

Revision ID: 87cbf883c2be
Revises: b0e8d5f3295d
Create Date: 2023-10-18 20:31:17.848799

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "87cbf883c2be"
down_revision = "b0e8d5f3295d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run
        set path = substr(path, length('benchmarks/') + 1)
        where path like 'benchmarks/tpch/%';
        """
    )


def downgrade() -> None:
    op.execute(
        """
        update test_run
        set path = 'benchmarks/' || path
        where path like 'tpch/%';
        """
    )
