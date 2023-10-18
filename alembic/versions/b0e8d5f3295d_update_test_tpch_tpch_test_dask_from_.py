"""Update test_tpch -> tpch/test_dask from #1044

Revision ID: b0e8d5f3295d
Revises: 778e617a2886
Create Date: 2023-10-18 20:14:47.476804

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b0e8d5f3295d'
down_revision = '778e617a2886'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run
        set path = 'benchmarks/tpch/test_dask.py'
        where path = 'benchmarks/test_tpch.py'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        update test_run
        set path = 'benchmarks/test_tpch.py'
        where path = 'benchmarks/tpch/test_dask.py'
        """
    )
