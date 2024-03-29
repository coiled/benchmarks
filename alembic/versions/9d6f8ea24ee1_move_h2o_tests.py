"""Move h2o tests

Revision ID: 9d6f8ea24ee1
Revises: a97d9375430f
Create Date: 2023-01-13 14:29:22.118276

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '9d6f8ea24ee1'
down_revision = 'a97d9375430f'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run
        set path = 'benchmarks/test_h2o.py'
        where path = 'benchmarks/h2o/test_h2o_benchmarks.py';
        """
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
