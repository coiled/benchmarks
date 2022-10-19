"""Declare bankruptcy for test_futures.py

Revision ID: fa79471ffa8c
Revises: 149d2048065b
Create Date: 2022-10-19 16:21:21.871309

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'fa79471ffa8c'
down_revision = '149d2048065b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where originalname in ('test_single_future', 'test_memory_efficient')
        and path = 'benchmarks/test_futures.py';
        """
    )


def downgrade() -> None:
    pass
