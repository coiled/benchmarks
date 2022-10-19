"""Declare bankruptcy for test_futures

Revision ID: 82e8a0855e91
Revises: a8785a7b3cae
Create Date: 2022-10-18 15:21:42.845790

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '82e8a0855e91'
down_revision = 'a8785a7b3cae'
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
