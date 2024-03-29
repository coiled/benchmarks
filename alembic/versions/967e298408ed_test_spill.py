"""test_spill

Revision ID: 967e298408ed
Revises: a9363331e323
Create Date: 2023-01-09 17:05:13.568510

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '967e298408ed'
down_revision = 'a9363331e323'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run 
        set name = 'test_spilling[uncompressible-release]',
        path = 'benchmarks/test_spill.py'
        where name == 'test_spilling[False]'
        and python_version like '3.9%';
        """
    )
    op.execute(
        """
        update test_run 
        set name = 'test_spilling[uncompressible-keep]',
        path = 'benchmarks/test_spill.py'
        where name == 'test_spilling[True]'
        and python_version like '3.9%';
        """
    )
    op.execute(
        """
        delete from test_run
        where originalname = 'test_spilling'
        and python_version not like '3.9%';
        """
    )
    op.execute(
        """
        delete from test_run
        where originalname = 'test_tensordot_stress';
        """
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
