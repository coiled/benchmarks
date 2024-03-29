"""Parametrize test_large_map

Revision ID: 9813b7160e69
Revises: f459b2c61eaf
Create Date: 2023-07-05 11:04:08.510205

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '9813b7160e69'
down_revision = 'f459b2c61eaf'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run 
        set name = 'test_large_map[rootish]' 
        where name == 'test_large_map';
        """
    )

def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
