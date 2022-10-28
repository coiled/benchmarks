"""Add default parameter to historical test_basic_sum

Revision ID: 149d2048065b
Revises: a8785a7b3cae
Create Date: 2022-10-18 15:18:00.603726

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '149d2048065b'
down_revision = 'a8785a7b3cae'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run 
        set name = 'test_basic_sum[fast-thin]' 
        where name == 'test_basic_sum';
        """
    )


def downgrade() -> None:
    pass
