"""Default parameter for test_dataframe.py::test_shuffle

Revision ID: a97d9375430f
Revises: c38b9d85915e
Create Date: 2023-01-03 19:36:30.469391

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a97d9375430f'
down_revision = 'c38b9d85915e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
        update test_run
            set name = 'test_shuffle[tasks]'
            where name == 'test_shuffle';
        """)


def downgrade() -> None:
    pass
