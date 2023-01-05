"""Remove tests in test_shuffle.py

Revision ID: 912c8e30690a
Revises: c38b9d85915e
Create Date: 2023-01-03 20:21:06.704816

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '912c8e30690a'
down_revision = 'c38b9d85915e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        delete from test_run
        where originalname in ('test_shuffle_simple', 'test_shuffle_parquet');
        """)


def downgrade() -> None:
    pass
