"""Drop outdated H2O data

Revision ID: 81f6fd019990
Revises: 2d2405ad763b
Create Date: 2024-08-22 13:11:40.650558

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '81f6fd019990'
down_revision = '2d2405ad763b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where path = 'benchmarks/test_h2o.py'
        """
    )


def downgrade() -> None:
    pass
