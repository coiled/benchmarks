"""Declare bankruptcy for cluster startup time

Revision ID: 2764a4f5582b
Revises: 924e9b1430e1
Create Date: 2022-09-14 11:45:46.024184

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "2764a4f5582b"
down_revision = "924e9b1430e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where originalname = 'test_default_cluster_spinup_time'
        and path = 'benchmarks/test_coiled.py';
        """
    )


def downgrade() -> None:
    pass
