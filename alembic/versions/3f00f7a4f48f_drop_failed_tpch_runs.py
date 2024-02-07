"""Drop failed TPCH runs

Revision ID: 3f00f7a4f48f
Revises: 24749594f367
Create Date: 2024-02-07 14:29:17.081847

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3f00f7a4f48f'
down_revision = '24749594f367'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DELETE
        FROM test_run
        WHERE
            (
                call_outcome = 'failed'
                OR (
                    name LIKE '%query_13'
                    AND duration > 20
                )
            )
            AND PATH='tpch/test_dask.py'
        """
    )


def downgrade() -> None:
    pass
