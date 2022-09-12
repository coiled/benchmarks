"""spark test bankruptcy

Revision ID: 924e9b1430e1
Revises: 7d7844fca7cf
Create Date: 2022-09-12 09:49:32.494687

"""
from alembic import op
from sqlalchemy.sql import delete, table


# revision identifiers, used by Alembic.
revision = "924e9b1430e1"
down_revision = "7d7844fca7cf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where originalname = 'test_read_spark_generated_data'
        and path = 'benchmarks/test_parquet.py';
        """
    )


def downgrade() -> None:
    pass
