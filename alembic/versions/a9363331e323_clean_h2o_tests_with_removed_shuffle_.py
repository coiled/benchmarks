"""Clean h2o tests with removed shuffle param

Revision ID: a9363331e323
Revises: c38b9d85915e
Create Date: 2023-01-03 19:56:22.838577

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a9363331e323'
down_revision = 'c38b9d85915e'
branch_labels = None
depends_on = None


def upgrade() -> None:
    for i in [1, 2, 3, 4, 5, 7]:
        test = f"test_q{i}"
        for ddf_param in ("0.5 GB (csv)", "0.5 GB (parquet)", "5 GB (parquet)"):
            op.execute(f"""
                update test_run
                    set name = '{test}[{ddf_param}]'
                    where name == '{test}[{ddf_param}-tasks]';
                """)
            op.execute(f"""
                delete from test_run
                    where name == '{test}[{ddf_param}-p2p]';
                """)             


def downgrade() -> None:
    pass
