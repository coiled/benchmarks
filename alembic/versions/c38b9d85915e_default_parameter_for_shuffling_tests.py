"""Default parameter for shuffling tests

Revision ID: c38b9d85915e
Revises: fa79471ffa8c
Create Date: 2022-12-23 09:05:57.440944

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c38b9d85915e'
down_revision = 'fa79471ffa8c'
branch_labels = None
depends_on = None


def h2o_update_query(test: str, ddf: str) -> str:
        return f"""
        update test_run
            set name = '{test}[{ddf}-tasks]'
            where name == '{test}[{ddf}]';
        """

def rename_h2o_tests() -> None:
    for i in range(1, 10):
        test = f"test_q{i}"
        for ddf_param in ("0.5 GB (csv)", "0.5 GB (parquet)", "5 GB (parquet)"):
            op.execute(f"""
                update test_run
                    set name = '{test}[{ddf_param}-tasks]'
                    where name == '{test}[{ddf_param}]';
                """)           

def rename_join_tests() -> None:
    for test in ("test_join_big", "test_join_small"):
        op.execute(f"""
            update test_run
                set name = '{test}[0.1-tasks]'
                where name == '{test}[0.1]';
            """)

def rename_shuffle_tests() -> None:
    for test in ("test_shuffle_parquet", "test_shuffle_simple", "test_shuffle"):
        op.execute(f"""
            update test_run
                set name = '{test}[tasks]'
                where name == '{test}';
            """)

def upgrade() -> None:
    rename_h2o_tests()
    rename_join_tests()
    rename_shuffle_tests()


def downgrade() -> None:
    pass
