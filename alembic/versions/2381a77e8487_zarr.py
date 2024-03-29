"""zarr

Revision ID: 2381a77e8487
Revises: d58983739401
Create Date: 2023-03-13 14:57:02.474967

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2381a77e8487'
down_revision = 'd58983739401'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        update test_run
        set path = 'benchmarks/test_zarr.py'
        where path = 'benchmarks/test_array.py'
        and originalname in (
            'test_filter_then_average', 
            'test_access_slices', 
            'test_sum_residuals'
        )
        """
    )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
