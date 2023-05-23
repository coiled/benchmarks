"""Remove non-upstream historical data

Revision ID: f459b2c61eaf
Revises: 4ee0e23d96da
Create Date: 2023-05-23 10:39:13.056358

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'f459b2c61eaf'
down_revision = '4ee0e23d96da'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where (
            coiled_runtime_version <> 'upstream'
            and coiled_runtime_version not like 'AB_%'
        )
        or python_version like '3.8%';
        """
    )


def downgrade() -> None:
    pass
