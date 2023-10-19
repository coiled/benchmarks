"""Remove task based shuffle

Revision ID: 78c6e00fee88
Revises: 778e617a2886
Create Date: 2023-10-19 15:26:04.281985

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "78c6e00fee88"
down_revision = "778e617a2886"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where (
               originalname in ('test_shuffle', 'test_cluster_reconnect')
               or name in ('test_join_big[1-tasks]', 'test_join_big[0.1-tasks]')
            )
        """
    )
    op.execute(
        """
        update test_run
        set name = 'test_join_big[1]'
        where name == 'test_join_big[1-p2p]';
        """
    )
    op.execute(
        """
        update test_run
        set name = 'test_join_big[0.1]'
        where name == 'test_join_big[0.1-p2p]';
        """
    )


def downgrade() -> None:
    pass
