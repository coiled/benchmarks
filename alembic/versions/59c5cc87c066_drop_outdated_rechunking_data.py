"""Drop outdated rechunking data

Revision ID: 59c5cc87c066
Revises: e11cd1aaed38
Create Date: 2024-08-16 15:16:27.114045

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '59c5cc87c066'
down_revision = 'e11cd1aaed38'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        delete from test_run
        where originalname in (
            'test_adjacent_groups',
            'test_heal_oversplit',
            'test_swap_axes',
            'test_tiles_to_rows'
        )
        """
    )


def downgrade() -> None:
    pass
