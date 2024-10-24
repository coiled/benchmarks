"""Add column for py-spy profiles url

Revision ID: aa1fc9fdc665
Revises: 1095dfdfc4ae
Create Date: 2024-10-23 16:11:24.794416

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa1fc9fdc665'
down_revision = '1095dfdfc4ae'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('test_run', sa.Column('py_spy_profiles_url', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("test_run", "py_spy_profiles_url")
