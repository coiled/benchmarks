"""Add column for Memray profiles url

Revision ID: 1095dfdfc4ae
Revises: 2d2405ad763b
Create Date: 2024-10-23 11:11:15.238042

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1095dfdfc4ae'
down_revision = '2d2405ad763b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('test_run', sa.Column('memray_profiles_url', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("test_run", "memray_profiles_url")
