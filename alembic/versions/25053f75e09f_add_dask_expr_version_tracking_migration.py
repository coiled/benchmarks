"""Add dask-expr version tracking migration

Revision ID: 25053f75e09f
Revises: 24749594f367
Create Date: 2024-02-26 08:04:47.704600

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '25053f75e09f'
down_revision = '24749594f367'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('test_run', sa.Column('dask_expr_version', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('test_run', 'dask_expr_version')
