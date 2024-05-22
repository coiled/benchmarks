"""Add cluster spec to db

Revision ID: e11cd1aaed38
Revises: 00d5844fd364
Create Date: 2024-04-15 10:32:18.323088

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e11cd1aaed38'
down_revision = '00d5844fd364'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('tpch_run', sa.Column('n_workers', sa.Integer(), nullable=True))
    op.add_column('tpch_run', sa.Column('worker_vm_type', sa.String(), nullable=True))
    op.add_column('tpch_run', sa.Column('cluster_disk_size', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('tpch_run', 'cluster_disk_size')
    op.drop_column('tpch_run', 'worker_vm_type')
    op.drop_column('tpch_run', 'n_workers')
    # ### end Alembic commands ###