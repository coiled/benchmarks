"""Drop TPC-H data

Revision ID: 2d2405ad763b
Revises: 59c5cc87c066
Create Date: 2024-08-15 13:54:45.251458

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2d2405ad763b'
down_revision = '59c5cc87c066'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("delete from tpch_run")


def downgrade() -> None:
    pass
