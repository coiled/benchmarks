"""Default parameter for test_rechunk_in_memory

Revision ID: d58983739401
Revises: 9d6f8ea24ee1
Create Date: 2023-03-07 11:20:28.558141

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd58983739401'
down_revision = '9d6f8ea24ee1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(f"""
        update test_run
            set name = 'test_rechunk_in_memory[tasks]'
            where name == 'test_rechunk_in_memory';
        """)


def downgrade() -> None:
    pass
