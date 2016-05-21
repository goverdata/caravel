"""Materializing permission

Revision ID: c3a8f8611885
Revises: 4fa88fe24e94
Create Date: 2016-04-25 08:54:04.303859

"""

# revision identifiers, used by Alembic.
revision = 'c3a8f8611885'
down_revision = '4fa88fe24e94'

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm.strategy_options import Load
from caravel import db
from caravel import models


def upgrade():
    bind = op.get_bind()
    op.add_column('slices', sa.Column('perm', sa.String(length=2000), nullable=True))
    session = db.Session(bind=bind)

    # slices.elasticsearch_datasource_id and elasticsearch_datasource don't exists yet
    for slc in session.query(models.Slice).options(
        Load(models.Slice).load_only("perm"),
        Load(models.SqlaTable).load_only("id", "table_name", "database_id"),
        Load(models.DruidDatasource).load_only("id", "datasource_name", "cluster_name")
        ).all():
        if slc.datasource:
            slc.perm = slc.datasource.perm
            session.merge(slc)
            session.commit()
    db.session.close()


def downgrade():
    # Use batch_alter_table because dropping columns is not supported in SQLite
    with op.batch_alter_table('slices') as batch_op:
        batch_op.drop_column('perm')
