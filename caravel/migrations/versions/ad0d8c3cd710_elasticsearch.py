"""ElasticSearch

Revision ID: ad0d8c3cd710
Revises: 1226819ee0e3
Create Date: 2016-05-21 22:04:24.482541

"""

# revision identifiers, used by Alembic.
revision = 'ad0d8c3cd710'
down_revision = '1226819ee0e3'

from alembic import op
import sqlalchemy as sa
from sqlalchemy_utils.types.encrypted import EncryptedType


def upgrade():
    with op.batch_alter_table('slices') as batch_op:
        batch_op.add_column(sa.Column('elasticsearch_datasource_id', sa.Integer()))
        batch_op.create_foreign_key('elasticsearch_datasource_id', 'elasticsearch_datasources', ['elasticsearch_datasource_id'], ['id'])
    op.create_table('elasticsearch_datasources',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('datasource_name', sa.String(255), unique=True),
        sa.Column('main_dttm_col', sa.String(255)),
        sa.Column('is_featured', sa.Boolean, default=False),
        sa.Column('description', sa.Text),
        sa.Column('default_endpoint', sa.Text),
        sa.Column('index_name', sa.String(255)),
        sa.Column('user_id', sa.Integer, sa.ForeignKey('ab_user.id')),
        sa.Column('cluster_name', sa.String(250), sa.ForeignKey('elasticsearch_clusters.cluster_name')),
        sa.Column('offset', sa.Integer, default=0),
        sa.Column('cache_timeout', sa.Integer),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True)
    )
    op.create_table('elasticsearch_clusters',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('cluster_name', sa.String(250), unique=True),
        sa.Column('urls', sa.Text),
        sa.Column('password', EncryptedType(sa.String(1024))),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True)
    )
    op.create_table('elasticsearch_metrics',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('metric_name', sa.String(512)),
        sa.Column('verbose_name', sa.String(1024)),
        sa.Column('metric_type', sa.String(32)),
        sa.Column('datasource_name', sa.String(250), sa.ForeignKey('elasticsearch_datasources.datasource_name')),
        sa.Column('json', sa.Text),
        sa.Column('description', sa.Text),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True)
    )
    op.create_table('elasticsearch_fields',
        sa.Column('created_on', sa.DateTime(), nullable=False),
        sa.Column('changed_on', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('datasource_name', sa.String(250), sa.ForeignKey('elasticsearch_datasources.datasource_name')),
        sa.Column('column_name', sa.String(255)),
        sa.Column('is_active', sa.Boolean, default=True),
        sa.Column('type', sa.String(32)),
        sa.Column('doc_values', sa.Boolean, default=False),
        sa.Column('indexed', sa.Boolean, default=False),
        sa.Column('analyzed', sa.Boolean, default=False),
        sa.Column('description', sa.Text),
        sa.Column('groupby', sa.Boolean, default=False),
        sa.Column('count_distinct', sa.Boolean, default=False),
        sa.Column('sum', sa.Boolean, default=False),
        sa.Column('max', sa.Boolean, default=False),
        sa.Column('min', sa.Boolean, default=False),
        sa.Column('filterable', sa.Boolean, default=False),
        sa.Column('created_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True),
        sa.Column('changed_by_fk', sa.Integer(), sa.ForeignKey("ab_user.id"), nullable=True)
    )


def downgrade():
    op.drop_column('slices', 'elasticsearch_datasource_id')
    op.drop_table('elasticsearch_datasources')
    op.drop_table('elasticsearch_clusters')
    op.drop_table('elasticsearch_metrics')
    op.drop_table('elasticsearch_fields')
