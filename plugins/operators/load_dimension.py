from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_template = 'INSERT INTO {} {};'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 table_name = '',
                 content_sql_template = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.content_sql_template = content_sql_template

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        complete_sql_template = LoadDimensionOperator.sql_template.format(self.table_name, self.content_sql_template)
        redshift.run(complete_sql_template)

