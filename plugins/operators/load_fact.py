from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The `LoadFactOperator` class is a custom Airflow operator designed to insert data into a Redshift
# table using a provided SQL template.
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_template = 'INSERT INTO {} {};'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 table_name = '',
                 sql_template = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_template = sql_template

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Load Fact for Table Name, {self.table_name} : PostgresHook Object, redshift instantiated')
        complete_sql_template = LoadFactOperator.sql_template.format(self.table_name, self.sql_template)
        self.log.info(f'Load Fact for Table Name, {self.table_name} : running Insert SQL statement - started')
        redshift.run(complete_sql_template)
        self.log.info(f'Load Fact for Table Name, {self.table_name} : running Insert SQL statement - completed')
