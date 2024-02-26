from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_sql_template = 'TRUNCATE TABLE {};'
    append_sql_template = 'INSERT INTO {} {};'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 table_name = '',
                 content_sql_template = '',
                 truncate_existing_records = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.content_sql_template = content_sql_template
        self.truncate_existing_records = truncate_existing_records

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Load Dimension for Table Name, {self.table_name} : PostgresHook Object, redshift instantiated')
        
        if self.truncate_existing_records:
            trucate_sql_filled = LoadDimensionOperator.truncate_sql_template.format(self.table_name)
            self.log.info(f'Load Dimension for Table Name, {self.table_name} : running Truncate SQL statement - started')
            redshift.run(trucate_sql_filled)
            self.log.info(f'Load Dimension for Table Name, {self.table_name} : running Truncate SQL statement - completed')

        complete_sql_template = LoadDimensionOperator.append_sql_template.format(self.table_name, self.content_sql_template)
        self.log.info(f'Load Dimension for Table Name, {self.table_name} : running Insert SQL statement - started')
        redshift.run(complete_sql_template)
        self.log.info(f'Load Dimension for Table Name, {self.table_name} : running Insert SQL statement - completed')

