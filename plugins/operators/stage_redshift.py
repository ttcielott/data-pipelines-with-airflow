from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    stage_to_redshift_sql_template = """
    COPY {table_name}
    FROM '{s3_bucket_key}'
    JSON '{json_path_file_bucket_key}'
    ACCESS_KEY_ID '{access_key}'
    SECRET_ACCESS_KEY '{secret_key}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id= 'redshift',
                 aws_credential_id= 'aws_credentials',
                 table_name = '',
                 s3_bucket_key = '',
                 jsonpath_key = 'auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket_key = s3_bucket_key
        self.jsonpath_key = jsonpath_key



    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Stage To Redshift for Table Name, {self.table_name} : PostgresHook Object, redshift instantiated')
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()
        self.log.info(f'Stage To Redshift for Table Name, {self.table_name} : AwsHook, get_credentials - completed')
        
        stage_to_redshift_sql = StageToRedshiftOperator.stage_to_redshift_sql_template.format(
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            table_name= self.table_name,
            s3_bucket_key= self.s3_bucket_key,
            json_path_file_bucket_key = self.jsonpath_key
        )
        self.log.info(f'Stage To Redshift for Table Name, {self.table_name} : running COPY SQL statement - started')
        redshift.run(stage_to_redshift_sql)
        self.log.info(f'Stage To Redshift for Table Name, {self.table_name} : running COPY SQL statement - completed')
        self.log.info(f'Stage To Redshift for Table Name, {self.table_name} - completed.')





