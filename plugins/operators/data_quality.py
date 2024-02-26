from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# This Python class `DataQualityOperator` performs data quality checks on tables in a Redshift
# database to ensure correct table names and minimum row counts.
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 dq_check = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_check = dq_check

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f'Data Quality Check : PostgresHook Object, redshift instantiated')

        for pair in self.dq_check:
            test_name = list(pair.keys())[0]
            test_sql = list(pair.values())[0]
            expected_result = list(pair.values())[1]
            self.log.info(f'Data Quality Check : {test_name} - started')

            records = redshift.get_records(test_sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data Quality Check: {test_name} failed. {test_name} returns no result.')
                
            record_value = records[0][0]

            if record_value != expected_result:
                raise ValueError(f'Data quality Check: {test_name} failed. Returned Value: {record_value} / Expected Result: {expected_result}')
        
            self.log.info(f'Data Quality Check : {test_name} - completed')

            
