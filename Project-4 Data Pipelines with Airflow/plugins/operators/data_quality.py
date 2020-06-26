from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
 
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 quality_check_list=[],
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_check_list = quality_check_list  

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('DataQualityOperator: running quality check.')
        for quality_check in self.quality_check_list:            
            self.log.info('DataQualityOperator: running quality_check_list for loop')
            rows = redshift.get_records(quality_check['null_count'])
            if rows[0] != quality_check['expected_null_result']:
                raise ValueError("Data load quality check not successful")
        