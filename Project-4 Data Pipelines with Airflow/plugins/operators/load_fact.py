from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 truncate_target_table=True,
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.target_table=target_table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_target_table=truncate_target_table
        self.query=query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_target_table:
            self.log.info('Truncate Table:'+ self.target_table)
            redshift.run("DELETE FROM {}".format(self.target_table))
            
        self.log.info('Run sql query'+ self.query)
        redshift.run(f"Insert into {self.target_table} {self.query}")
