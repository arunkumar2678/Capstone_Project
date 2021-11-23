
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_conn_id="",
                 #target_array=[],
                 table = "",
                 sql = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.sql = sql
        self.execution_date = kwargs.get('execution_date')
        
    def execute(self, context):
        self.log.info('Data Quality checks')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        record_count = redshift.get_records("SELECT COUNT(*) FROM {}".format(self.table))
        total_records = record_count[0][0]
        self.log.info (total_records)
        if total_records < 1:
            raise ValueError("Fail: 0 rows in {self.table}")
        null_count = redshift.get_records(self.sql)
        null_constraint_check = null_count[0][0]
        if null_constraint_check > 0:
            raise ValueError("Fail: Primary key column is not unique in {}". format(self.table))
        
        