from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    An Airflow DAG operator class to copy data into Redshift tables from S3 data sources. Empties the destination
    table before loading in data.
    
    By default, Airflow runs the execute method when this operator is included in a DAG.
    """    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    @apply_defaults
    def __init__(self,
                 aws_credentials="aws_credendials",
                 redshift_conn_id="redshift",
                 s3_bucket="s3://udacity-dend/",
                 s3_key="",
                 source_format="json",
                 copy_format_option="auto ignorecase",
                 destination_db="dev",
                 destination_schema="public",
                 destination_table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.source_format = source_format
        self.copy_format_option = copy_format_option
        self.destination_db = destination_db
        self.destination_schema = destination_schema
        self.destination_table = destination_table

    def execute(self, context):
        # insert the relevant partition into the s3_key based on execution date
        rendered_s3_key = self.s3_key.format(**context)
        
        s3_path = self.s3_bucket + rendered_s3_key
        full_table_name = f"{self.destination_db}.{self.destination_schema}.{self.destination_table}"
        
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_copy_stmt = SqlQueries.copy_stmt.format(full_table_name, 
                                                    s3_path, 
                                                    credentials.access_key, 
                                                    credentials.secret_key, 
                                                    self.source_format, 
                                                    self.copy_format_option)

        # empty staging table before loading it
        self.log.info(f'Emptying table {full_table_name}')
        redshift_hook.run(f"DELETE FROM {full_table_name}")
        
        self.log.info(f'Copying {s3_path} to table {full_table_name}')
        redshift_hook.run(sql_copy_stmt)
        



