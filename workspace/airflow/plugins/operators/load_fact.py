import datetime

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    An Airflow DAG operator class to load fact tables into a Postgres DB by inserting the output of
    a provided SQL statement into a provided destination table. Appends data to any existing data in 
    the destination_table
    
    By default, Airflow runs the execute method when this operator is included in a DAG.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_stmt="",
                 destination_table="",
                 destination_db="dev",
                 destination_schema="public",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.destination_table = destination_table
        self.destination_db = destination_db
        self.destination_schema = destination_schema

    def execute(self, context):        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        full_table_name = f"{self.destination_db}.{self.destination_schema}.{self.destination_table}"
        rendered_sql_stmt = f"INSERT INTO {full_table_name}\n" + self.sql_stmt
        
        self.log.info(f'Loading fact table via query: \n{rendered_sql_stmt}')
        redshift_hook.run(rendered_sql_stmt)
