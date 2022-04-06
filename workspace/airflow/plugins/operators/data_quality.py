from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    An Airflow DAG operator class to check data quality of tables in a Postgres DB.
    Checks all tables passed in via a 'tables' list have >0 rows of data and raises ValueError if not.
    
    By default, Airflow runs the execute method when this operator is included in a DAG.
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 db="dev",
                 schema="public",
                 tables=[], # list of table names on which to run checks
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.db = db
        self.schema = schema
        self.tables = tables        
        
    def execute(self, context):    
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # for each table, check to make sure there are >0 rows
        for table in self.tables:
            full_table_name = f"{self.db}.{self.schema}.{table}"
            records = redshift_hook.get_records(f"SELECT count(*) from {full_table_name}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {full_table_name} returned no results")
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {full_table_name} contained 0 rows")
                
            self.log.info(f"Data quality on table {full_table_name} check passed with {records[0][0]} records")
            
        
            
            
            