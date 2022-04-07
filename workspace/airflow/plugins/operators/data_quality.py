from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    An Airflow DAG operator class to check data quality of tables in a Postgres DB.
    Runs all checks passed in via 'checks' list. Raises error in case of any failures.
    
    By default, Airflow runs the execute method when this operator is included in a DAG.
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 db="dev",
                 schema="public",
                 checks=[], # list of checks to run
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.db = db
        self.schema = schema
        self.checks = checks        
        
    def execute(self, context):    
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # for each check provided, run query and check that output matches expected
        for check in self.checks:
            query = check['test_sql']

            records = redshift_hook.get_records(query)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {query} returned no results")
            result = records[0][0]
            comparison_str = str(result) + check['comparison'] + str(check['expected_result']) # e.g. '0 == 0'
            if not eval(comparison_str):
                raise ValueError(f"Data quality check failed. {query} produced result {result}, which failed \
                                   check '{check['comparison']} {check['expected_result']}'")
            else:
                self.log.info(f"Data quality check for {query} passed with result {result}")

        # # for each table, check to make sure there are >0 rows
        # for table in self.tables:
        #     full_table_name = f"{self.db}.{self.schema}.{table}"
        #     records = redshift_hook.get_records(f"SELECT count(*) from {full_table_name}")
            
        #     if len(records) < 1 or len(records[0]) < 1:
        #         raise ValueError(f"Data quality check failed. {full_table_name} returned no results")
        #     if records[0][0] < 1:
        #         raise ValueError(f"Data quality check failed. {full_table_name} contained 0 rows")
                
        #     self.log.info(f"Data quality on table {full_table_name} check passed with {records[0][0]} records")
            
        
            
            
            