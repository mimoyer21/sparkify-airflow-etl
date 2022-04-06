from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    An Airflow DAG operator class to load dimension tables into a Postgres DB by inserting the output of
    a provided SQL statement into a provided destination table.
    
    By default, Airflow runs the execute method when this operator is included in a DAG.
    --
    
    Options: 
        - delete_load: when set to True (which is the default), deletes existing contents 
          of the destination table before loading. Otherwise appends data to existing table
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql_stmt="",
                 destination_table="",
                 destination_db="dev",
                 destination_schema="public",
                 delete_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmt = sql_stmt
        self.destination_table = destination_table
        self.destination_db = destination_db
        self.destination_schema = destination_schema
        self.delete_load = delete_load

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        full_table_name = f"{self.destination_db}.{self.destination_schema}.{self.destination_table}"
        
        # empty the target dim table before the load if delete-load mode is set to true (which is the default)
        if self.delete_load:
            self.log.info(f'Emptying dimension table {full_table_name}')
            redshift_hook.run(f"DELETE FROM {full_table_name}")
        
        # insert into target dim table
        rendered_sql_stmt = f"INSERT INTO {full_table_name}\n" + self.sql_stmt
        self.log.info(f'Loading dimension table {full_table_name} via query: \n{rendered_sql_stmt}')
        redshift_hook.run(rendered_sql_stmt)
