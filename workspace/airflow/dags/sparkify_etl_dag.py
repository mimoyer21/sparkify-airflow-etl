from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'mmoyer',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform Sparkify data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="s3://udacity-dend/",
    s3_key="log_data", # to parameterize this step to allow for backfills could use "log_data/{execution_date.year}/{execution_date.month}" and update Operator to convert to correct YYYY/MM values, but S3 data source only has data for 2018/11, so then only backfilling for that month would produce results
    source_format="json",
    copy_format_option="auto ignorecase",
    destination_schema="public",
    destination_table="staging_events",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket="s3://udacity-dend/",
    s3_key="song_data",
    source_format="json",
    copy_format_option="auto ignorecase",
    destination_schema="public",
    destination_table="staging_songs",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.songplay_table_insert,
    destination_schema="public",
    destination_table="songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.user_table_insert,
    destination_schema="public",
    destination_table="users",
    delete_load=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.song_table_insert,
    destination_schema="public",
    destination_table="songs",
    delete_load=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.artist_table_insert,
    destination_schema="public",
    destination_table="artists",
    delete_load=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_stmt=SqlQueries.time_table_insert,
    destination_schema="public",
    destination_table="time",
    delete_load=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    schema="public",
    checks=[
        {'test_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 0, 'comparison': '>'},
        {'test_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null", 'expected_result': 0, 'comparison': '=='}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> (stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table
load_songplays_table >> (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks >> end_operator