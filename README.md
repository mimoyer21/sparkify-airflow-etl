# sparkify-airflow-etl
Airflow DAG to run ETL process to populate Redshift db with Sparkify data from S3 data sources.

The source datasets consist of JSON logs that tell about user activity in the Sparkify music application and JSON metadata 
about the songs the users listen to. Source datasets reside in S3. This repo contains an Airflow DAG consisting of three 
main steps:
* load the JSON files into staging tables in Redshift
* transform data from staging tables into fact and dimension final tables
* run data quality checks on final tables

To do so, we create the following four Airflow operators:
* StageToRedshiftOperator - copies data from S3 data sources to staging tables in Redshift. For fact data, copies only the relevant partitions (meaning Airflow backfills can be run to backfill/populate only event data from a certain time frame)
* LoadFactOperator - populates 'songplays' fact table
* LoadDimensionOperator - populates 'users', 'artists', 'songs', and 'time' dimension tables
* DataQualityOperator - checks to ensure all tables are populated with data (simply checks that num rows > 0). Raises error if not.

Final DAG:
![alt text](https://github.com/mimoyer21/sparkify-airflow-etl/blob/main/Sparkify_Airflow_ETL_DAG.png?raw=true) 

*Note: to populate a new Redshift db for the first time where the tables have not yet been created, run `create_tables.sql` 
to create all tables before running the DAG.*
