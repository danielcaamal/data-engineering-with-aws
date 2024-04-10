from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                        LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries, SqlDataQualityQueries


# Default_args object is used in the DAG
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

@dag(
    # Defaults_args are bind to the DAG
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    # The DAG has a correct schedule (once an hour)
    schedule_interval='0 * * * *',
    tags=['project-data-pipelines'],
)
def final_project():
    """Final Project DAG to load and transform data in Redshift with Airflow.
    
    The DAG has the following tasks:
    - Begin_execution: DummyOperator to start the execution
    - Stage_events: StageToRedshiftOperator to load events data from S3 to Redshift staging table
    - Stage_songs: StageToRedshiftOperator to load songs data from S3 to Redshift staging table
    - Load_songplays_fact_table: LoadFactOperator to load data from staging tables to fact table
    - Load_user_dim_table: LoadDimensionOperator to load data from staging tables to user dimension table
    - Load_song_dim_table: LoadDimensionOperator to load data from staging tables to song dimension table
    - Load_artist_dim_table: LoadDimensionOperator to load data from staging tables to artist dimension table
    - Load_time_dim_table: LoadDimensionOperator to load data from staging tables to time dimension table
    - Run_data_quality_checks: DataQualityOperator to run data quality checks
    """


    # Start execution
    start_operator = DummyOperator(task_id='Begin_execution')

  
    # Stage events to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="project-data-pipelines",
        s3_key="log-data",
        copy_params="REGION 'us-east-1' FORMAT AS JSON 's3://project-data-pipelines/log_json_path.json'",
        # Context is also supported
        # Example:
        # s3_key="log-data/{year}/{month}",
        # context={
        #     'year':'2018',
        #     'month': '11'
        # },
    )

    # Stage songs to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="project-data-pipelines",
        s3_key="song-data/A/A/B",
        copy_params="REGION 'us-east-1' FORMAT as JSON 'auto' TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL",
    )

    # Load songplays fact table from staging tables
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql_insert=SqlQueries.songplay_table_insert,
        insert_mode="replace",
    )

    # Load dimension table user from staging tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql_insert=SqlQueries.user_table_insert,
        insert_mode="replace",
    )

    # Load dimension table song from staging tables
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql_insert=SqlQueries.song_table_insert,
        insert_mode="replace",
    )

    # Load dimension table artist from staging tables
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql_insert=SqlQueries.artist_table_insert,
        insert_mode="replace",
    )

    # Load dimension table time from staging tables
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql_insert=SqlQueries.time_table_insert,
        insert_mode="replace",
    )

    # Run data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        dq_checks=SqlDataQualityQueries.data_quality_checks,
    )

    # End execution
    end_operator = DummyOperator(task_id='End_execution')

    # All tasks have correct dependencies
    # Staging tables
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    # Load dimension tables
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    # Quality check
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    # End execution
    run_quality_checks >> end_operator

final_project_dag = final_project()