from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
    
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup = False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table_name = 'staging_songs',
        s3_bucket_key = 's3://dana-kim/song-data/A/A/A/'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table_name = 'staging_events',
        s3_bucket_key = 's3://dana-kim/log-data/',
        jsonpath_key = 's3://dana-kim/log_json_path.json'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
	table_name = 'songplays',
        sql_template = SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table_name = 'users',
        content_sql_template = SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table_name = 'songs',
        content_sql_template = SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table_name = 'artists',
        content_sql_template = SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table_name = 'time',
        content_sql_template = SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        table_name='songplays'
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_songs_to_redshift
    start_operator >> stage_events_to_redshift
    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
