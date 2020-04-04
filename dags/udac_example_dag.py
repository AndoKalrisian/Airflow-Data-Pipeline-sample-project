from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.udacity_plugin import (StageToRedshiftOperator, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)

#from helpers import SqlQueries
 
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
 
from helpers.sql_queries import SqlQueries

import operator

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'retry_delay': timedelta(seconds=10),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          #schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    target_table="staging_events",
    sql_table_create=SqlQueries.staging_events_table_create,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    #s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    s3_key="log_data",
    json_file="s3://udacity-dend/log_json_path.json",
    region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    target_table="staging_songs",
    sql_table_create=SqlQueries.staging_songs_table_create,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_file="auto",
    region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    target_table="songplays",
    sql_table_create=SqlQueries.songplay_table_create,
    sql_table_insert=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",
    mode=""
) 

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    target_table="users",
    sql_table_create=SqlQueries.user_table_create,
    sql_table_insert=SqlQueries.user_table_insert,
    redshift_conn_id="redshift"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    target_table="songs",
    sql_table_create=SqlQueries.song_table_create,
    sql_table_insert=SqlQueries.song_table_insert,
    redshift_conn_id="redshift"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    target_table="artists",
    sql_table_create=SqlQueries.artist_table_create,
    sql_table_insert=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    target_table="time",
    sql_table_create=SqlQueries.time_table_create,
    sql_table_insert=SqlQueries.time_table_insert,
    redshift_conn_id="redshift"
)

run_staging_quality_checks = DataQualityOperator(
    task_id='Run_staging_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_test_arr=[{"query": "SELECT COUNT(*) FROM staging_events", "operator": operator.gt, "test_value": 0},
                   {"query": "SELECT COUNT(*) FROM staging_songs","operator": operator.gt, "test_value": 0}]
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_test_arr=[{"query": "SELECT COUNT(*) FROM songplays", "operator": operator.gt, "test_value": 1 },
                   {"query": "SELECT COUNT(*) FROM artists", "operator": operator.gt, "test_value": 1},
                   {"query": "SELECT COUNT(*) FROM users", "operator": operator.gt, "test_value": 1},
                   {"query": "SELECT COUNT(*) FROM songs", "operator": operator.gt, "test_value": 1},
                   {"query": "SELECT COUNT(*) FROM time", "operator": operator.gt, "test_value": 1},
                   {"query": "SELECT COUNT(*) FROM songplays WHERE song_id = NULL", "operator": operator.eq, "test_value": 0}]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> run_staging_quality_checks
stage_songs_to_redshift >> run_staging_quality_checks
run_staging_quality_checks >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
