from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 11, 1),
    'retries' : 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
    'catchup_by_default': False,
    'trigger_rule': 'all_success'
}

dag = DAG('udacity_college_football_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
          #schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_conference_to_redshift = StageToRedshiftOperator(
    task_id='load_conference_table',
    dag=dag,
    table = "public.conference",
    s3_path = "s3://collegefootball/conference.csv",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

stage_team_to_redshift = StageToRedshiftOperator(
    task_id='load_team_table',
    dag=dag,
    table = "public.team",
    s3_path = "s3://collegefootball/team.csv",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

stage_game_to_redshift = StageToRedshiftOperator(
    task_id='load_game_table',
    dag=dag,
    table = "public.game",
    s3_path = "s3://collegefootball/game.csv",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

stage_drive_to_redshift = StageToRedshiftOperator(
    task_id='load_drive_table',
    dag=dag,
    table = "public.drive",
    s3_path = "s3://collegefootball/drive.csv",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

stage_play_to_redshift = StageToRedshiftOperator(
    task_id='load_play_table',
    dag=dag,
    table = "public.play",
    s3_path = "s3://collegefootball/play.csv",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

stage_playtype_to_redshift = StageToRedshiftOperator(
    task_id='load_playtype_table',
    dag=dag,
    table = "public.playtype",
    s3_path = "s3://collegefootball/playtype.csv",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

data_quality_conference_table = DataQualityOperator(
    task_id='data_quality_conference_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    #tables_array=["conference", "team", "game", "drive", "play", "playtype"],
    table = "public.conference"
)

data_quality_team_table = DataQualityOperator(
    task_id='data_quality_team_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    table = "public.team"
)

data_quality_game_table = DataQualityOperator(
    task_id='data_quality_game_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    table = "public.game"
)
data_quality_drive_table = DataQualityOperator(
    task_id='data_quality_drive_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    table = "public.drive"
)

data_quality_play_table = DataQualityOperator(
    task_id='data_quality_play_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    table = "public.play"
)

data_quality_playtype_table = DataQualityOperator(
    task_id='data_quality_playtype_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_credentials",
    table = "public.playtype"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Execute tasks

start_operator >> [stage_conference_to_redshift, stage_team_to_redshift, stage_game_to_redshift, stage_drive_to_redshift, stage_play_to_redshift, stage_playtype_to_redshift] 
[stage_conference_to_redshift >> data_quality_conference_table, stage_team_to_redshift >> data_quality_team_table, stage_game_to_redshift >> data_quality_game_table, stage_drive_to_redshift >> data_quality_drive_table, stage_play_to_redshift >> data_quality_play_table, stage_playtype_to_redshift >> data_quality_playtype_table] >> end_operator
