from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sql_stmt

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 11, 21),
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
    aws_conn_id = "aws_default",
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
    aws_conn_id = "aws_default",
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
    aws_conn_id = "aws_default",
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
    aws_conn_id = "aws_default",
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
    aws_conn_id = "aws_default",
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
    aws_conn_id = "aws_default",
    region="us-west-2",
    data_format="csv",
    bash_command='exit 0'
)

data_quality_conference_table = DataQualityOperator(
    task_id='data_quality_conference_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    table = "public.conference"
)

data_quality_team_table = DataQualityOperator(
    task_id='data_quality_team_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    table = "public.team"
)

data_quality_game_table = DataQualityOperator(
    task_id='data_quality_game_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    table = "public.game"
)
data_quality_drive_table = DataQualityOperator(
    task_id='data_quality_drive_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    table = "public.drive"
)

data_quality_play_table = DataQualityOperator(
    task_id='data_quality_play_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    table = "public.play"
)

data_quality_playtype_table = DataQualityOperator(
    task_id='data_quality_playtype_table',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    table = "public.playtype"
)

begin_schema_build_operator = DummyOperator(task_id='Begin_Schema_Build',  dag=dag)

#game_fact_table_create = CreateFactDimensionOperator(
#    task_id='game_fact_table_create',
#    dag=dag,
#    redshift_conn_id = "redshift",
#    aws_conn_id = "aws_default",
#    sql = sql_stmt.game_fact_table_create
#)

create_fact_dim_tables = BashOperator(
    task_id='create_fact_dim_tables',    
    bash_command='python /home/workspace/airflow/plugins/create_table_etl.py',
    dag=dag)


load_game_fact_table = LoadFactOperator(
    task_id='load_game_fact_table',
    dag=dag,
    table = "public.game_fact",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.game_fact_table_insert
)

load_conference_dim_table = LoadDimensionOperator(
    task_id='load_conference_dim_table',
    dag=dag,
    table = "public.conference_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.conference_dim_table_insert
)

load_team_dim_table =  LoadDimensionOperator( 
    task_id='load_team_dim_table',
    dag=dag,
    table = "public.tea,_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.team_dim_table_insert
)

load_play_dim_table =  LoadDimensionOperator( 
    task_id='load_play_dim_table',
    dag=dag,
    table = "public.play_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.play_dim_table_insert
)
    
load_game_dim_table = LoadDimensionOperator( 
    task_id='load_game_dim_table',
    dag=dag,
    table = "public.game_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.game_dim_table_insert
)         

load_drive_dim_table = LoadDimensionOperator( 
    task_id='load_drive_dim_table',
    dag=dag,
    table = "public.drive_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.drive_dim_table_insert
)         
    
load_playtype_dim_table = LoadDimensionOperator( 
    task_id='load_playtype_dim_table',
    dag=dag,
    table = "public.playtype_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.playtype_dim_table_insert
)         
    
load_betting_line_dim_table = LoadDimensionOperator( 
    task_id='load_betting_line_dim_table',
    dag=dag,
    table = "public.betting_line_dim",
    redshift_conn_id = "redshift",
    aws_conn_id = "aws_default",
    sql = sql_stmt.betting_line_dim_table_insert
)         

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Execute tasks

start_operator >> [stage_conference_to_redshift, stage_team_to_redshift, stage_game_to_redshift, stage_drive_to_redshift, stage_play_to_redshift, stage_playtype_to_redshift] 
[stage_conference_to_redshift >> data_quality_conference_table, stage_team_to_redshift >> data_quality_team_table, stage_game_to_redshift >> data_quality_game_table, stage_drive_to_redshift >> data_quality_drive_table, stage_play_to_redshift >> data_quality_play_table, stage_playtype_to_redshift >> data_quality_playtype_table] >> begin_schema_build_operator >> create_fact_dim_tables
create_fact_dim_tables >> [load_game_fact_table, load_conference_dim_table, load_team_dim_table,  load_drive_dim_table, load_play_dim_table, load_game_dim_table, load_playtype_dim_table, load_betting_line_dim_table] >> end_operator


