from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
aws_bucket= os.getenv("AWS_BUCKET_NAME")

with DAG( 
    dag_id='NBA',
    start_date=datetime(2025,10,12), # create dag 
    schedule='@weekly',
) as dag:
    
    wait_complete, load_complete = EmptyOperator(task_id="wait_complete"), EmptyOperator(task_id="load_complete")

    # run script to fetch nba data
    python_api_operation_task = BashOperator(
        task_id ='run_python_job',
        bash_command='python /opt/airflow/scripts/api_to_aws.py',

    )

    # make sure raw data lands into datalake
    wait_for_s3_file_team = S3KeySensor(
        task_id='wait_for_nba_data_team',
        bucket_name=aws_bucket,
        bucket_key='raw/dim_nba_teams.csv',
        aws_conn_id=Variable.get("AWS_CONN_ID"),   
        poke_interval=60,            
        timeout=60 * 8,             # 8 minutes max wait
        mode='reschedule'                  # or 'reschedule' 
    )

    wait_for_s3_file_player = S3KeySensor(
        task_id='wait_for_nba_data_player',
        bucket_name=aws_bucket,
        bucket_key='raw/dim_nba_players.csv',     
        aws_conn_id=Variable.get("AWS_CONN_ID"),   
        poke_interval=60,            
        timeout=60 * 8,             
        mode='reschedule'                  
    )

    wait_for_s3_file_fact_player = S3KeySensor(
        task_id='wait_for_nba_data_f_player',
        bucket_name=aws_bucket,
        bucket_key='raw/fact_nba_player_stats.csv',
        aws_conn_id=Variable.get("AWS_CONN_ID"),   
        poke_interval=60,            
        timeout=60 * 8,             
        mode='reschedule'                  
    )

    wait_for_s3_file_coach = S3KeySensor(
        task_id='wait_for_nba_data_coach',
        bucket_name=aws_bucket,
        bucket_key='raw/dim_coaches.csv',
        aws_conn_id=Variable.get("AWS_CONN_ID"),   
        poke_interval=60,            
        timeout=60 * 8,             
        mode='reschedule'                 
    )
    truncate_fact = SnowflakeOperator(
        task_id="truncate_fact_table",
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"),
        sql="TRUNCATE TABLE fact_nba_player_stats;"
    )

    truncate_dim_players = SnowflakeOperator(
        task_id="truncate_dim_players",
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"),
        sql="TRUNCATE TABLE dim_nba_players;"
    )

    truncate_dim_teams = SnowflakeOperator(
        task_id="truncate_dim_teams",
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"),
        sql="TRUNCATE TABLE dim_nba_teams;"
    )

    truncate_dim_coaches = SnowflakeOperator(
        task_id="truncate_dim_coaches",
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"),
        sql="TRUNCATE TABLE dim_coaches;"
    )
     # Load fact and dim raw tables to sf     
    load_fct_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_fct_to_snowflake",
        table="fact_nba_player_stats",
        stage=Variable.get("SNOWFLAKE_STAGE"), 
        file_format="(TYPE=CSV, SKIP_HEADER=1)",
        files=['fact_nba_player_stats.csv'],
        pattern=".*[.]csv",
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"), 
        autocommit=True
    )

    load_dp_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_dp_to_snowflake",
        table="dim_nba_players",
        stage=Variable.get("SNOWFLAKE_STAGE"), 
        file_format="(TYPE=CSV, SKIP_HEADER=1)",
        pattern=".*[.]csv",
        files=['dim_nba_players.csv'],
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"), 
        autocommit=True
    )
       
    load_dc_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_dc_to_snowflake",
        table="dim_coaches",
        stage=Variable.get("SNOWFLAKE_STAGE"), 
        file_format="(TYPE=CSV, SKIP_HEADER=1)",
        pattern=".*[.]csv",
        files=['dim_coaches.csv'],
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"), 
        autocommit=True
    )

    load_dt_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_dt_to_snowflake",
        table="dim_nba_teams",
        stage=Variable.get("SNOWFLAKE_STAGE"), 
        file_format="(TYPE=CSV, SKIP_HEADER=1)",
        pattern=".*[.]csv",
        files=['dim_nba_teams.csv'],
        snowflake_conn_id=Variable.get("SNOWFLAKE_CONN_ID"), 
        autocommit=True
    )

    # Step 1: Install dbt packages
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps"
    )

    # Step 2: SCD 2
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/dbt && dbt snapshot --profiles-dir /opt/airflow/dbt"
    )

    # Step 3: Seed static data
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt seed --profiles-dir /opt/airflow/dbt"
    )

    # Step 4: Build models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"
    )

    # Step 5: Run data tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt"
    )

    # Step 6: Generate docs
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir /opt/airflow/dbt"
    )

# Groups 
s3_sensors = [
    wait_for_s3_file_team,
    wait_for_s3_file_player,
    wait_for_s3_file_fact_player,
    wait_for_s3_file_coach
]

truncate_tasks = [
    truncate_fact,
    truncate_dim_players,
    truncate_dim_teams,
    truncate_dim_coaches
]

snowflake_loads = [
    load_fct_to_snowflake,
    load_dp_to_snowflake,
    load_dt_to_snowflake,
    load_dc_to_snowflake
]

# --- Dependencies ---
python_api_operation_task >> s3_sensors >> wait_complete

# Chain truncates → loads in parallel per table
wait_complete >> [
    truncate_fact >> load_fct_to_snowflake,
    truncate_dim_players >> load_dp_to_snowflake,
    truncate_dim_teams >> load_dt_to_snowflake,
    truncate_dim_coaches >> load_dc_to_snowflake
] >> load_complete

# Downstream dbt orchestration
load_complete >> dbt_deps >> dbt_snapshot >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs_generate

