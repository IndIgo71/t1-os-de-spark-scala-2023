from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

_OWNER = 'Ivanov Nikolay'
_DAG_ID = 'student45_hw08'
_JAR = f'/opt/airflow/dags/scripts/student45-hw08-assembly-1.0.jar'

schema_name = 'student45'

account_table_name = 'account'
csv_path = f'/var/data/{account_table_name}*.csv'

hdfs_csv_path = f'/user/{schema_name}/hw08/'

PG_URL = Variable.get('postgres_testdb_url')
PG_USER = 'testuser'
PG_PASSWORD = 'testuser'

# имя широкой таблицы
wide_table_name = 'person_wide'

# имя таблицы с весами
datamart_table = 'datamart'

# время запуска дага
current_time = datetime.now()
next_hour = current_time + timedelta(hours=1)

# границы создаваемых партиций
currentPartValue = current_time.strftime('%Y%m%d%H')
nextPartValue = next_hour.strftime('%Y%m%d%H')

default_args = {
    "owner": _OWNER,
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    dag_id=_DAG_ID,
    description="Student45 homework08 Solution",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=['student45', 'hw08'],
)

start = EmptyOperator(task_id='start', dag=dag)

end = EmptyOperator(task_id='end', dag=dag)

upload_csv_to_hdfs = BashOperator(
    dag=dag,
    task_id='upload_csv_to_hdfs',
    bash_command=f'hdfs dfs -mkdir -p  /user/{schema_name}/hw08 && hdfs dfs -put -f {csv_path} {hdfs_csv_path}',
)

load_from_hdfs_to_ods = SparkSubmitOperator(
    dag=dag,
    task_id='load_from_hdfs_to_ods',
    conn_id='spark_cluster',
    deploy_mode='cluster',
    executor_memory='1g',
    num_executors='3',
    name="student45.hw08.HDFSToODSLoader",
    application=_JAR,
    java_class='hw08.loader.HDFSToODSLoader',
    application_args=[schema_name, account_table_name, hdfs_csv_path],
)

load_from_pg_to_ods = SparkSubmitOperator(
    dag=dag,
    task_id='load_from_pg_to_ods',
    conn_id='spark_cluster',
    deploy_mode='cluster',
    executor_memory='1g',
    num_executors='3',
    name='student45.hw08.PgToODSLoader',
    application=_JAR,
    java_class='hw08.loader.PgToODSLoader',
    application_args=[PG_URL, PG_USER, PG_PASSWORD, schema_name],
)

load_wide_table = SparkSubmitOperator(
    dag=dag,
    task_id='load_wide_table',
    conn_id='spark_cluster',
    deploy_mode='cluster',
    executor_memory='1g',
    num_executors='3',
    name='student45.hw08.WideTableLoader',
    application=_JAR,
    java_class='hw08.loader.WideTableLoader',
    application_args=[schema_name, wide_table_name, currentPartValue],
)

load_datamart = SparkSubmitOperator(
    dag=dag,
    task_id='load_datamart',
    conn_id='spark_cluster',
    deploy_mode='cluster',
    executor_memory='1g',
    num_executors='3',
    name='student45.hw08.DataMartLoader',
    application=_JAR,
    java_class='hw08.loader.DataMartLoader',
    application_args=[schema_name, wide_table_name, datamart_table, currentPartValue],
)

create_pg_datamart_schema = PostgresOperator(
    dag=dag,
    task_id='create_pg_datamart_schema',
    postgres_conn_id='pg_conn',
    sql=f'CREATE SCHEMA IF NOT EXISTS {schema_name};',
)

create_pg_datamart_table = PostgresOperator(
    dag=dag,
    task_id='create_pg_datamart_table',
    postgres_conn_id='pg_conn',
    sql=f'''
        CREATE TABLE IF NOT EXISTS {schema_name}.{datamart_table} (
            category TEXT,
            weight NUMERIC(4, 2),
            mans_weight NUMERIC(4, 2),
            hour NUMERIC(10)
        )
        PARTITION BY RANGE (hour);
        
        CREATE TABLE IF NOT EXISTS {schema_name}.{datamart_table}_{currentPartValue} PARTITION OF {schema_name}.{datamart_table}
        FOR VALUES FROM ({currentPartValue}) TO ({nextPartValue});
    ''',
)

load_datamart_to_pg = SparkSubmitOperator(
    dag=dag,
    task_id='load_datamart_to_pg',
    conn_id='spark_cluster',
    deploy_mode='cluster',
    executor_memory='1g',
    num_executors='3',
    name='student45.hw08.DataMartToPGLoader',
    application=f'/opt/airflow/dags/scripts/student45_hw08-assembly-1.0.jar',
    java_class="hw08.loader.DataMartToPGLoader",
    application_args=[PG_URL, PG_USER, PG_PASSWORD, schema_name, datamart_table, currentPartValue],
)

start >> upload_csv_to_hdfs >> [load_from_hdfs_to_ods,
                                load_from_pg_to_ods] >> load_wide_table >> load_datamart >> create_pg_datamart_schema >> create_pg_datamart_table >> load_datamart_to_pg >> end
