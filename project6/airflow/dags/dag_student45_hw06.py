from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from hdfs import InsecureClient

_OWNER = 'Ivanov Nikolay'
_DAG_ID = 'student45_hw06'
_HDFS_URL = 'http://hdfs_url:9870'

schema_name = Variable.get("student45_hw06_schema", "default")
table_name = Variable.get("student45_hw06_table")
email = Variable.get("student45_hw06_email", "test@test.org")
result_path = Variable.get("student45_hw06_result_path")


def get_result_from_hdfs(url, path):
    client = InsecureClient(url)
    result_file = next(part for part in client.list(path) if part.startswith('part-'))
    with client.read(f'{path}/{result_file}', encoding='utf-8') as file:
        return file.read().strip()


default_args = {
    "owner": _OWNER,
    "depends_on_past": False,
    "retries": 0,
}

dag = DAG(
    dag_id=_DAG_ID,
    description="Student45 homework06 Solution",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=4),
    catchup=False,
    max_active_runs=1,
    tags=['student45', 'hw06'],
)

start = EmptyOperator(task_id='start', dag=dag)

end = EmptyOperator(task_id='end', dag=dag)

check_table_exists = HiveOperator(
    task_id='check_table_exists',
    hive_cli_conn_id='hive_student',
    hql=f'select * from {schema_name}.{table_name} limit 1',
    dag=dag,
)

send_email_error = EmailOperator(
    task_id='send_email_error',
    to=email,
    subject=f'Info: {_DAG_ID}. Ошибка при проверке наличия таблицы в Hive',
    html_content=f'''<h2>Ошибка при проверке наличия таблицы в Hive</h2>
    <p>В процессе выполнения DAGв Airflow выявлена ошибка:</p>
    <p>Таблица <strong>{schema_name}.{table_name}</strong> не найдена в Hive.</p>
    <p>Пожалуйста, проверьте состояние Hive или указанные параметры.</p>''',
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

calc_row_count = SparkSubmitOperator(
    dag=dag,
    task_id="calc_row_count",
    conn_id="spark_cluster",
    deploy_mode="cluster",
    executor_memory="1g",
    num_executors="3",
    application=f"/opt/airflow/dags/scripts/student45-hw06-assembly-1.0.jar",
    java_class="student45-hw06",
    name="student45.hw06.CalcRowCount",
    application_args=[schema_name, table_name, result_path],
    trigger_rule=TriggerRule.ALL_SKIPPED,
)

get_row_count = PythonOperator(
    task_id="get_row_count",
    python_callable=get_result_from_hdfs,
    op_args=[_HDFS_URL, result_path],
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

send_email_success = EmailOperator(
    task_id='send_email_success',
    to=email,
    subject=f'Info: {_DAG_ID}. Результат подсчета количества строк в рамках ',
    html_content=f'''<h2>Таблица <strong>{schema_name}.{table_name}:</h2>
    <p>Количество строк: {{ task_instance.xcom_pull(task_ids='get_row_count') }}</p>''',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

start >> check_table_exists >> send_email_error >> calc_row_count >> get_row_count >> send_email_success >> end
