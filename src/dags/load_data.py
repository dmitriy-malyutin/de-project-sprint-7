#импорт библиотек для инициализации спарка
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import findspark
findspark.init()
findspark.find()

#переменные окружения спарка
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

#импорт библиотек для DAG
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


#задаем все переменные далее по коду (они будут обозначены коментариями в коде где они используются)
user = "dmitriymal"
hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = "/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/"

## /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster initial_load_job.py dmitriymal hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/
## spark-submit --master yarn --deploy-mode cluster initial_load_job.py user hdfs_path geo_path

args = {
    "owner": "dmitriymal",
    'retries': 3,
    'start_date': datetime(2022, 1, 1)
}

with DAG("load_data",
    schedule_interval='0 15 * * *',
    default_args=args,
    catchup=True,
    concurrency=4,
    max_active_runs=1,
    tags=['practicum', 'project', 's7']
         ) as dag:

    start_task = DummyOperator(task_id='start')

    initial_load_task = SparkSubmitOperator(
        task_id='initial_load_job',
        application ='/lessons/dags/scripts/load_job.py' ,
        conn_id= 'yarn_spark',
        application_args = [user, hdfs_path, geo_path],
        conf={
            "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> initial_load_task >> end_task
