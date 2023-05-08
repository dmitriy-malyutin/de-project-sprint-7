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
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


#задаем все переменные далее по коду
user = "dmitriymal"
hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = "/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/"
start = '2022-06-2dd1'
depth = 28


## /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster calculating_geo_analitics.py dmitriymal hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/ 2022-05-21 28
## spark-submit --master yarn --deploy-mode cluster calculating_geo_analitics.py user hdfs_path geo_path date depth

args = {
    "owner": "dmitriymal",
    'retries': 3
}

with DAG(
        'main_project',
        default_args=args,
        description='start pyspark for update stg and calculate marts',
        catchup=True,
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        tags=['practicum', 'project', 's7'],
        is_paused_upon_creation=True,
) as dag:

    start_task = DummyOperator(
        task_id='start'
        )

    update_stg_task = SparkSubmitOperator(
        task_id='update_stg',
        application ='/lessons/dags/scripts/update_stg.py' ,
        conn_id= 'yarn_spark',
        application_args = [user, hdfs_path, geo_path, start, depth],
        conf={
            "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    calculate_geo_task = SparkSubmitOperator(
        task_id='calculate_geo',
        application ='/lessons/dags/scripts/calculate_geo.py',
        conn_id= 'yarn_spark',
        application_args = [user, hdfs_path, geo_path, start, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    user_analitics_task = SparkSubmitOperator(
        task_id='user_analitics_task',
        application ='/lessons/dags/scripts/user_analitics.py' ,
        conn_id= 'yarn_spark',
        application_args = [user, hdfs_path, geo_path, start, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 4,
        executor_memory = '16g'
    )

    friends_recommendation_task = SparkSubmitOperator(
        task_id='calculate_friend_recomendation',
        application ='/lessons/dags/scripts/calculate_friend_recomendation.py' ,
        conn_id= 'yarn_spark',
        application_args = [user, hdfs_path, geo_path, start, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 6,
        executor_memory = '24g'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> update_stg_task >> user_analitics_task >> calculate_geo_task >> friends_recommendation_task >> end_task
