import os
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

from datetime import datetime, timedelta
import sys


# пример джобы
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster load_job.py shevet hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/


def main():
    # задаем все переменные далее по коду они будут обозначены где они используются
    user = sys.argv[1] 
    hdfs_path = sys.argv[2] 
    geo_path = sys.argv[3]  

    spark = SparkSession.builder \
        .master('yarn') \
        .appName(f"{user}_load") \
        .getOrCreate()

    events = spark.read \
                .option("basePath", f"{hdfs_path}{geo_path}") \
                .parquet(f"{hdfs_path}{geo_path}")

        
    events.write \
            .partitionBy("date", "event_type") \
            .mode("overwrite") \
            .parquet(f"{hdfs_path}/user/{user}/data/events")


if __name__ == '__main__':
    main()
