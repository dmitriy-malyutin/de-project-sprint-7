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
import pyspark.sql.functions as F 
from pyspark.sql.window import Window 
from pyspark.sql.types import *


user = sys.argv[1] 
hdfs_path = sys.argv[2]
citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/geo.csv"
start_date = sys.argv[4] 
depth = sys.argv[5] 
geo_path = sys.argv[3]


#Функция расчета партиционирования данных за день и сохранения в STG слой

def main():

    spark = SparkSession.builder \
        .master('yarn') \
        .appName(f"{user}_update_stg_by_date_{start_date}_with_depth_{depth}") \
        .getOrCreate()  


    def parquet_event(date, depth, user, hdfs_path, geo_path): 

        for i in range(int(depth)):
            i_date = ((datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d'))
            i_input_source_path = hdfs_path + geo_path + "date=" + i_date
            i_output_path = hdfs_path + "/user/" + user + "/data/events/date=" + i_date
            
            print(f"input: {i_input_source_path}")
            print(f"output: {i_output_path}")
            
            events = spark.read \
                .option('basePath', f'{i_input_source_path}') \
                .parquet(f"{i_input_source_path}")
            
            #Сохраняем файл в parquet по партиции event_type только в папку соответствующего дня
            events.write. \
                mode('overwrite'). \
                partitionBy('event_type'). \
                parquet(f'{hdfs_path}/user/{user}/data/events/date={i_date}')

    parquet_event(date, depth, user, hdfs_path, geo_path)

    spark.stop()



if __name__ == '__main__':
    main()
