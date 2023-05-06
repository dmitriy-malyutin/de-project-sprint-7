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
from pyspark.sql.functions import regexp_replace
import sys
import pyspark.sql.functions as F 
from pyspark.sql.window import Window 
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt

user = sys.argv[1] 
hdfs_path = sys.argv[2]
citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/geo.csv"
start_date = sys.argv[4] 
depth = sys.argv[5] 
geo_path = sys.argv[3]


#Функция формирует list со список папок для загрузки  
def input_paths (start_date, depth):

    list_date = []
    for i in range(depth):
        list_date.append(f'{hdfs_path}{geo_path}date='+str((datetime.fromisoformat(start_date) - timedelta(days=i)).date()))
    return list_date


#comment: Func calc distance
def get_distance(lon_a, lat_a, lon_b, lat_b):
    # Transform to radians
    lon_a, lat_a, lon_b, lat_b = map(radians, [lon_a,  lat_a, lon_b, lat_b])
    dist_longit = lon_b - lon_a
    dist_latit = lat_b - lat_a

    # Calculate area
    area = sin(dist_latit/2)**2 + cos(lat_a) * cos(lat_b) * sin(dist_longit/2)**2

    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371

    # Calculate Distance
    distance = central_angle * radius

    return abs(round(distance, 2))

#create a user defined function to use it on our Spark DataFrame
udf_get_distance = F.udf(get_distance)


def main():
    spark = (
            SparkSession
            .builder
            .master('yarn')
            .appName(f"{user}_calculating_user_by_date_{start_date}_with_depth_{depth}")
            .getOrCreate()
    )

    
    #Загрузка всех сообщений
    df_message = (
        spark.read.parquet(*input_paths(start_date, depth)).filter("event_type == 'message'")\
        .select(F.col('event.message_from').alias('user_id')
            ,F.date_trunc("day",F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date")
            ,'lat', 'lon')
        )

    #citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/geo.csv"
    df_csv = spark.read.csv(citygeodata_csv, sep = ';', header = True)
    df_csv = df_csv.withColumn("lat",regexp_replace("lat", ",", ".")).withColumn("lng",regexp_replace("lng",",","."))
    df_citygeodata = df_csv.select(F.col("id").cast(LongType()).alias("city_id"),(F.col("city")).alias("city_name"),(F.col("lat")).cast(DoubleType()).alias("city_lat"),(F.col("lng")).cast(DoubleType()).alias("city_lon"))


    #Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_message_and_citygeodata = (df_message.crossJoin(
        df_citygeodata.hint("broadcast")
    ))

    df_message_and_distance = (df_message_and_citygeodata.select(
        "user_id","date","city_name"
        ,udf_get_distance(
        F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
        ).cast(DoubleType()).alias("distance")).withColumn("row",F.row_number().over(
        Window.partitionBy("user_id","date").orderBy(F.col("distance"))
        )).filter(F.col("row") ==1).
        drop("row", "distance"))

    #расчет показателя act_city по пользователям
    df_act_city = (df_message_and_distance.withColumn("row",F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.col("date").desc())
            ))
        .filter(F.col("row") == 1)
        .drop("row", "distance")
        .withColumnRenamed("city_name", "act_city"))

    #Промежуточный DF дли построение последующих 3 DF
    df_change = (df_message_and_distance.withColumn('max_date',F.max('date')
        .over(Window().partitionBy('user_id')))
        .withColumn('city_name_lag_1_desc',F.lag('city_name',-1,'start')
        .over(Window().partitionBy('user_id').orderBy(F.col('date').desc())))
        .filter(F.col('city_name') != F.col('city_name_lag_1_desc')))

    #расчет домашнего города по условию что сотрудник находился в этом городе 27 дней или более
    df_home_city = (df_change.withColumn('date_lag',F.coalesce(F.lag('date')
        .over(Window().partitionBy('user_id').orderBy(F.col('date').desc())),F.col('max_date')))
        .withColumn('date_diff',F.datediff(F.col('date_lag'),F.col('date')))
        .filter(F.col('date_diff') > 27)
        .withColumn('row',F.row_number()
        .over(Window.partitionBy("user_id").orderBy(F.col("date").desc())))
        .filter(F.col('row') == 1)
        .drop('date','city_name_lag_1_desc','date_lag','row','date_diff','max_date'))

    #считаем количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
    df_travel_count = (df_change.groupBy("user_id").count().withColumnRenamed("count", "travel_count"))

    #формируем список городов в порядке посещения.
    df_travel_array = (df_change
        .groupBy("user_id")
        .agg(F.collect_list('city_name').alias('travel_array')))

    #расчет метики local_time - формула расчета описана в документации
    df_local_time = (
        df_act_city
        .withColumn("timezone", F.concat(F.lit("Australia/"), F.col('act_city')) )
        .withColumn("local_time", F.from_utc_timestamp(F.col("date"),F.col('timezone')))
        .drop("timezone", "date", "act_city"))

    #объединения всех метрик в одину ветрину
    df_user_analitics_mart = (df_act_city.select("user_id", "act_city")
        .join(df_home_city, 'user_id', how='left')
        .join(df_travel_count, 'user_id', how='left')
        .join(df_travel_array, 'user_id', how='left')
        .join(df_local_time, 'user_id', how='left'))

    #Сохранение витрины для аналитиков на hdfs 
    (
        df_user_analitics_mart
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{user}/marts/users")
    )



if __name__ == '__main__':
    main()
