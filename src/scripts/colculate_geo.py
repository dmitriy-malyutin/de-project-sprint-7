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
geo_path = sys.argv[3]
citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/"
start_date = sys.argv[4]
depth = sys.argv[5]


#Функция формирует list со список папок для загрузки  
def input_paths (start_date, depth):

    list_date = []
    for i in range(depth):
        list_date.append(f'{hdfs_path}{geo_path}date='+str((datetime.fromisoformat(start_date) - timedelta(days=i)).date()))
    return list_date

#Функция расчета дистанции
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

#Создадим пользовательскую функцию, чтобы использовать ее в нашем Spark DataFrame
udf_get_distance = F.udf(get_distance)


def main():
    spark = (
            SparkSession
            .builder
            .master('yarn')
            .appName(f"{user}_calculate_GEO_by_date_{start_date}_with_depth_{depth}")
            .getOrCreate()
        )

    #Сохраняем все события message с заполненными координатами
    df_all_message = (spark.read.parquet(*input_paths(start_date, depth)).filter("event_type == 'message'")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(F.col('event.message_from').alias('user_id')
            ,F.col('event.message_id').alias('message_id')
            ,'lat','lon',F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts')))
            .alias("date")))

    #Сохраняем все события reaction с заполненными координатами
    df_all_reaction = (spark.read.parquet(*input_paths(start_date, depth)).filter("event_type == 'reaction'")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(F.col('event.message_id').alias('message_id')
        ,'lat','lon',F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts')))
        .alias("date")))

    #Сохраняем все события subscription с заполненными координатами
    df_all_subscription = (spark.read.parquet(*input_paths(start_date, depth)).filter("event_type == 'subscription'")
        .where(F.col("lat").isNotNull() & (F.col("lon").isNotNull()))
        .select(F.col('event.user').alias('user')
        ,'lat','lon',F.to_date(F.coalesce(F.col('event.datetime'),F.col('event.message_ts')))
        .alias("date")))

    #hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
    #citygeodata_csv = f"{hdfs_path}/user/{user}/data/citygeodata/geo.csv"
    df_csv = spark.read.csv(citygeodata_csv, sep = ';', header = True)
    df_csv = df_csv.withColumn("lat",regexp_replace("lat", ",", ".")).withColumn("lng",regexp_replace("lng",",","."))
    df_citygeodata = df_csv.select(F.col("id").cast(LongType()).alias("city_id"),(F.col("city")).alias("city_name"),(F.col("lat")).cast(DoubleType()).alias("city_lat"),(F.col("lng")).cast(DoubleType()).alias("city_lon"))


    #Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_all_message_and_citygeodata = df_all_message.crossJoin(
        df_citygeodata.hint("broadcast"))

    #Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_all_reaction_and_citygeodata = df_all_reaction.crossJoin(
        df_citygeodata.hint("broadcast"))

    #Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_all_subscription_and_citygeodata = df_all_subscription.crossJoin(
        df_citygeodata.hint("broadcast"))

    #Получение DF с дистанцией (distance) до города
    df_distance_message = (
        df_all_message_and_citygeodata
        .withColumn("distance", udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
            ).cast(DoubleType())))

    #Получение DF с дистанцией (distance) до города
    df_distance_reaction = (
        df_all_reaction_and_citygeodata
        .withColumn("distance", udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
            ).cast(DoubleType())))

    #Получение DF с дистанцией (distance) до города
    df_distance_subscription = (
        df_all_subscription_and_citygeodata
        .withColumn("distance", udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
            ).cast(DoubleType())))

    # получение ближайшего города
    df_city_message = (
        df_distance_message
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("user_id", "message_id").orderBy(F.col("distance").asc())))
        .filter(F.col("row") == 1)
        .select(
            "user_id", "message_id", "date", "city_id", "city_name")
        .withColumnRenamed("city_id", "zone_id"))

    # получение ближайшего города
    df_city_reaction = (
        df_distance_reaction
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("message_id", "date", "lat", "lon").orderBy(F.col("distance").asc())))
        .filter(F.col("row") == 1)
        .select(
            "message_id", "date", "city_id", "city_name")
        .withColumnRenamed("city_id", "zone_id"))

    #получение ближайшего города
    df_city_subscription = (
        df_distance_subscription
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("lat", "lon", "date").orderBy(F.col("distance").asc())
            ))
        .filter(F.col("row") == 1)
        .select(
            "user", "date", "city_id", "city_name")
        .withColumnRenamed("city_id", "zone_id"))

    #zone_id — идентификатор зоны (города);
    #week — неделя расчёта;
    #month — месяц расчёта;
    #week_message — количество сообщений за неделю;
    #month_message — количество сообщений за месяц;
    df_count_message = (
        df_city_message
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        .withColumn("week_message",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "week"))))
        .withColumn("month_message",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "month"))))
        .select("zone_id", "week", "month", "week_message", "month_message").distinct())

    df_count_reg = (
        df_city_message
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        #.groupBy("zone_id", "week", "month").agg(F.count("message_id").alias("week_message"))
        .withColumn("row",
                    (
                        F.row_number().over(
                            Window.partitionBy("user_id").orderBy(F.col("date").asc()))))
        .filter(F.col("row") == 1)
        .withColumn("week_user",
                    (
                        F.count("row").over(
                            Window.partitionBy("zone_id", "week"))))
        .withColumn("month_user",
                    (
                        F.count("row").over(
                            Window.partitionBy("zone_id", "month"))))
        .select("zone_id", "week", "month", "week_user", "month_user").distinct())
    
    df_count_reaction = (
        df_city_reaction
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        .withColumn("week_reaction",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "week"))))
        .withColumn("month_reaction",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "month"))))
        .select("zone_id", "week", "month", "week_reaction", "month_reaction").distinct())
    

    df_count_subscription = (
        df_city_subscription
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        .withColumn("week_subscription",
                    (
                        F.count("user").over(
                            Window.partitionBy("zone_id", "week"))))
        .withColumn("month_subscription",
                    (
                        F.count("user").over(
                            Window.partitionBy("zone_id", "month"))))
        .select("zone_id", "week", "month", "week_subscription", "month_subscription").distinct())

    #объединения всех метрик в одину ветрину
    #df_count_message
    #df_count_reg
    #df_count_reaction
    #df_count_subscription
    df_geo_analitics_mart = (
        df_count_message.join(df_count_reg, ['zone_id','week','month'],how = 'full')
        .join(df_count_reaction, ['zone_id','week','month'], how='full')
        .join(df_count_subscription, ['zone_id','week','month'], how='full')
    )

    #Сохранение витрины для аналитиков на hdfs 
    (
        df_geo_analitics_mart
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{user}/marts/geo")
    )


if __name__ == '__main__':
    main()
