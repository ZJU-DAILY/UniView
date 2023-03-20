#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import json
import pandas as pd
import subprocess
import time
# from procConf import *
from pyspark import SparkConf
from pyspark.sql import SparkSession

def init_pyspark(database):
    os.environ['SPARK_HOME'] = '/usr/local/spark'
    os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    # if ('SPARK_HOME' not in os.environ.keys() and 'HADOOP_HOME' not in os.environ.keys()):
    #     raise Exception("SPARK_HOME and HADOOP_HOME is not set")
    # spark_home = os.environ['SPARK_HOME']
    # os.environ['PYSPARK_PYTHON'] = f"{spark_home}/python"
    # os.environ['PYSPARK_DRIVER_PYTHON'] = f"{spark_home}/python"
    conf = SparkConf().setAppName("mview")
    conf.setMaster("yarn")
    conf.set('spark.num.executors', 18) \
        .set('spark.executor.cores', 4) \
        .set('spark.executor.memory', "48g") \
        .set('spark.driver.memory', "48g") \
        .set('spark.sql.extensions', "com.huawei.boostkit.spark.OmniCache") \
        .set("spark.sql.omnicache.logLevel", "WARN") \
        .set('spark.jars', "hdfs://172.1.15.26:9000/user/root/CachePlugin-1.0-SNAPSHOT.jar")
    spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    use_database = f"use {database}"
    spark_session.sql(use_database)
    return spark_session


# 使用shell脚本执行99条sql
def shell_run_all_sql(sql_path="/home/tmp/tpcds"):
    shell_command = "source /etc/profile; spark-sql --deploy-mode client --driver-cores 5 --driver-memory 5g" \
                    " --num-executors 18 --executor-cores 21 --executor-memory 55g --master" \
                    " yarn --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniCache" \
                    " --conf spark.sql.omnicache.logLevel=WARN --database tpcds_bin_orc_5" \
                    " --jars /home/tmp/CachePlugin-1.0-SNAPSHOT.jar" \
                    " -f /home/tmp/tpcds/"
    start_time = time.time()
    time_map = {}
    for root, dirs, files in os.walk(sql_path):
        for f in files:
            start_time1 = time.time()
            run_command = shell_command + f
            subprocess.call(run_command, shell=True)
            end_time1 = time.time()
            running_time1 = end_time1 - start_time1
            print(f, running_time1)
            time_map[f] = running_time1
    end_time = time.time()
    running_time = end_time - start_time
    print(running_time)
    return running_time, time_map

# # 使用shell脚本执行一条sql
# def shell_run_one_sql():


def get_running_time(spark_session):
    time_map = {}
    start_time = time.time()
    succ_len = 0
    for root, dirs, files in os.walk('/home/tmp/tpcds'):
        for f in files:
            start_time1 = time.time()
            try:
                print("*****************************")
                sql_file = os.path.join(root, f)
                sqlstr = get_sqlstr(sql_file)
                # spark_session.sql(sqlstr).show()
                spark_session.sql(sqlstr).collect()
            except Exception as e:
                # print(str(e))
                end_time_f = time.time()
                print(f,"fail",end_time_f-start_time1)
            else:
                end_time_s = time.time()
                print(f,"succ",end_time_s-start_time1)
                time_map[f] = end_time_s-start_time1
                succ_len += 1
            print("*****************************")
            print("")
    end_time = time.time()
    running_time = end_time-start_time
    print("----------")
    return running_time, time_map, succ_len


def get_sqlstr(sql_file):
    sql = open(sql_file, 'r', encoding='utf8')
    sqltxt = sql.readlines()
    sql.close()
    sql = "".join(sqltxt)
    return sql

def create_view(spark_session):
    for root, dirs, files in os.walk('/root/mview0814/mview/resources/spark/mv/topMV'):
        for f in files:
            try:
                sql_file = os.path.join(root, f)
                sqlstr = get_sqlstr(sql_file)
                spark_session.sql(sqlstr).cache()
                print(f,"111111111")
            except:
                print(f,"222222222")

def remove_view(spark_session):
    for root, dirs, files in os.walk('/root/mview0814/mview/resources/spark/mv/topMV'):
        for f in files:
            try:
                remove_sh = "drop materialized view " + f[:-4] + ";"
                spark_session.sql(remove_sh ).cache()
                print(f,"111111111")
            except:
                print(f,"222222222")

def stop_spark_session(spark_session):
    spark_session.stop()

if __name__ == '__main__':
    #database name
    database = "tpcds_bin_orc_5"
    # # 1.原始sql
    # running_time1, time_map1 = shell_run_all_sql()
    # # 2.创建top视图
    # spark_session = init_pyspark(database)
    # create_view(spark_session)
    # stop_spark_session(spark_session)
    # # 3.物化sql
    # running_time2, time_map2 = shell_run_all_sql()
    # # 4.删除视图
    # spark_session = init_pyspark(database)
    # remove_view(spark_session)
    # stop_spark_session(spark_session)
    # # 输出
    # for k, v in time_map2.items():
    #     if time_map1[k] > v:
    #         print(k, time_map1[k], v, "true")
    #     else:
    #         print(k, time_map1[k], v)

    # 1.获取原始sql运行时间
    spark_session = init_pyspark(database)
    running_time1, time_map1, succ_len1 = get_running_time(spark_session)
    stop_spark_session(spark_session)
    # 2.创建top视图
    spark_session = init_pyspark(database)
    create_view(spark_session)
    stop_spark_session(spark_session)
    # 3.获取创建视图后sql运行时间
    spark_session = init_pyspark(database)
    running_time2, time_map2, succ_len2 = get_running_time(spark_session)
    stop_spark_session(spark_session)
    # 4.删除视图
    spark_session = init_pyspark(database)
    remove_view(spark_session)
    stop_spark_session(spark_session)

    # 输出结果
    print(' Running time: {},{} (Seconds). '.format(running_time1, running_time2))
    # print(time_map1)
    # print(time_map2)
    print("len:")
    print(succ_len1)
    print(succ_len2)
    for k, v in time_map2.items():
        if time_map1[k] > v:
            print(k, time_map1[k], v, "true")
        else:
            print(k, time_map1[k], v)

