#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import json
import pandas as pd
import subprocess
from procConf import *
from pyspark import SparkConf
from pyspark.sql import SparkSession
import time


def get_current_time():
    now = time.localtime()
    nowTime = time.strftime("%Y-%m-%d %H:%M", now)
    return nowTime

# init pyspark
def init_pyspark(database):
    os.environ['SPARK_HOME'] = '/usr/local/spark'
    os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    if ('SPARK_HOME' not in os.environ.keys() and 'HADOOP_HOME' not in os.environ.keys()):
        raise Exception("SPARK_HOME and HADOOP_HOME is not set")
    spark_home = os.environ['SPARK_HOME']
    os.environ['PYSPARK_PYTHON'] = f"{spark_home}/python"
    os.environ['PYSPARK_DRIVER_PYTHON'] = f"{spark_home}/python"
    conf = SparkConf().setAppName(getAppName())
    conf.setMaster(getSparkMaster())
    conf.set('spark.num.executors', getSparkNumExecutors()) \
        .set('spark.executor.cores', getNumProc()) \
        .set('spark.executor.memory', getSparkExecutorMemory()) \
        .set('spark.driver.memory', getDriverMemory()) \
        .set('spark.sql.extensions', "com.huawei.boostkit.spark.OmniCache") \
        .set('spark.sql.omnicache.logLevel', 'WARN') \
        .set('spark.jars', get_cache_path())
    spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    use_database = f"use {database}"
    spark_session.sql(use_database)
    return spark_session


def print_query_plan(spark_session, query, mode="formatted"):
    spark_session.sql(query).explain(mode)


# https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html
# mode: [simple, extended, codegen, cost, formatted]
def get_query_plan(spark_session, query, mode="formatted"):
    df = spark_session.sql(query)
    return df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), mode)


def run_cmd(args_list):
    """
    run linux commands
    """
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    s_output = s_output.decode('utf-8').split('\n')
    return s_return, s_output, s_err


def get_mv_bytes(spark_session, mv_table):
    os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    desc_table = f"desc formatted {mv_table}"
    try:
        location = spark_session.sql(desc_table).filter("col_name=='Location'").collect()[0].data_type
    except:
        return None
    print(f"materialized view location: {location}")
    (ret, out, err) = run_cmd(['/usr/local/hadoop/bin/hdfs', 'dfs', '-du', '-s', location])
    # type(out) is bytes, thus, we should decode it
    if ret == 0:
        mv_table_bytes = out[0].split(' ')[0]
    else:
        mv_table_bytes = None
    return mv_table_bytes


def get_all_yarn_logs(spark_history_path='/spark2-history'):
    (ret, out, err) = run_cmd(['/usr/local/hadoop/bin/hdfs', 'dfs', '-ls', spark_history_path])
    yarn_logs_timestamp = {}
    # example log: '-rw-rw----   3 root supergroup      30426 2022-01-05 10:45 /spark2-history/application_1641349792829_0004.lz4'
    for log in out[1:]:
        log_hdfs_path = log.split(':')[-1].split(' ')[-1]
        match_str = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', log)
        if match_str and 'inprogress' not in log_hdfs_path and 'local' not in log_hdfs_path:
            time_stamp = match_str.group()
            yarn_logs_timestamp[log_hdfs_path.split('/')[-1]] = time_stamp
    return yarn_logs_timestamp


def get_latest_yarn_logs(yarn_logs, num):
    yarn_logs_timestamp = dict(sorted(yarn_logs.items(), key=lambda x: x[1], reverse=True)[:num])
    return yarn_logs_timestamp


# date format: y-m-d H:M, e.g. 2022-08-01 11:11
def get_yarn_logs_interval(yarn_logs, start_time, end_time):
    yarn_logs_interval = {}
    for log, time in yarn_logs.items():
        if start_time < time < end_time:
            yarn_logs_interval[log] = time
    return yarn_logs_interval


def generate_json_logs(logparser_jar_path, hdfs_spark_history_path, app_id, hdfs_output_path, cache_path):
    if not os.path.exists(logparser_jar_path):
        raise Exception("LogParser-1.0-SNAPSHOT.jar does not exists, please check")
    # ret1, _, _ = run_cmd(['/usr/local/hadoop/bin/hdfs', 'dfs', '-ls', hdfs_output_path])
    # if ret1 == 0:
    #     print(f"{hdfs_output_path} has already existed, skip generate json logs")
    #     return ret1
    os.environ['SPARK_HOME'] = '/usr/local/spark'
    os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    spark_conf_path = f"{os.environ['SPARK_HOME']}/conf/"
    spark_jar_path = f"{os.environ['SPARK_HOME']}/jars/*"
    hadoop_conf_path = f"{os.environ['HADOOP_HOME']}/etc/hadoop/"
    logparser_classname = "org.apache.spark.deploy.history.ParseLog"
    dependency_path = f"{spark_conf_path}:{spark_jar_path}:{hadoop_conf_path}:{logparser_jar_path}:{cache_path}"
    (ret2, out, err) = run_cmd(
        ['java', '-cp', dependency_path, logparser_classname, hdfs_spark_history_path, hdfs_output_path, app_id])
    # if ret != 0, it means execution failure
    return ret2


def parse_json_logs(hdfs_json_file):
    (ret, out, err) = run_cmd(['/usr/local/hadoop/bin/hdfs', 'dfs', '-cat', hdfs_json_file])
    if ret != 0:
        return "", "", "", "", ""
    if len(out) == 0 or out[0] == '[]':
        return "", "", "", "", ""
    json_str = json.loads(out[0])
    json_str = json_str[0]
    # json_str = json_str[0]
    return json_str['original query'], json_str['node metrics'], json_str['physical plan'], \
           json_str['dot metrics'], json_str['materialized views']

def init_pyspark_plugin(database):
    os.environ['SPARK_HOME'] = '/usr/local/spark'
    os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    if ('SPARK_HOME' not in os.environ.keys() and 'HADOOP_HOME' not in os.environ.keys()):
        raise Exception("SPARK_HOME and HADOOP_HOME is not set")
    spark_home = os.environ['SPARK_HOME']
    os.environ['PYSPARK_PYTHON'] = f"{spark_home}/python"
    os.environ['PYSPARK_DRIVER_PYTHON'] = f"{spark_home}/python"
    conf = SparkConf().setAppName(getAppName())
    conf.setMaster(getSparkMaster())
    conf.set('spark.num.executors', getSparkNumExecutors()) \
        .set('spark.executor.cores', getNumProc()) \
        .set('spark.executor.memory', getSparkExecutorMemory()) \
        .set('spark.driver.memory', getDriverMemory()) \
        .set('spark.jars', '/opt/logparse_jars/CachePlugin-1.0-SNAPSHOT.jar') \
        .set('spark.sql.extensions', 'com.huawei.boostkit.spark.OmniCache')
    spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    use_database = f"use {database}"
    spark_session.sql(use_database)
    return spark_session

def drop_mv_tables(spark_session):
    df_all_tables = spark_session.sql("show tables").toPandas()
    mv_tables = []
    try:
        for table in df_all_tables['tableName'].values:
            # TODO: judge according to the name
            if "mv" in table:
                mv_tables.append(table)
        for table in mv_tables:
            print(f"DROP MATERIALIZED VIEW {table}")
            drop_mv_sql = f"DROP MATERIALIZED VIEW {table}"
            spark_session.sql(drop_mv_sql)
    except Exception:
        print("drop materialized view error")

def stop_spark_session(spark_session):
    spark_session.stop()

def get_query_mv_mappings():
    mapping_path = getTmpPath(DataType.Q_MV)
    query_mv_mapping = pd.read_csv(mapping_path)
    query_mv_mapping.columns = ['query_sql', "mv_id"]
    create_mv_sql = []
    drop_mv_sql = []
    for mv_id in query_mv_mapping['mv_id'].values:
        mv_id_path = f"./resources/spark/mv/mv/mv{mv_id}.sql"
        with open(mv_id_path, 'r') as f:
            mv_sql = f.read()
            create_mv_sql.append(mv_sql)
            drop_mv_sql.append(f"DROP MATERIALIZED VIEW mv{mv_id}")

    query_mv_mapping['create_mv_sql'] = create_mv_sql
    query_mv_mapping['drop_mv_sql'] = drop_mv_sql
    return query_mv_mapping


# 执行一条query
def run_query(flag=False):
    # pycharm运行需要开启
    # if flag:
    #     os.environ['SPARK_HOME'] = '/usr/local/spark'
    #     os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    spark_sql_cmd = f"{os.environ['SPARK_HOME']}/bin/spark-sql --deploy-mode client \
        --driver-memory {getDriverMemory()} --num-executors {getSparkNumExecutors()} --executor-cores {getNumProc()} \
        --executor-memory {getSparkExecutorMemory()} --master {getSparkMaster()} \
        --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniCache \
        --jars /home/tmp/CachePlugin-1.0-SNAPSHOT.jar \
        --database {getDatabase()} "
    if flag:
        tpcds_path = get_tpcds_path()
    else:
        tpcds_path = get_increment_path()
    fileNames = os.listdir(tpcds_path)
    try:
        fileNames = sorted(fileNames, key=lambda x: int(x[1:-4]))
    except:
        try:
            fileNames = sorted(fileNames, key=lambda x: int(x[3:-4]))
        except:
            print("[run_query] sort error!")
    query_md5 = {}
    for file in fileNames:
        filePath = tpcds_path + file
        key, ext = os.path.splitext(os.path.basename(filePath))
        sql = ""
        with open(filePath, "r") as f:
            lines = f.readlines()
            for line in lines:
                sql += line
        print(filePath)
        subprocess.call(f"{spark_sql_cmd} --name query-excute -f {filePath}", shell=True)





# 创建一条视图，执行一条query，删除一条视图的过程
def run_query_mv_mappings(query_mv_mapping):
    # TODO: 记得修改相应的参数，比如数据库的名字，可以先print一下spark_sql_cmd
    spark_sql_cmd = f"{os.environ['SPARK_HOME']}/bin/spark-sql --deploy-mode client \
    --driver-memory {getDriverMemory()} --num-executors {getSparkNumExecutors()} --executor-cores {getNumProc()} \
    --executor-memory {getSparkExecutorMemory()} --master {getSparkMaster()} \
    --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniCache \
    --jars {get_cache_path()} \
    --database {getDatabase()}"
    spark_sql_loacl_cmd = f"{os.environ['SPARK_HOME']}/bin/spark-sql --deploy-mode client \
        --driver-memory {getDriverMemory()} --num-executors {getSparkNumExecutors()} --executor-cores {getNumProc()} \
        --executor-memory {getSparkExecutorMemory()} --master yarn \
        --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniCache \
        --jars {get_cache_path()} \
        --database {getDatabase()}"
    print(spark_sql_cmd)
    for i in range(len(query_mv_mapping)):
        # 创建mv
        create_mv_sql = query_mv_mapping['create_mv_sql'][i]
        with open('tmp.txt', 'w') as f:
            f.write(create_mv_sql)
        subprocess.call(f"{spark_sql_loacl_cmd} --name mv_create -f tmp.txt", shell=True)
        # 执行query
        query_sql = query_mv_mapping['query_sql'][i]
        with open('tmp.txt', 'w') as f:
            f.write(query_sql)
        subprocess.call(f"{spark_sql_cmd} --name query_mv_running -f tmp.txt", shell=True)
        # 删除mv
        drop_mv_sql = query_mv_mapping['drop_mv_sql'][i]
        with open('tmp.txt', 'w') as f:
            f.write(drop_mv_sql)
        subprocess.call(f"{spark_sql_loacl_cmd} --name mv_drop -f tmp.txt", shell=True)


# 获取shema文件信息
def init_database_schema():
    database = getDatabase()
    session = init_pyspark(database)
    df_all_tables = session.sql("show tables").toPandas()
    schema_sql = ''
    for table in df_all_tables['tableName'].values:
        show_create = f"show create table {table}"
        query = session.sql(show_create).collect()[0].createtab_stmt
        schema_sql += f"{query[:-1]};\n\n"
    schema_sql = schema_sql.replace('`', '')

    session.stop()
    schema_path = get_schema_file()
    with open(schema_path, "w", encoding="utf-8") as f:
        f.write(schema_sql)
    print(schema_sql)
    # write_text_to_hdfs(schema_sql, schema_path)
    print("init_database_schema success!")


# just for test
if __name__ == '__main__':
    start = get_current_time()
    try:
        init_database_schema()
    except:
        print("run fail!")
        exit(-1)
    end = get_current_time()
    print(f"start_time:{start}")
    print(f"start_time:{end}")
    print("run success!")


    # test get physical plan
    # query = 'select * from item limit 2'
    # database = getDatabase()
    # mode = 'formatted'
    # # 初始化spark，重要
    # spark_session = init_pyspark(database)
    # # 下面是获取explain的执行计划，使用不到
    # plan = get_query_plan(spark_session, query, mode)
    # print(f"Physical Plan is: \n{plan}")
    #
    # # 获取视图的大小，需要使用
    # mv_table = "mv8"
    # get_mv_bytes(spark_session, mv_table)
    # print(f"capacity of materialized table {mv_table} is: " + get_mv_bytes(spark_session, mv_table))
    #
    # # 获取所有日志，必须使用
    # yarn_logs = get_all_yarn_logs()
    # # 获取最近的几个日志，用不上
    # latest_log = get_latest_yarn_logs(yarn_logs, 20)
    # print(f"latest yarn logs:\n{latest_log}")
    #
    # # 根据yarn_logs获取一段时间的日志信息，重要
    # start_time = '2022-08-13 12:18'
    # end_time = '2022-08-13 15:00'
    # interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
    # print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")
    #
    # # 几个重要的路径
    # logparser_jar_path = get_logparser_path()
    # hdfs_spark_history_path = get_hdfs_inputpath()
    # hdfs_output_path = get_hdfs_outputpath()
    # cache_path = get_cache_path()
    #
    # # interval_logs
    # for app_id, value in interval_logs.items():
    #     ret = generate_json_logs(logparser_jar_path, hdfs_spark_history_path, app_id, hdfs_output_path, cache_path)
    #     if ret != 0:
    #         print("Generate Json log file failed!")
    #
    # app_id = "application_1655197295219_0989.lz4"
    # # extract information from json history log
    # hdfs_json_file = f"{hdfs_output_path}/{app_id}.json"
    # original_query, node_metrics, physical_plan, dot_metrics, materialized_views = parse_json_logs(hdfs_json_file)
    # print(original_query)
    # print(materialized_views)
