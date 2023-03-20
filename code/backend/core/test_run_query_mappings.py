#!/usr/bin/env python
# -*- coding: utf-8 -*-

from generateCandidateView_SP import generate_candidate_view_new_SP
from get_physical_plan import *
import time
import sys

def get_current_time():
    now = time.localtime()
    nowTime = time.strftime("%Y-%m-%d %H:%M", now)
    return nowTime

# start = time.time()
# # 1. 根据已经执行过的tpcds99数据，生成query-mv映射关系
# # generate_candidate_view_new_SP("resources/spark")
#
# # 2. 获取query-mv映射关系
# query_mv_mappings = get_query_mv_mappings()
#
# # 3. 执行query_mv 映射关系，运行一次就行，这一步非常耗时！
# # # TODO: run_query_mv_mappings函数里面记得修改相应的参数，比如数据库的名字，可以先print一下spark_sql_cmd
# run_query_mv_mappings(query_mv_mappings)
# end = time.time()
# print(f"start_time:{start}")
# print(f"start_time:{end}")


def run_query_mv():
    # 获取query-mv映射关系
    query_mv_mappings = get_query_mv_mappings()
    # 执行query_mv 映射关系，运行一次就行，这一步非常耗时！
    run_query_mv_mappings(query_mv_mappings)


if __name__ == "__main__":
    start = get_current_time()
    # argv[1]:执行的类型
    # argv[1]: original_query->原始查询, rewrite_query->改写查询
    if len(sys.argv) == 1:
        run_query()
    elif len(sys.argv) == 2:
        if sys.argv[1] == "original_query":
            run_query()
        elif sys.argv[1] == "rewrite_query":
            run_query_mv()
    print(sys.argv)
    end = get_current_time()
    print(f"start_time:{start}")
    print(f"start_time:{end}")
    print("run success!")


'''
# 如果这里需要执行generate_json_logs，这里还需要配置一次log日志的起止时间
# 获取所有的日志信息
yarn_logs = get_all_yarn_logs()
# 调用shell脚本获取一段时间内query
start_time, end_time = get_query_mv_log_timestamp()
# 指定jar包路径和输入、输出路径
logparser_jar_path = get_logparser_path()
hdfs_spark_history_path = get_hdfs_inputpath()
hdfs_output_path = get_hdfs_outputpath()
cache_path = get_cache_path()
# 选择出时间内的日志
interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")
# 获取所有的日志
for app_id, value in interval_logs.items():
    generate_json_logs(logparser_jar_path, hdfs_spark_history_path, app_id, hdfs_output_path, cache_path)

for app_id, value in interval_logs.items():
    # 获取一个query的执行计划
    hdfs_json_file = f"{hdfs_output_path}/{app_id}.json"
    original_query, node_metrics, physical_plan, dot_metrics, materialized_views = parse_json_logs(
        hdfs_json_file)
    # 如果是create或者drop mv的话，则跳过
    if "MATERIALIZED VIEW" in original_query:
        print("this is physical plan of create/drop materialzed view")
        continue
    # 不会空的话，表示query被mv改写了
    if materialized_views != "":
        print(f"using mv: {materialized_views}")
'''
