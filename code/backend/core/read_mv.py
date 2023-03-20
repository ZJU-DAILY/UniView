import os
import subprocess
import time
from encodeFeatures_SP import *
import sys
from mv_util import *


# 读取topMV下的所有mv，生成各种脚本
def read_mv_name(path = "./resources/spark/mv/topmv"):
    os.makedirs("resources/spark/mv/log/create_log", exist_ok=True)
    remove_command = ""
    remove = "drop materialized view if exists mv"
    files = os.listdir(path)
    s = ""
    for file in files:
        mv_name = file.split(".")[0].split("mv")[1]
        s += " " + mv_name
        remove_command += remove + mv_name + ";\n"
    print(s)
    # 创建command
    command = "#!/bin/bash\n"
    command += "for idx in" + s + "\n"
    command += "do\n"
    command += "    echo \"#\"$idx" + "\n"
    command += f"    spark-sql --deploy-mode client --driver-cores 5 --driver-memory {getDriverMemory()}" \
               f" --num-executors {getSparkNumExecutors()} --executor-cores {getNumProc()} --executor-memory {getSparkExecutorMemory()} --master" \
               f" yarn --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniCache" \
               f" --database {getDatabase()} --jars {get_cache_path()}" \
               f" -f resources/spark/mv/topmv/mv$idx.sql 2>&1 | " \
               f"tee resources/spark/mv/log/create_log/test$idx.log" + "\n"
    command += "done\n"
    # print(command)
    # print(remove_command)
    with open("./resources/spark/mv/log/create_mv.sh", "w") as f:
        f.write(command)
    with open("./resources/spark/mv/log/remove_mv_command.txt", "w") as f:
        f.write(remove_command)

    # 删除command
    remove_shell = "#!/bin/bash\n"
    remove_shell += f"spark-sql --deploy-mode client --driver-cores 5 --driver-memory {getDriverMemory()}" \
               f" --num-executors {getSparkNumExecutors()} --executor-cores {getNumProc()} --executor-memory {getSparkExecutorMemory()} --master" \
               f" yarn --conf spark.sql.extensions=com.huawei.boostkit.spark.OmniCache" \
               f" --database {getDatabase()} --jars {get_cache_path()}" \
               f" -f ./resources/spark/mv/log/remove_mv_command.txt 2>&1 | " \
               f"tee ./resources/spark/mv/log/remove_log/test$idx.log" + "\n"
    with open("./resources/spark/mv/log/remove_mv.sh", "w") as f:
        f.write(remove_shell)


# 创建top30mv
def create_top30_mv():
    subprocess.call(f"source /etc/profile; sh ./resources/spark/mv/log/create_mv.sh", shell=True)


# 删除top30mv
def remove_top30_mv():
    subprocess.call(f"source /etc/profile; sh ./resources/spark/mv/log/remove_mv.sh", shell=True)


# 执行改写脚本后台执行，测试覆盖率，一对一的情况
def execute_rewrite_cover_one(PROJECT_PATH=".", ENV_NAME="env", log="output.log"):
    subprocess.call(f"cd {PROJECT_PATH}/mview", shell=True)
    subprocess.call(f"rm -rf {log}", shell=True)
    subprocess.call(f"conda activate {ENV_NAME}", shell=True)
    subprocess.call(f"source /etc/profile; nohup python -u test_run_query_mappings.py > {log} 2>&1 &", shell=True)


# 执行改写脚本后台执行，测试覆盖率，一对多的情况
def execute_rewrite_cover_more(PROJECT_PATH="/home/xu/shellScript", ENV_NAME="env", log="output.log"):
    subprocess.call(f"rm -rf {PROJECT_PATH}/{log}", shell=True)
    subprocess.call(f"source /etc/profile; nohup sh {PROJECT_PATH}/newQuery.sh > {PROJECT_PATH}/{log} 2>&1 &", shell=True)


# 执行encode_sp得到覆盖率
def execute_get_cover(isInc=False):
    q_mv_dict = parse_qmv_data_SP(DataType.Q_MV, isInc)
    print("----------------------")
    print(len(q_mv_dict))


def get_size_by_table(database, table):
   (ret, out, err) = run_cmd(
      ['psql', '-p', '5432', '-d', f'{database}', '-c', f"select pg_relation_size('{table}');"])
   if ret == 0:
      print(out)
      mv_table_size = out[2].strip()
   else:
      mv_table_size = None
   return mv_table_size


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


def get_tables_bytes_bound_PG():
   table_bytes = {}
   for table in g_table.data:
      table = table.lower()
      size = get_size_by_table("imdbload", table)
      print(size)
      table_bytes[table] = int(size)
   return table_bytes


def get_tables_bytes_bound_ds():
   with open("./resources/data/table_bytes_pg.ds", "rb") as f:
      table_bytes = pickle.load(f)
   return table_bytes


# 计算top视图的大小，写入文件
def generate_materialized_view_size(path="./resources/spark/mv/topmv"):
    spark_session = init_pyspark(getDatabase())
    files = os.listdir(path)
    s = ""
    mv_bytes_dict = {}
    topmv_remove_list = []
    for file in files:
        mv_name = file.split(".")[0]
        mv_bytes = get_mv_bytes(spark_session, mv_name)
        mv_bytes_dict[mv_name] = mv_bytes
        if not mv_bytes:
            topmv_remove_list.append(int(mv_name[2:]))
    stop_spark_session(spark_session)
    # 写入ds文件
    path = get_mv_bytes_dict_path()
    with open(path, "wb") as f:
        pickle.dump(mv_bytes_dict, f)
    # 无法创建的sql写入ds文件
    with open("./resources/data/topmv_remove_list.ds", "wb") as f:
        pickle.dump(topmv_remove_list, f)


# 获取创建视图大小
def get_generate_materialized_view_size():
    path = get_mv_bytes_dict_path()
    with open(path, "rb") as f:
        mv_bytes_dict = pickle.load(f)
    total_cost = 0.0
    for key, cost in mv_bytes_dict.items():
        if cost:
            cost = float(cost)
            total_cost += cost
    print(f"total_cost:{total_cost}")
    return total_cost



# 获取PG的topmv的大小
def generate_materialized_view_size_PG(path="./resources/spark/mv/topmv"):
    files = os.listdir(path)
    mv_bytes_dict = {}
    for file in files:
        mv_name = file.split(".")[0]
        mv_bytes = get_size_by_table("imdbload", mv_name)
        mv_bytes_dict[mv_name] = mv_bytes
    # 写入ds文件
    path = get_mv_bytes_dict_path()
    with open(path, "wb") as f:
        pickle.dump(mv_bytes_dict, f)


def get_mv_rewrite_data(url="/home/tmp/tpcds/"):
    hdfs_output_path = get_hdfs_outputpath()
    logparser_jar_path = get_logparser_path()
    hdfs_spark_history_path = get_hdfs_inputpath()
    cache_path = get_cache_path()
    query_md5 = get_table_file_from_exist(get_tpcds_path())
    yarn_logs = get_all_yarn_logs()
    start_time, end_time = get_query_mv_log_timestamp()
    interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
    print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")
    # 获取所有的日志
    # for app_id, value in interval_logs.items():
    #     generate_json_logs(logparser_jar_path, hdfs_spark_history_path, app_id, hdfs_output_path, cache_path)

    q_dict = {}
    mv_dict = {}
    for app_id, value in interval_logs.items():
        # 获取一个query的执行计划
        hdfs_json_file = f"{hdfs_output_path}/{app_id}.json"
        original_query, node_metrics, physical_plan, dot_metrics, materialized_views = parse_json_logs(
            hdfs_json_file)
        if "MATERIALIZED VIEW" in original_query:
            print("this is physical plan of create/drop materialzed view")
            continue
        if materialized_views == "":
            print("warn: [parse_qmv_data_SP] not a q-mv query but a common query")
            continue
        if original_query == "" or node_metrics == "" or physical_plan == "" or dot_metrics == "":
            print(app_id)
            print("warn: [parse_qmv_data_SP] all 4 file must exists!")
            continue
        print(app_id)
        # 获取信息
        if sql2md5(original_query) not in query_md5:
            print("can't find original_query!")
            continue
        sql_name = query_md5[sql2md5(original_query)]
        q_dict[sql_name] = materialized_views
        if materialized_views not in mv_dict:
            mv_dict[materialized_views] = [sql_name]
        else:
            mv_dict[materialized_views].append(sql_name)

    return q_dict, mv_dict


def create_views():
    spark_session = init_pyspark(getDatabase())
    mv_path = getRawPath(DataType.MV)
    topn_path = mv_path + "/topmv/"
    for root, dirs, files in os.walk(topn_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_content = load_file(file_path)
            try:
                spark_session.sql(file_content).show()
                print(file, "create succ")
            except:
                print(file, "create fail")

    stop_spark_session(spark_session)


def remove_views():
    spark_session = init_pyspark(getDatabase())
    for n in range(600):
        mv = "mv"+str(n)
        try:
            remove_sh = "drop materialized view " + mv + ";"
            spark_session.sql(remove_sh).cache()
            print(mv, "remove succ")
        except Exception as e:
            # print(e)
            print(mv, "remove fail")
    print("The historical view is deleted!")
    stop_spark_session(spark_session)


# 加上explain检查视图是否命中
# def explain_view_cover(is_incre=False):
#     if is_incre:
#         files_path = get_increment_path()
#     else:
#         file_paths = get_tpcds_path()
#     for root, dirs, files in os.walk(files_path):
#         for file in files:
#             file_path = os.path.join(root, file)
#             file_content = load_file(file_path)
#             explain_file_content = f"explain {file_content}"




def generate_view_size(is_incre=False):
    create_views()
    generate_materialized_view_size()
    remove_views()


if __name__ == "__main__":
    start_time = get_current_time()
    generate_view_size()

    # create_views()

    # read_mv_name()
    # create_top30_mv()
    # generate_materialized_view_size()
    # remove_top30_mv()
    # execute_rewrite_cover_more()
    # execute_get_cover(True)
    # spark_session = init_pyspark(getDatabase())
    # spark_session.sql("drop materialized view mv34;").show()
    # stop_spark_session(spark_session)

    end_time = get_current_time()
    print("-----------------------")
    print(start_time)
    print(end_time)
    print("-----------------------")