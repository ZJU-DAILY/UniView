from procConf import *
from get_physical_plan import *
import sys
import csv
from mv_util import *
import pickle

# 将日志获取下来
def generate_all_json_logs(flag=True, is_incre=False):
    logparser_jar_path = get_logparser_path()
    hdfs_spark_history_path = get_hdfs_inputpath()
    hdfs_output_path = get_hdfs_outputpath()
    cache_path = get_cache_path()
    database = getDatabase()
    # 获取所有的日志信息
    yarn_logs = get_all_yarn_logs()
    if flag:
        if is_incre:
            start_time, end_time = get_query_incre_log_timestamp()
        else:
            start_time, end_time = get_query_log_timestamp()
    else:
        if is_incre:
            start_time, end_time = get_query_mv_incre_log_timestamp()
        else:
            start_time, end_time = get_query_mv_log_timestamp()
    # 选择出时间内的日志
    interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
    print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")
    i = 0
    # 获取所有的日志
    for app_id, value in interval_logs.items():
        ret = generate_json_logs(logparser_jar_path, hdfs_spark_history_path, app_id, hdfs_output_path, cache_path)
        if ret != 0:
            print("Generate Json log file failed!")
        else:
            i += 1
    print(f"start from {start_time} to {end_time}. generate total logs len: {i}")
    print("success!")


def get_recommend_csv_mv():
    id_mv = {}
    # 确保文件存在
    os.makedirs(getRawPath(DataType.MV) + "/recommend", exist_ok=True)
    os.makedirs(getRawPath(DataType.MV) + "/recommend/mv", exist_ok=True)
    with open("./resources/data/id_mv.csv") as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            mv_id = row[0]
            if "mv" in row[0]:
                mv_id = mv_id[2:]
            mv_id = int(mv_id)
            id_mv[mv_id] = row[2]
    with open(getRecommendMvCSV(), "r") as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            mv_id = row[0]
            if "mv" in mv_id:
                mv_id = mv_id[2:]
            mv_id = int(mv_id)
            with open(getRawPath(DataType.MV) + "/recommend/mv/mv" + str(mv_id) + ".sql", "w") as f:
                f.write(id_mv[mv_id])


def get_recommend_projection_mv():
    id_mv = {}
    # 确保文件存在
    os.makedirs(getRawPath(DataType.MV) + "/recommend", exist_ok=True)
    os.makedirs(getRawPath(DataType.MV) + "/recommend/mv", exist_ok=True)
    remove_directory_file(getRawPath(DataType.MV) + "/recommend/mv")
    with open("./resources/data/id_mv.csv") as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            mv_id = row[0]
            if "mv" in row[0]:
                mv_id = mv_id[2:]
            mv_id = int(mv_id)
            id_mv[mv_id] = row[2]
    p_list = []
    # id->table name
    with open("resources/data/projection_table_dict.ds", "rb") as f:
        projection_table_dict = pickle.load(f)
    with open(getRecommendMvCSV(), "r") as f:
        f_csv = csv.reader(f)
        for row in f_csv:
            mv_id = row[0]
            if "mv" in mv_id:
                mv_id = mv_id[2:]
            mv_id = int(mv_id)
            mv_name = f"{projection_table_dict[mv_id]}-cube{mv_id}.sql"
            with open(getRawPath(DataType.MV) + "/recommend/mv/" + mv_name, "w") as f:
                f.write(id_mv[mv_id])
            p_list.append(mv_id)
    # 如果少于最小数量
    # min_pro = int(get_min_projection())
    # if len(p_list) < min_pro:
    #     for mv_id, value in id_mv.items():
    #         if mv_id in p_list:
    #             continue
    #         with open(getRawPath(DataType.MV) + "/recommend/mv/mv" + str(mv_id) + ".sql", "w") as f:
    #             f.write(id_mv[mv_id])
    #         p_list.append(mv_id)
    #         if len(p_list) >= min_pro:
    #             break



if __name__ == "__main__":
    # get_recommend_csv_mv()
    if len(sys.argv) == 1:
        generate_all_json_logs()
    elif len(sys.argv) == 2:
        if sys.argv[1] == "original_query":
            generate_all_json_logs()
        elif sys.argv[1] == "rewrite_query":
            generate_all_json_logs(False)
        else:
            print("error input!")
    else:
        print("error input!")