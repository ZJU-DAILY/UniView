from generateCandidateView_SP import generate_candidate_view_new_SP
from get_physical_plan import *
import time
from encodeFeatures import *
from encodeFeatures_CH import *
from encodeFeatures_SP import *

# 获取当前时间->str eg. 2022-08-25 14:40
def get_current_time():
    now = time.localtime()
    nowTime = time.strftime("%Y-%m-%d %H:%M", now)
    return nowTime


#
def get_candidate_time():
    os.environ['SPARK_HOME'] = '/usr/local/spark'
    os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    # 1. 根据已经执行过的tpcds99数据，生成query-mv映射关系
    generate_candidate_view_new_SP("resources/spark")
    # 2. 获取query-mv映射关系
    query_mv_mappings = get_query_mv_mappings()
    print("=======================================")
    print("generate_candidate_view_new_SP success!")
    print("=======================================")
    # 3. 执行query_mv 映射关系，运行一次就行，这一步非常耗时！
    # # TODO: run_query_mv_mappings函数里面记得修改相应的参数，比如数据库的名字，可以先print一下spark_sql_cmd
    start_time = get_current_time()
    run_query_mv_mappings(query_mv_mappings)
    end_time = get_current_time()
    print(start_time)
    print(end_time)
    print("get_candidate_time end.")


# 生成qmv的时间
def generate_qmv_time():
    # 获取所有的日志信息3
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
    print("generate_qmv_time end.")


database_types = ["click_house", "PG", "spark"]
tasks = ["cost_estimation", "update_model", "recommend", "get_candidates"]
# 获得txt等各种文件
def generate_qmv_txt(task="recommend", database_type="spark"):
    if database_type not in database_types:
        print("database type " + database_type + " not support.")
        return
    if task not in tasks:
        print("task " + task + " not support.")
        return


    # 从json文件构建原始数据
    q_data = None
    qmv_data = None
    q_keyword = None
    mv_data = None
    candidate_mv_data = None
    if database_type == "click_house":
        q_data, q_keyword = buildData_CH()
    elif database_type == "PG":
        q_data, q_keyword = buildData()
    elif database_type == "spark":
          q_data, qmv_data, mv_data, candidate_mv_data, candidate_qmv_data, q_keyword = buildData_SP()

    # 写入txt文件
    with open('./resources/data/candidate_qmv_data.txt', 'wb') as f:
        pickle.dump(candidate_qmv_data, f)
    with open('./resources/data/q_data.txt', 'wb') as f:
        pickle.dump(q_data, f)
    with open('./resources/data/qmv_data.txt', 'wb') as f:
        pickle.dump(qmv_data, f)
    with open('./resources/data/mv_data.txt', 'wb') as f:
        pickle.dump(mv_data, f)
    with open('./resources/data/candidate_mv_data.txt', 'wb') as f:
        pickle.dump(candidate_mv_data, f)
    with open('./resources/data/q_keyword.txt', 'wb') as f:
        pickle.dump(q_keyword, f)

    print("generate_qmv_txt end!")


def calculate_cost():
    q_path = getResultPath(DataType.Q)
    mv_path = getResultPath(DataType.MV)
    qmv_path = getResultPath(DataType.Q_MV)
    total_origin_cost = 0.0
    q_dict = {}
    with open(q_path) as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            key, cost = line[0], float(line[1])
            q_dict[key] = cost
            total_origin_cost += cost
    total_rewrite_cost = total_origin_cost
    qmv_dict = {}
    with open(qmv_path) as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            key, cost = line[0].split("-")[0].strip(), float(line[1])
            qmv_dict[key] = cost
            if key in q_dict:
                total_rewrite_cost = total_rewrite_cost - q_dict[key] + cost
    total_mv_cost = 0.0
    mv_dict = {}
    with open(mv_path) as f:
        csv_reader = csv.reader(f)
        i = 0
        for line in csv_reader:
            key, cost = line[0], float(line[1])
            mv_dict[key] = cost
            i += 1
            total_mv_cost += cost
            if i > int(getMvCntLimit()):
                break

    print(f"total_mv_cost:{total_mv_cost}")
    print(f"total_rewrite_cost:{total_rewrite_cost}")
    print(f"total_origin_cost:{total_origin_cost}")
    ratio = 1 - (total_mv_cost + total_rewrite_cost) / total_origin_cost
    print(f"ratio:{ratio}")


if __name__ == "__main__":
    # 1.执行get_candidate_time()得到时间
    # 2.执行generate_qmv_time()生成日志
    # 3.执行generate_qmv_txt生成txt文件，注意，这里需要创建mv和删除mv

    start_time = get_current_time()

    # calculate_cost()
    get_candidate_time()
    # generate_qmv_time()
    # generate_qmv_txt()

    end_time = get_current_time()
    print("-----------------------")
    print(start_time)
    print(end_time)
    print("-----------------------")