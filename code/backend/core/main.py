#! /usr/bin/env python3
import sys
import pickle
# from encodeFeatures_SP import buildData_SP
from encodeFeatures import generate_build_data, generate_projection_data
from generateCandidateView_SP import generate_candidate_view_new_SP
from jobWideDeep import train_distributed, test_distributed, update_distributed, train, test
from procConf import getModelPath, DataType, getResultPath
# from recommend import recommend
from new_recommend import recommend
from recommend import recommend_dqn
from getCandidate import getCandidate, build_projection, create_projection, remove_projection
from test_run_encode import *
from generate_hdfs_json_logs import *
import torch
import random
import numpy as np
from read_mv import generate_view_size
from get_physical_plan import init_database_schema
from test_run_query_mappings import run_query, run_query_mv
from generate_hdfs_json_logs import generate_all_json_logs
from get_json_PG import get_json_file_PG, get_json_file_para_PG
from time import time
from get_qmv_run_time import calculate_cost
from read_mv import get_generate_materialized_view_size


database_types = ["CH", "PG", "spark"]
tasks = ["cost_estimation", "update_model", "recommend", "get_candidates", "init_database_schema", "get_recommend_csv_mv", "build_data", "build_projection", "create_projection",
         "generate_projection_data", "generate_view_size", "run_rewrite_query", "test_encode_run", "remove_projection", "get_json_file_PG",
         "run_original_query_tpcds", "run_original_query_incre", "get_original_log_tpcds", "get_original_log_incre", "get_rewrite_log_tpcds", "get_rewrite_log_incre",
         "get_candidates_spark_tpcds", "get_candidates_spark_incre", "test_encode_run_tpcds", "test_encode_run_incre", "calculate_cost", "get_generate_materialized_view_size"]


def set_seed(seed):
    if seed != -1:
        random.seed(seed)
        np.random.seed(seed)
        torch.manual_seed(seed)
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        os.environ['PYTHONHASHSEED'] = str(seed)


def clear_recommend_file():
    try:
        with open(getResultPath(DataType.Q), "w") as f:
            pass
    except:
        print(f"info: {getResultPath(DataType.Q)} file not found!")
    try:
        with open(getResultPath(DataType.MV), "w") as f:
            pass
    except:
        print(f"info: {getResultPath(DataType.MV)} file not found!")
    try:
        with open(getResultPath(DataType.Q_MV), "w") as f:
            pass
    except:
        print(f"info: {getResultPath(DataType.Q_MV)} file not found!")
    try:
        with open(getRecommendMvCSV(), "w") as f:
            pass
    except:
        print(f"info: resources/data/recommend-mvdata.csv file not found!")


def run(task="recommend", database_type="spark", para3="resources/PG/q_mv/sql", para4="resources/PG/q_mv/json"):
    if database_type not in database_types:
        print("database type " + database_type + " not support.")
        return False
    if task not in tasks:
        print("task " + task + " not support.")
        return False

    # 先从文件中取出训练数据
    try:
        if database_type == "spark":
            with open('./resources/data/q_data.txt', 'rb') as f:
                q_data = pickle.load(f)
            with open('./resources/data/mv_data.txt', 'rb') as f:
                mv_data = pickle.load(f)
            with open('./resources/data/qmv_data.txt', 'rb') as f:
                qmv_data = pickle.load(f)
            with open('./resources/data/candidate_mv_data.txt', 'rb') as f:
                candidate_mv_data = pickle.load(f)
            with open('./resources/data/candidate_qmv_data.txt', 'rb') as f:
                candidate_qmv_data = pickle.load(f)
            with open('./resources/data/q_keyword.txt', 'rb') as f:
                q_keyword = pickle.load(f)
        elif database_type == "PG":
            with open('./resources/data/q_data.txt', 'rb') as f:
                q_data = pickle.load(f)
            with open('./resources/data/mv_data.txt', 'rb') as f:
                mv_data = pickle.load(f)
            with open('./resources/data/qmv_data.txt', 'rb') as f:
                qmv_data = pickle.load(f)
            with open('./resources/data/candidate_mv_data.txt', 'rb') as f:
                candidate_mv_data = pickle.load(f)
            with open('./resources/data/q_keyword.txt', 'rb') as f:
                q_keyword = pickle.load(f)
        elif database_type == "CH":
            with open('./resources/data/q_data.txt', 'rb') as f:
                q_data = pickle.load(f)
            with open('./resources/data/mv_data.txt', 'rb') as f:
                mv_data = pickle.load(f)
            with open('./resources/data/qmv_data.txt', 'rb') as f:
                qmv_data = pickle.load(f)
            with open('./resources/data/candidate_mv_data.txt', 'rb') as f:
                candidate_mv_data = pickle.load(f)
            with open('./resources/data/q_keyword.txt', 'rb') as f:
                q_keyword = pickle.load(f)
        else:
            print(f"[main] error:The system only support spark/PG/CH, not support {database_type}")
            return False
    except:
        print("if you are in recommend step, please check file if exists!")

    # 根据参数类型执行脚本
    if task == "init_database_schema":
        init_database_schema()
    elif task == "get_recommend_csv_mv":
        if get_use_projection() == "True" and database_type == "CH":
            get_recommend_projection_mv()
        else:
            get_recommend_csv_mv()
    elif task == "cost_estimation":
        if get_recommend_method() == "RL":
            train(mv_data, q_keyword, replacedquery=False)
            train(qmv_data, q_keyword, replacedquery=True)
        else:
            train(mv_data, q_keyword, replacedquery=False)
            train(qmv_data, q_keyword, replacedquery=True)
    elif task == "recommend":
        # 先清空文件，文件不存在无需处理
        clear_recommend_file()
        if get_recommend_method() == "RL":
            all_mv_data = mv_data + candidate_mv_data
            test(qmv_data, DataType.Q_MV, replacedquery=True)
            test(q_data, DataType.Q, replacedquery=False)
            test(all_mv_data, DataType.MV, replacedquery=False)
            recommend_dqn()
        else:
            all_mv_data = mv_data + candidate_mv_data
            test(qmv_data, DataType.Q_MV, replacedquery=True)
            test(q_data, DataType.Q, replacedquery=False)
            test(all_mv_data, DataType.MV, replacedquery=False)
            # topk 或者 固定存储阈值（单位MB）
            recommend(topk=True, k=getMvCntLimit(), threshold=get_mv_space_limit())
    elif task == "get_candidates":
        if database_type == "PG":
            getCandidate(database_type)
        elif database_type == "CH":
            getCandidate(database_type)
        else:
            print(f"[main] error:The system only support PG/CH, not support {database_type}")
            return False
    elif task == "get_candidates_spark_tpcds":
        if database_type == "spark":
            generate_candidate_view_new_SP("resources/spark", False)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "get_candidates_spark_incre":
        if database_type == "spark":
            generate_candidate_view_new_SP("resources/spark", True)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "build_data":
        if database_type == "PG":
            generate_build_data(database_type)
        elif database_type == "CH":
            generate_build_data(database_type)
        else:
            print(f"[main] error:The system only support spark/PG/CH, not support {database_type}")
            return False
    elif task == "create_projection":
        if database_type == "CH":
            create_projection()
        else:
            print(f"[main] error:create_projection only support spark/PG/CH, not support {database_type}")
            return False
    elif task == "build_projection":
        if database_type == "CH":
            build_projection()
        else:
            print(f"[main] error:build_projection only support spark/PG/CH, not support {database_type}")
            return False
    elif task == "generate_projection_data":
        if database_type == "CH":
            generate_projection_data()
        else:
            print(f"[main] error:generate_projection_data only support spark/PG/CH, not support {database_type}")
            return False
    elif task == "generate_view_size":
        if database_type == "spark":
            generate_view_size()
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "run_original_query_tpcds":
        if database_type == "spark":
            run_query(True)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "run_original_query_incre":
        if database_type == "spark":
            run_query(False)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "run_rewrite_query":
        if database_type == "spark":
            run_query_mv()
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "get_original_log_tpcds":
        if database_type == "spark":
            generate_all_json_logs(True, False)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "get_original_log_incre":
        if database_type == "spark":
            generate_all_json_logs(True, True)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "get_rewrite_log_tpcds":
        if database_type == "spark":
            generate_all_json_logs(False, False)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "get_rewrite_log_incre":
        if database_type == "spark":
            generate_all_json_logs(False, True)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "test_encode_run_tpcds":
        if database_type == "spark":
            test_encode_run()
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "test_encode_run_incre":
        if database_type == "spark":
            test_encode_run(True)
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "remove_projection":
        if database_type == "CH":
            remove_projection()
        else:
            print(f"[main] error:The system only support CH, not support {database_type}")
            return False
    elif task == "get_json_file_PG":
        if database_type == "PG":
            get_json_file_para_PG(para3, para4)
        else:
            print(f"[main] error:The system only support CH, not support {database_type}")
            return False
    elif task == "calculate_cost":
        if database_type == "spark":
            calculate_cost()
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False
    elif task == "get_generate_materialized_view_size":
        if database_type == "spark":
            get_generate_materialized_view_size()
        else:
            print(f"[main] error:The system only support spark, not support {database_type}")
            return False


    return True



if __name__ == '__main__':
    # start, start_x = get_current_time()
    begin_y = time()
    set_seed(2022)
    if len(sys.argv) == 3:
        run_flag = run(sys.argv[1], sys.argv[2])
    else:
        print(f"error: [main] para need 2 but you just input {len(sys.argv)}! please check para!")
    # end, end_x = get_current_time()
    end_y = time()
    # print(f"start_time:{start}")
    # print(f"start_time:{end}")
    print(end_y - begin_y)
    # 输出结果
    if len(sys.argv) == 3 and run_flag:
        print("run success!")
    else:
        print("run fail!")

# python main.py recommend PG

# glpsol --cpxlp /tmp/5dce8b63271445ccbd6497691aa76f69-pulp.lp -o /tmp/5dce8b63271445ccbd6497691aa76f69-pulp.sol

