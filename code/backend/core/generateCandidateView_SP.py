from joinCandidate_SP import *
from get_physical_plan import *
from generate_hdfs_json_logs import *

def recommend_sp_test(k=30, threshold=100):
    mv_benefit = []


    benefit_data = list(csv.reader(open('./resources/data/clusters_data.csv', 'r')))

    for data in benefit_data:
        mv_id = data[0]
        benefit = float(data[1])
        mv_benefit.append((mv_id, benefit))

    mv_benefit.sort(key=lambda k: k[1], reverse=True)

    recommend_results = []

    for i in range(k):
        recommend_results.append(mv_benefit[i][0])

    output_path = './resources/data/recommend-mvdata.csv'
    with open(output_path, 'w', newline='') as write_file:
        for mv in recommend_results:
            write_file.write(mv)
            write_file.write('\n')


def print_time(q_time_map, end, start):
    print(q_time_map)
    sum_value = 0
    for value in q_time_map.values():
        sum_value += value
    print(f"total time of equal: {sum_value}")
    print(f"average time of equal: {sum_value / len(q_time_map)}")
    print(f"total time: {end - start}")
    print("generate success!")


# spark-生成候选视图/topn
def generate_candidate_view_SP(url="resources/spark/plan-log"):
    solver = Solver()
    clusters = {}
    g_query_subs = {}
    cntDict = {}
    g_table_common = {}  # 表->列
    q_time_map = {}
    mvPath = url + "/mv/sql/"
    realMVPath = url + "/mv"
    clusters_ds_path = realMVPath + "/ds/"
    check_file_path_exists()
    start = time.time()
    # 1.解析执行计划生成多棵树
    buildPlanTrees_SP(url, clusters, g_query_subs, cntDict, solver, g_table_common, q_time_map)
    check_clusters_common_SP(clusters, g_table_common)
    idWithCnt, initIDS = getInitMVs_SP(g_query_subs, clusters, cntDict, method="topk")
    saveTmpQueryMap_SP(g_query_subs, initIDS)
    
    # 2.生成候选视图
    generate_all_mvs(clusters, 0, mvPath, initIDS, realMVPath)

    # 3.将clusters/g_query_subs/cntDict信息存入二进制ds文件
    save_all_gv_SP(clusters, g_query_subs, cntDict, clusters_ds_path)
    end = time.time()

    # 输出时间信息
    print_time(q_time_map, end, start)



# 最新spark生成候选视图
def generate_candidate_view_new_SP(url="resources/spark", is_incre=False):
    solver = Solver()   # z3的解决工具
    clusters = {}       # 候选视图集
    g_query_subs = {}   # query->视图集
    cntDict = {}        #
    g_table_common = {} # 表->列
    q_time_map = {}     # 时间map
    check_file_path_exists()
    mvPath = url + "/mv/sql/"
    realMVPath = url + "/mv"
    clusters_ds_path = realMVPath + "/ds/"

    # 加载之前生成候选视图的信息
    clusters, g_query_subs, cntDict = load_para_incre_SP(realMVPath)

    start = time.time()
    # 1.解析执行计划生成多棵树
    build_plan_trees_new_SP(clusters, g_query_subs, cntDict, g_table_common, solver, q_time_map, is_incre)
    idWithCnt, initIDS = getInitMVs_SP(g_query_subs, clusters, cntDict, method="topk")
    if is_incre:
        saveTmpQueryMap_SP(g_query_subs, initIDS, get_increment_path(), is_incre)
    else:
        saveTmpQueryMap_SP(g_query_subs, initIDS, get_tpcds_path(), is_incre)
    
    # 2.生成候选视图
    generate_all_mvs(clusters, 0, mvPath, initIDS, realMVPath)

    # 3.将clusters/g_query_subs/cntDict信息存入二进制ds文件
    save_all_gv_SP(clusters, g_query_subs, cntDict, clusters_ds_path)

    end = time.time()
    # 输出时间信息
    print_time(q_time_map, end, start)


def load_para_incre_SP(realMVPath):
    clusters = get_candidate_gv_SP(realMVPath)
    if not clusters:
        return dict(), dict(), dict()
    g_query_subs_path = get_g_query_subs_path()
    cnt_dict_path = get_cnt_dict_path()
    try:
        with open(g_query_subs_path, "rb") as f:
            g_query_subs = pickle.load(f)
    except:
        return dict(), dict(), dict()
    try:
        with open(cnt_dict_path, "rb") as f:
            cntDict = pickle.load(f)
    except:
        return dict(), dict(), dict()
    return clusters, g_query_subs, cntDict


if __name__ == "__main__":
    
    generate_candidate_view_new_SP("resources/spark")
    # generate_candidate_view_SP()

    # generate_top_n()

