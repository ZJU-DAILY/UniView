import configparser
from enum import Enum

__PG__ = 0
__CH__ = 1


class DataType(Enum):
    Q = 0
    MV = 1
    Q_MV = 2


def initConfig():
    config = configparser.ConfigParser()
    config.read("windows_config.cfg", encoding="utf-8")
    # config.read("config.cfg", encoding="utf-8")
    return config


def getRawPath(dataType=DataType.Q, isInc=False):
    if isInc:
        if DataType.Q == dataType:
            itemName = "inc_query_path"
        elif DataType.MV == dataType:
            itemName = "inc_mv_path"
        elif DataType.Q_MV == dataType:
            itemName = "inc_q_mv_path"
        else:
            return
    else:
        if DataType.Q == dataType:
            itemName = "query_path"
        elif DataType.MV == dataType:
            itemName = "mv_path"
        elif DataType.Q_MV == dataType:
            itemName = "q_mv_path"
        else:
            return
    return g_cfg.get("rawFile", itemName)


def getResultPath(dataType=DataType.Q):
    if DataType.Q == dataType:
        itemName = "query_csv"
    elif DataType.MV == dataType:
        itemName = "mv_csv"
    elif DataType.Q_MV == dataType:
        itemName = "q_mv_csv"
    else:
        return
    return g_cfg.get("csv", itemName)


def getTmpPath(dataType=DataType.Q):
    if DataType.Q == dataType:
        itemName = "temp_query_csv"
    elif DataType.MV == dataType:
        itemName = "temp_mv_csv"
    elif DataType.Q_MV == dataType:
        itemName = "temp_q_mv_csv"
    else:
        return
    return g_cfg.get("csv", itemName)

def getCandidateQMVMappings():
    return g_cfg.get("csv", "candidate_q_mv_csv")

def getModelPath():
    return g_cfg.get("modle_pre", "wd_modle")


def getTrainRate():
    return float(g_cfg.get("train", "train_rate"))

def getBatchSize():
    return int(g_cfg.get("train", "batch_size"))

def getEpoch():
    return int(g_cfg.get("train", "epoch"))

def getLearningRate():
    return float(g_cfg.get("train", "learning_rate"))

def getWeightDecay():
    return float(g_cfg.get("train", "weight_decay"))

def getModelSavePath(replace_query=True):
    if replace_query:
        return g_cfg.get("train", "model_save_path_q_mv")
    else:
        return g_cfg.get("train", "model_save_path_q")

def getKeywordEmbeddingSize():
    return int(g_cfg.get("train", "keyword_embedding_size"))

def getCharEmbeddingSize():
    return int(g_cfg.get("train", "char_embedding_size"))

def getNodeAuxiliarySize():
    return int(g_cfg.get("train", "node_auxiliary_size"))

def getFirstHiddenSize():
    return int(g_cfg.get("train", "first_hidden_size"))

def getSecondHiddenSize():
    return int(g_cfg.get("train", "second_hidden_size"))

def getDropRate():
    return float(g_cfg.get("train", "drop_rate"))

def get_train_type():
    return float(g_cfg.get("train", "train_type"))

def getNumProc():
    return int(g_cfg.get("distributed", "num_processors"))

def getMaxNumKeywordPerNode():
    return int(g_cfg.get("distributed", "max_num_keyword_per_node"))

def getMaxNumNode():
    return int(g_cfg.get("distributed", "max_num_node"))

def getHvdStorePath():
    return g_cfg.get("distributed", "hvd_store_path")

def getSparkMaster():
    return g_cfg.get("spark", "spark_master")

def getPySparkPython():
    return g_cfg.get("spark", "pyspark_python")

def getPySparkDrivePython():
    return g_cfg.get("spark", "pyspark_drive_python")

def getAppName():
    return g_cfg.get("spark", "app_name")

def getSparkNumExecutors():
    return g_cfg.get("spark", "spark_num_executors")

def getSparkExecutorMemory():
    return g_cfg.get("spark", "spark_executor_memory")

def getDriverMemory():
    return g_cfg.get("spark", "spark_driver_memory")

def getMvCntLimit():
    return int(g_cfg.get("mv_limit", "cnt_limit"))

def get_mv_space_limit():
    return int(g_cfg.get("mv_limit", "space_limit"))

def getThreshold():
    return float(g_cfg.get("mv_limit", "time_threshold"))


def getColThreshold():
    return float(g_cfg.get("mv_limit", "col_threshold"))


def getBatchSizeRL():
    return int(g_cfg.get("rl", "batch_size_rl"))

def getLearningRateRL():
    return float(g_cfg.get("rl", "learning_rate_rl"))

def getEpsilon():
    return float(g_cfg.get("rl", "epsilon"))

def getGamma():
    return float(g_cfg.get("rl", "gamma"))

def getTargetReplaceIer():
    return int(g_cfg.get("rl", "target_replace_iter"))

def getMemoryCapacity():
    return int(g_cfg.get("rl", "memory_capacity"))

def getDropRateRL():
    return float(g_cfg.get("rl", "drop_rate_rl"))

def getRunId():
    return g_cfg.get("distributed", "run_id")

def getRecommendMvCSV():
    return g_cfg.get("csv", "recommend_mv_csv")

def getRemoveMvCSV():
    return g_cfg.get("csv", "remove_mv_csv")

def getProjectionList(schemaDict):
    tables = []
    try:
        tablesStr = g_cfg.get("projection_list", "table_list")
    except:
        return tables
    if len(tablesStr.strip()) == 0:
        return tables
    tables = [table.strip().upper() for table in tablesStr.split(",") if
              len(table.strip()) != 0 and table.strip().upper() in schemaDict]

    return tables


def getRefThreshold():
    refCnt = 2
    try:
        refCnt = g_cfg.get("mv_limit", "refer_threshold")
        return int(refCnt)
    except:
        return int(refCnt)


def getDatabase():
    return g_cfg.get("rawFile", "database")

def get_logparser_path():
    return g_cfg.get("rawFile", "logparser_jar_path")

def get_cache_path():
    return g_cfg.get("rawFile", "cache_path")

def get_hdfs_inputpath():
    return g_cfg.get("rawFile", "hdfs_input_path")

def get_hdfs_outputpath():
    return g_cfg.get("rawFile", "hdfs_output_path")

def get_query_log_timestamp():
    return g_cfg.get("rawFile", 'q_log_start_time'), g_cfg.get("rawFile", 'q_log_end_time')

def get_query_mv_log_timestamp():
    return g_cfg.get("rawFile", 'q_mv_log_start_time'), g_cfg.get("rawFile", 'q_mv_log_end_time')

def get_query_incre_log_timestamp():
    return g_cfg.get("rawFile", 'q_log_incre_start_time'), g_cfg.get("rawFile", 'q_log_incre_end_time')

def get_query_mv_incre_log_timestamp():
    return g_cfg.get("rawFile", 'q_mv_log_incre_start_time'), g_cfg.get("rawFile", 'q_mv_log_incre_end_time')

def get_g_query_subs_path():
    return g_cfg.get("rawFile", "g_query_subs_path")

def get_cnt_dict_path():
    return g_cfg.get("rawFile", "cnt_dict_path")

def get_mv_bytes_dict_path():
    return g_cfg.get("rawFile", "mv_bytes_dict_path")

def get_tpcds_path():
    return g_cfg.get("rawFile", "tpcds_path")

def get_increment_path():
    return g_cfg.get("rawFile", "increment_path")

# 视图选择的权重
def get_selection_query_ratio():
    return g_cfg.get("mv_selection", "query_ratio")

def get_selection_space_ratio():
    return g_cfg.get("mv_selection", "space_ratio")

def get_selection_filter_ratio():
    return g_cfg.get("mv_selection", "filter_ratio")

def get_table_size_upper_bound():
    return g_cfg.get("mv_selection", "table_size_upper_bound")

def get_table_size_lower_bound():
    return g_cfg.get("mv_selection", "table_size_lower_bound")

def get_table_size_big_bound():
    return g_cfg.get("mv_selection", "table_size_big_bound")

def get_table_size_small_bound():
    return g_cfg.get("mv_selection", "table_size_small_bound")

def get_sort_mv_test_csv():
    return g_cfg.get("module_test", "sort_mv_test_csv")

def get_CH_IP():
    return g_cfg.get("module_test", "CH_IP")

def get_CH_USER():
    return g_cfg.get("module_test", "CH_USER")

def get_CH_PASSWD():
    return g_cfg.get("module_test", "CH_PASSWD")

def get_spark_cnt_limit_flag():
    return g_cfg.get("module_test", "spark_cnt_limit_flag")

def get_schema_file():
    return g_cfg.get("schema", "fullpath")

def get_PG_schema_path():
    return g_cfg.get("schema", "PG_schema_path")

def get_engine():
    return g_cfg.get("schema", "engine")

def get_recommend_method():
    return g_cfg.get("schema", "recommend_method")

def get_use_projection():
    return g_cfg.get("projection_list", "use_projection")

def get_min_projection():
    return g_cfg.get("projection_list", "min_projection")


g_cfg = initConfig()
