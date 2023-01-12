#!/usr/bin/env python
import sys
import pickle
from encodeFeatures import buildData
from encodeFeatures_CH import buildData_CH
from encodeFeatures_SP import buildData_SP
# from generateCandidateView_SP import generate_candidate_view_new_SP
# from jobWideDeep import train_distributed, test_distributed, update_distributed, train, test
# from procConf import getModelPath, DataType, getResultPath
# from recommend import recommend
# from getCandidate import getCandidate
import time



def get_current_time():
    now = time.localtime()
    nowTime = time.strftime("%Y-%m-%d %H:%M", now)
    return nowTime


def test_encode_run(is_incre=False):
    q_data = None
    qmv_data = None
    q_keyword = None
    mv_data = None
    candidate_mv_data = None
    # 从json文件构建原始数据
    q_data, qmv_data, mv_data, candidate_mv_data, candidate_qmv_data, q_keyword = buildData_SP(False, is_incre)

    # 导入文件
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




if __name__ == '__main__':
    start = get_current_time()
    test_encode_run()
    end = get_current_time()
    print(f"start_time:{start}")
    print(f"start_time:{end}")
    print("run success!")

