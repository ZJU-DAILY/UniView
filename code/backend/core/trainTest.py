from pack_functions import buildData_CH, train_distributed
from procConf import getModelPath





if __name__ == '__main__':
    pro_dir = getModelPath() + "/"
    wide_deep_file_pre = "wide_"
    result = "wd_estimated.text"
    # 从json文件构建原始数据
    q_data, q_keyword = buildData_CH()
    rep_q_data = []
    for i in range(64):
        rep_q_data.append(q_data[0])
    # train(rep_q_data, q_keyword, model_pre=pro_dir + wide_deep_file_pre, result_file=pro_dir + result)
    train_distributed(rep_q_data, q_keyword, model_pre=pro_dir + wide_deep_file_pre, result_file=pro_dir + result)
    # test_distributed(rep_q_data, q_keyword, model_pre=pro_dir + wide_deep_file_pre, result_file=pro_dir + result)
    # q_data, q_keyword = buildData_Test()
    # print(len(q_keyword))
    # exit()
    # test(q_data, q_keyword[:230], model_pre=pro_dir + wide_deep_file_pre, result_file=pro_dir + result)
