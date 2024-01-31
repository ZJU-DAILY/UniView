#! /usr/bin/env python3
from jobWideDeep import *
from procOpt import *
from jobRL import *





def recommend_dqn():
    n1 = 10
    n2 = 1

    # 将wide deep模型结果作为输入,进行mv选择训练
    queries, mvs, recommend_mvs, q_mvs, mv_edge, overlapping_dict, q_dict, mv_dict, recomend_mv_dict, q_mv_dict = getRawData()
    graph = Graph(queries, mvs, recommend_mvs, q_mvs, mv_edge, overlapping_dict, constraint=1)
    Z, Y = graph.iterOpt(n1)

    env = IlpEnv(graph, Z, Y)
    dqn = DQN(env.n_state, env.n_action)
    print('\nCollecting experience...')
    for i in range(n2):
        print(f"{get_current_time()}:[recommend_dqn] currrent step is {i}")
        env.reset(Z, Y)
        s = env.normalState()
        t = 0
        while True:
            a = dqn.choose_action(s)
            # take action
            s_, r = env.step(a, graph)
            dqn.store_transaction(s, a, r, s_)
            if dqn.memory_counter > MEMORY_CAPACITY:
                dqn.learn()
                # print(env.computeUtility())
            s = s_
            t += 1
            if r <= 0 and t > len(Z):
                break
    Z, Y = env.Z, env.Y
    print("result:")
    selectedZ = []
    selectedZKeys = []
    for k, v in Z.items():
        if v == 1:
            selectedZ.append((k, mvs[k]))
            selectedZKeys.append(k)
    if len(selectedZ) != 0:
        appendBaseCSVFile(selectedZ, getRecommendMvCSV())
    removeMvs = []
    for k, v in recommend_mvs.items():
        if k not in selectedZKeys:
            removeMvs.append((k, v))
    if len(removeMvs) != 0:
        appendBaseCSVFile(removeMvs, getRemoveMvCSV())
    print("z:{}".format(selectedZ))
    print("y:{}".format(Y))
    ratio = graph.computeRatio(Z, Y)
    print(ratio)


if __name__ == '__main__':
    recommend_dqn()
