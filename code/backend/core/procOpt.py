#! /usr/bin/env python3
import random

import pulp
# import random
import copy
from procCSVFile import *
from procConf import *
# from pulp import GLPK
from mv_util import get_current_time


def getRawData():
    queries, mvs, q_mvs, recommend_mvs, mv_edge, q_dict, mv_dict, recommend_mv_dict, q_mv_dict = loadCSVFile_CH(getResultPath(DataType.Q),
                                                                           getResultPath(DataType.MV),
                                                                           getResultPath(DataType.Q_MV),
                                                                              getRecommendMvCSV())
    overlapping_dict = {}  # building overlap contraint
    for mv1, v1 in mv_edge.items():
        overlapping_dict[mv1] = {}
        for mv2, v2 in mv_edge.items():
            if mv1 == mv2:
                overlapping_dict[mv1][mv2] = 0
            elif len(set(v1).intersection(set(v2))) != 0:
                overlapping_dict[mv1][mv2] = 1
            else:
                overlapping_dict[mv1][mv2] = 0
    return queries, mvs, recommend_mvs, q_mvs, mv_edge, overlapping_dict, q_dict, mv_dict, recommend_mv_dict, q_mv_dict


class Graph(object):
    def __init__(self, queries, mvs, recommend_mvs, q_mvs, mv_edges, overlapping_dict, constraint=1):
        self.queries = queries
        self.mvs = mvs
        self.recommend_mvs = recommend_mvs
        self.q_mvs = q_mvs
        self.mv_edges = mv_edges
        self.overlapping_dict = overlapping_dict
        self.utility = self.computeBenefitDict()
        self.total_overhead = np.sum([mv.getOverhead() for k, mv in self.mvs.items()])
        self.constraint = constraint * self.total_overhead
        # print(self.utility)

    def getTotalCost(self):
        total = 0
        for i, q in self.queries.items():
            total += q.cost
        return total

    def computeBenefitDict(self):
        b_dict = {}
        for key, value in self.q_mvs.items():
            q_id, mv_id = key
            print("key:{}, q:{}, q-mv:{}".format(key, self.queries[q_id].cost, value.cost))
            real_benefit = self.queries[q_id].cost - value.cost
            b_dict[key] = real_benefit
        return b_dict

    def detectOverlap(self, q_id, cur_mv_id, Y):
        cur_value = np.sum(
            [Y.get((q_id, mv_id), 0) * self.overlapping_dict.get((cur_mv_id, mv_id), 1) for mv_id in self.mvs if
             mv_id != cur_mv_id])
        return cur_value > 0

    def iterOpt(self, n):
        Z = {z: 0 for z in self.mvs}    # mv是否被选中
        Y = {key: 0 for key in self.q_mvs}  # q-mv是否被选中
        B_max = {z: 0 for z in self.mvs}  # max benefit, 所有可以用z替换的q都用z替换后得到的benefit
        B_cur = {z: 0 for z in self.mvs}  # current benefit，当前Y的取值下，用z替换后得到的benefit
        O_cur = 0  # current overhead
        # initialize
        all_flip = 0
        res_Z = None
        res_Y = None
        max_estimated = -np.inf
        max_all_flip = 0
        random.seed()
        for z in Z:
            if z in self.recommend_mvs.keys():
                Z[z] = 1
            else:
                Z[z] = 0
            if Z[z] > 0 and O_cur + self.mvs[z].getOverhead() * Z[z] > self.constraint:
                Z[z] = 0
                continue
            O_cur += self.mvs[z].getOverhead() * Z[z]    # mv的总成本
            B_max[z] = np.sum([self.utility.get((q_id, z), 0.0) for q_id in self.queries])  # 所有q对于此mv的收益和
            for q in self.queries:
                if Z[z] > 0 and self.utility.get((q, z), None) is not None and self.detectOverlap(q, z, Y) is False:
                    Y[(q, z)] = random.randint(0, 1)
            B_cur[z] = np.sum([self.utility.get((q, z), 0) * Y.get((q, z), 0) for q in self.queries])   # 所有被随机选中的q对于此mv的收益和
        random.seed()
        for iter in range(n):
            print(f"{get_current_time()}:[iterOpt] cur_range: {iter}")
            t = np.random.rand()
            Z, O_cur, flip_num = self.zOpt(Z, O_cur, B_max, B_cur, t)
            Y, B_cur = self.yOpt(Y, B_cur, Z)
            estimated_utility = self.computotalUtility(Z, Y)
            print(f"{get_current_time()}: z-y {Z}, {Y}")
            print(f"{get_current_time()}: f-e {flip_num}, {estimated_utility}")
            all_flip += flip_num
            if estimated_utility > max_estimated:
                max_estimated = estimated_utility
                max_all_flip = all_flip
                res_Z = copy.deepcopy(Z)
                res_Y = copy.deepcopy(Y)
        print(f"{get_current_time()}: max_all_flip = {max_all_flip}")
        return res_Z, res_Y

    def zOpt(self, Z, O_cur, B_max, B_cur, t):
        B_cur_all = np.sum([v for k, v in B_cur.items()])
        B_max_all = np.sum([v for k, v in B_max.items()])
        O_max = np.sum([v.getOverhead() for k, v in self.mvs.items()])
        flip_num = 0
        for z in Z:
            B_max_z = B_max[z]
            B_cur_z = B_cur[z]
            if Z[z] == 0:
                p_flip = (1 - O_cur / O_max) * ((B_max_z / self.mvs[z].getOverhead()) / (B_max_all / O_max))
            else:
                p_flip = (self.mvs[z].getOverhead() / O_cur) * (1 - B_cur_z / B_cur_all)
            if p_flip >= t:
                if Z[z] == 0 and O_cur + self.mvs[z].getOverhead() > self.constraint:
                    continue
                Z[z] = 1 - Z[z]
                flip_num += 1
                O_cur += (1 if Z[z] > 0 else -1) * self.mvs[z].getOverhead()
        return Z, O_cur, flip_num

    def yOpt(self, Y, B_cur, Z):
        for q in self.queries:
            relative_mvs = []
            for z in self.mvs:
                if self.q_mvs.get((q, z), None) is not None:
                    relative_mvs.append(z)
            if len(relative_mvs) == 0:
                continue
            variables = [pulp.LpVariable(z, lowBound=0, upBound=1, cat=pulp.LpInteger) for z in relative_mvs]
            objective = sum([self.utility.get((q, z), 0) * variables[index] for index, z in enumerate(relative_mvs)])

            constraints = [variables[index] <= Z[z] for index, z in enumerate(relative_mvs)]
            # constraints += ([self.q_mvs[(q, z)].cost * variables[index] for index, z in enumerate(relative_mvs)])*(-1)
            constraints += [variables[index] + sum(
                [self.overlapping_dict[z][z_2] * variables[index_2] for index_2, z_2 in enumerate(relative_mvs) if
                 index_2 != index]) <= 1 for index, z in enumerate(relative_mvs)]
            res = self.solve_ilp(variables, objective, constraints)
            if res is None:
                print("solve ilp is wrong!")
                continue
            for index, z in enumerate(relative_mvs):
                if Y[(q, z)] != res[index]:
                    Y[(q, z)] = res[index]
                    if B_cur is not None:
                        B_cur[z] += (1 if Y[(q, z)] > 0 else -1) * self.utility[(q, z)]
        return Y, B_cur

    def solve_actual_ilp(self):
        z_keys = list(self.mvs.keys())
        variables_Z = [pulp.LpVariable("Z_{}".format(z), lowBound=0, upBound=1, cat=pulp.LpInteger) for z in z_keys]
        objective = [-self.mvs[z].getOverhead() * variables_Z[index] for index, z in enumerate(z_keys)]
        constraints = []
        for q in self.queries:
            relative_mvs = []
            for z in z_keys:
                if self.q_mvs.get((q, z), None) is not None:
                    relative_mvs.append(z)
            if len(relative_mvs) == 0:
                continue
            # print(q,relative_mvs)
            varibales_YZ = [pulp.LpVariable("{}_{}".format(q, z), lowBound=0, upBound=1, cat=pulp.LpInteger) for z in
                            relative_mvs]
            objective += [self.utility[(q, z)] * varibales_YZ[index] for index, z in enumerate(relative_mvs)]
            constraints += [varibales_YZ[index] <= variables_Z[z_keys.index(z)] for index, z in enumerate(relative_mvs)]
            constraints += [varibales_YZ[index] + sum(
                [self.overlapping_dict[z][z_2] * varibales_YZ[index_2] for index_2, z_2 in enumerate(relative_mvs) if
                 index_2 != index]) <= 1 for index, z in enumerate(relative_mvs)]
        # res = self.solve_ilp(objective, constraints)
        objective = sum(objective)

        prob = pulp.LpProblem('LP1', pulp.LpMaximize)

        prob.addVariables(variables_Z)
        prob.addVariables(varibales_YZ)
        prob += objective
        for cons in constraints:
            prob += cons
        status = prob.solve()
        print("end ilp solver........")
        z_labels = {}
        q_z_lables = {}
        if status != 1:
            print("solve ilp is wrong!")
            return None, None
        else:
            # res =  [v.varValue.real for v in prob.variables()]
            # print(res)
            for v in prob.variables():
                # print(v.name, "=", v.varValue)
                name, value = v.name, v.varValue.real
                if name.startswith("Z"):
                    z_labels[name[2:]] = value
                else:
                    q, z = name.split("_")
                    q_z_lables[(q, z)] = value
            print(z_labels, q_z_lables)
            # print("total utility: {}".format(self.computotalUtility(z_labels, q_z_lables)))
            return z_labels, q_z_lables

    def solve_ilp(self, varibales, objective, constraints):
        # print("start ilp solver........")
        # print(objective)
        # print(constraints)
        # print(varibales)
        prob = pulp.LpProblem('LP1', pulp.LpMaximize)
        prob.solver = pulp.getSolver("GLPK_CMD")
        # prob.solver = pulp.getSolver("COIN_CMD")
        print(varibales)
        prob.addVariables(varibales)
        prob += objective
        for cons in constraints:
            prob += cons
        # status = prob.solve()
        # print("end ilp solver........")
        status = prob.solve()
        if status != 1:
            return None
        else:
            # return [v.varValue.real for v in prob.variables()]
            return [v.varValue.real if v.varValue is not None else 0 for v in prob.variables()]

    def getTotocalCost(self):
        total = 0
        for i, q in self.queries.items():
            total += q.cost
        return total

    def computotalUtility(self, Z, Y):
        overhead = np.sum([Z[z] * self.mvs[z].getOverhead() for z in Z])
        benefit = 0
        for q, v in self.queries.items():
            utility = []
            for z in self.mvs:
                if self.utility.get((q, z), None) is not None and Y.get((q, z), 0) > 0:
                    utility.append(self.utility.get((q, z), 0) * Y.get((q, z), 0))
            c = np.sum(utility) if len(utility) > 0 else 0
            benefit += c
        utility = benefit - overhead
        print("overhead:{},benefit:{}".format(overhead, benefit))
        return utility

    def computeRatio(self, Z, Y):
        utility = self.computotalUtility(Z, Y)
        cost = self.getTotocalCost()
        # cost = 0
        # for q, v in self.queries.items():
        #     orgCost = []
        #     for z in self.mvs:
        #         if self.utility.get((q, z), None) is not None and Y.get((q, z), 0) > 0:
        #             orgCost.append(Z[z] * self.queries[q].cost)
        #     c = np.sum(orgCost) if len(orgCost) > 0 else 0
        #     cost += c
        ratio = utility / cost
        print("cost:{},utility:{}, ratio:{}".format(cost, utility, ratio))
        return ratio

    def computeTime(self, Z, Y):
        curTime = 0
        orgTime = 0
        for q, v in self.queries.items():
            ct = []
            # ot = []
            orgTime += self.queries[q].time
            for z in self.mvs:
                if self.utility.get((q, z), None) is not None and Y.get((q, z), 0) > 0:
                    #       ot.append(Z[z] * self.queries[q].time)
                    ct.append(Z[z] * self.q_mvs[(q, z)].time)
                # else:
                #    ct.append(self.queries[q].time)
            # orgT = np.sum(ot) if len(ot) > 0 else 0
            curT = np.sum(ct) if len(ct) > 0 else self.queries[q].time
            curTime += curT
            # orgTime += orgT
        ratio = (orgTime - curTime) / orgTime
        print("query time:{},q-mvtime:{}, ratio:{}".format(orgTime, curTime, ratio))
        return ratio

    def printTimeAndCost(self, Z, Y):
        for z in Z:
            if 1 != Z[z]:
                continue
            print("z:{}, overhead:{}".format(z, self.mvs[z].getOverhead()))
            for q, v in self.queries.items():
                if self.utility.get((q, z), None) is not None and Y.get((q, z), 0) > 0:
                    print("q-z:{}-{},utility:{},q-cost:{}, q-mv-cost:{}".format(q, z, self.utility.get((q, z), 0),
                                                                                self.queries[q].cost,
                                                                                self.q_mvs[(q, z)].cost))
                    print(
                        "q-z:{}-{},q-time:{}, q-mv-time:{}".format(q, z, self.queries[q].time, self.q_mvs[(q, z)].time))


if __name__ == "__main__":
    from pulp import *

    # pulpTestAll()

    # Variables
    x = LpVariable('x')
    y = LpVariable('y')

    # Problem
    prob = LpProblem('problem', LpMinimize)

    # Constraints
    prob += x + y <= 1
    prob += x <= 1
    prob += -2 + y <= 4

    # Objective function to minimize
    # prob +=

    # Solve the problem
    # status = prob.solve(GLPK(msg=0))