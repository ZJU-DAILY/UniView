import pulp
import random
import torch
import torch.nn as nn
from procCSVFile import *
from jobWideDeep import *

# 超参数
BATCH_SIZE = getBatchSizeRL()
LR = getLearningRateRL()  # learning rate
# 强化学习的参数
EPSILON = getEpsilon()  # greedy policy
GAMMA = getGamma()  # reward discount
TARGET_REPLACE_ITER = getTargetReplaceIer()  # target update frequency
MEMORY_CAPACITY = getMemoryCapacity()
drop_rate = getDropRateRL()

class IlpEnv(object):
    def __init__(self, graph, Z, Y):
        self.graph = graph
        self.action_space = [mv for mv in graph.mvs]
        self.n_action = len(self.action_space)
        self.state1_space = [mv for mv in graph.mvs]
        self.state2_space = [key for key in graph.q_mvs]
        self.n_state = len(self.state2_space) + len(self.state1_space)
        self.Z, self.Y = Z, Y
        self.utility = self.graph.computotalUtility(Z, Y)

    def normalState(self):
        return [self.Z[z] for z in self.state1_space] + [self.Y[qmv] for qmv in self.state2_space]

    def computeOverhead(self, graph):
        overhead = 0
        for z, v in self.Z.items():
            overhead += v * graph.mvs[z].getOverhead()
        return overhead

    def step(self, action, graph):
        mv = self.action_space[action]
        if self.Z[mv] == 0 and self.graph.mvs[mv].getOverhead() + self.computeOverhead(graph) > graph.constraint:
            reward = 0
        else:
            self.Z[mv] = 1 - self.Z[mv]
            pre_Y = self.Y
            self.Y, _ = self.graph.yOpt(self.Y, None, self.Z)
            cur_utility = self.graph.computotalUtility(self.Z, self.Y)
            reward = (cur_utility - self.utility) * 10
            if reward < 0:
                self.Z[mv] = 1 - self.Z[mv]
                self.Y = pre_Y
                cur_utility = self.utility
                reward = 0
            self.utility = cur_utility
        reward = 1 if reward > 0 else (-1 if reward < 0 else 0)
        return self.normalState(), reward

    def reset(self, Z, Y):
        self.Z, self.Y = Z, Y
        self.utility = self.graph.computotalUtility(Z, Y)

    def computeUtility(self):
        return self.graph.computotalUtility(self.Z, self.Y)


class Net(nn.Module):
    def __init__(self, n_states, n_actions):
        super(Net, self).__init__()
        self.fc1 = nn.Linear(n_states, 16).to(device)
        self.fc1.weight.data.normal_(0, 0.1)  # 初始化
        self.fc2 = nn.Linear(16, 64).to(device)
        self.fc2.weight.data.normal_(0, 0.1)  # 初始化
        self.fc3 = nn.Linear(64, 128).to(device)
        self.fc3.weight.data.normal_(0, 0.1)  # 初始化
        self.fc4 = nn.Linear(128, 64).to(device)
        self.fc4.weight.data.normal_(0, 0.1)  # 初始化
        self.layers = nn.Sequential(
            self.fc1,
            nn.Dropout(drop_rate),
            nn.ReLU(),
            self.fc2,
            nn.Dropout(drop_rate),
            nn.ReLU(),
            self.fc3,
            nn.Dropout(drop_rate),
            nn.ReLU(),
            self.fc4,
            nn.Dropout(drop_rate),
        ).to(device)
        self.value_layer = nn.Linear(64, 1).to(device)
        self.advantage_layer = nn.Linear(64, n_actions).to(device)
        # self.softmax = nn.Softmax()

    def forward(self, x):
        #4*(mv+q_mv)=>4*64
        x = self.layers(x)
        # 4*64=>4*1
        actions_value = self.value_layer(x).to(device)
        # 4*64=>4*mv
        actions_advantage = self.advantage_layer(x).to(device)
        # 4*mv
        out = torch.add(actions_value, actions_advantage)
        return out


class DQN(object):
    def __init__(self, n_states, n_actions):
        self.eval_net, self.target_net = Net(n_states, n_actions).to(device), Net(n_states, n_actions).to(device)
        self.n_states, self.n_actions = n_states, n_actions
        # 记录学习到多少步
        self.learn_step_counter = 0  # for target update
        self.memory_counter = 0  # for storing memory
        # 初始化memory
        self.memory = np.zeros((MEMORY_CAPACITY, n_states * 2 + 2))
        self.optimizer = torch.optim.Adam(self.eval_net.parameters(), lr=LR)
        self.loss_func = nn.MSELoss()

    def choose_action(self, x):
        x = torch.FloatTensor(x).to(device).unsqueeze(0)
        if np.random.uniform() < EPSILON or self.learn_step_counter > 100:
            action_value = self.eval_net.forward(x)
            # print(torch.max(action_value, 1))
            action = torch.max(action_value, 1)[1].data.cpu().numpy()[0]
        else:  # random
            action = np.random.randint(0, self.n_actions)
        return action

    # s:当前状态， a:动作, r:reward奖励, s_:下一步状态
    def store_transaction(self, s, a, r, s_):
        transaction = np.hstack((s, [a, r], s_))
        # replace the old memory with new memory
        index = self.memory_counter % MEMORY_CAPACITY
        self.memory[index, :] = transaction
        self.memory_counter += 1

    def learn(self):
        # target net update
        if self.learn_step_counter % TARGET_REPLACE_ITER == 0:
            self.target_net.load_state_dict(self.eval_net.state_dict())

        self.learn_step_counter += 1

        if self.memory_counter < MEMORY_CAPACITY:
            array_size = self.memory_counter
        else:
            array_size = MEMORY_CAPACITY

        sample_index = np.random.choice(array_size, BATCH_SIZE)
        b_memory = self.memory[sample_index, :]
        b_s = torch.FloatTensor(b_memory[:, :self.n_states]).to(device)
        b_a = torch.LongTensor(b_memory[:, self.n_states: self.n_states + 1].astype(int)).to(device)
        b_r = torch.FloatTensor(b_memory[:, self.n_states + 1: self.n_states + 2]).to(device)
        b_s_ = torch.FloatTensor(b_memory[:, -self.n_states:]).to(device)

        q_eval = self.eval_net(b_s).gather(1, b_a)
        q_next = self.target_net(b_s_).detach()
        q_target = b_r + GAMMA * q_next.max(1)[0].unsqueeze(1)
        # print(q_eval.shape,q_next.shape,q_next.max(1),b_r.shape)
        loss = self.loss_func(q_eval, q_target)
        self.optimizer.zero_grad()
        loss.backward()
        self.optimizer.step()
