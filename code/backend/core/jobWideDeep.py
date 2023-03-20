# encoding: utf-8
from statistics import mean
from typing import List

import numpy as np
import torch

import torch.nn as nn
from torch import Tensor
# import horovod.spark.torch as hvd
# from horovod.spark.common.backend import SparkBackend
# from horovod.spark.common.store import HDFSStore, Store, FilesystemStore
# from pyspark.ml.linalg import DenseVector, SparseVector
# from sklearn.metrics import mean_absolute_percentage_error

from procCSVFile import *
from procSparkEnv import get_distributed_spark_session

# device = torch.device("cuda:{}".format("0") if torch.cuda.is_available() else "cpu")
device = "cpu"


def str2int(strs):
    # return list(filter(lambda x: x < 128, [ord(ch) < 128 for ch in strs]))
    return list(ord(ch) % 128 for ch in strs)
    #
    # length = len(strs)

    # # results = np.zeros((length, 128))
    # results = [([0] * 128) for i in range(length)]
    # for i, character in enumerate(strs):
    #     if ord(character) < 128:
    #         index = ord(character)
    #         results[i][index] = 1
    # return results


def mae_loss():
    return torch.nn.SmoothL1Loss()


def mse_loss():
    return torch.nn.MSELoss()


def rmse_loss():
    return np.sqrt(torch.nn.MSELoss)


class MAPELoss(torch.nn.Module):
    def __init__(self, eps=1e-6):
        super().__init__()
        self.mae = torch.nn.SmoothL1Loss(reduction='none')
        self.eps = eps

    def forward(self, input, target):
        loss = 100. * torch.mean(self.mae(input, target) / (torch.abs(target) + self.eps))
        return loss

def mape(y_true, y_pred):
    return np.mean(np.abs((y_pred - y_true) / y_true)) * 100


def mape_loss():
    return MAPELoss()


def reset_parameters(module):
    for m in module.modules():
        if not isinstance(m, nn.Sequential):
            if hasattr(m, 'weight') and m.weight is not None:
                if 1 != m.weight.dim():
                    nn.init.kaiming_uniform_(m.weight, a=math.sqrt(3))
                if hasattr(m, 'bias') and m.bias is not None:
                    if 1 != m.bias.dim():
                        fan_in, _ = nn.init._calculate_fan_in_and_fan_out(m.weight)
                        bound = 1 / math.sqrt(fan_in)
                        nn.init.uniform_(m.bias, -bound, bound)


class WideDeep(nn.Module):
    def __init__(self, args):
        super(WideDeep, self).__init__()
        self.keywords = args['keywords']
        self.keyword_embedding_size = args['keyword_embedding_size']
        self.char_embedding_size = args['char_embedding_size']
        self.node_auxiliary_size = args['node_auxiliary_size']
        self.first_hidden_size = args['first_hidden_size']
        self.second_hidden_size = args['second_hidden_size']
        self.drop_rate = args['drop_rate']
        self.other_size = args['other_size']
        self.key2index = args['key2index']
        self.index2key = args['index2key']
        self.usekeyword = args['usekeyword']
        self.usestring = args['usestring']
        self.usesequence = args['usesequence']
        self.replacedquery = args['replacedquery']

        if self.usestring:
            self.char_embedding = nn.Embedding(128, self.char_embedding_size, max_norm=1).to(device)
            # encoding string
            reset_parameters(self.char_embedding)
            self.cnn = nn.Sequential(
                nn.Conv2d(in_channels=1, out_channels=4, kernel_size=(3, 1), stride=1, padding=(1, 0)).to(device),
                nn.BatchNorm2d(4).to(device),
                # nn.Dropout2d(self.drop_rate),
                nn.ReLU().to(device),
                nn.Conv2d(in_channels=4, out_channels=1, kernel_size=(3, 1), stride=1, padding=(1, 0)).to(device),
                nn.BatchNorm2d(1, affine=False).to(device),
                # nn.Dropout2d(self.drop_rate),
                nn.ReLU().to(device),
                # nn.AvgPool2d(kernel_size=4)
            ).to(device)
            reset_parameters(self.cnn)

            self.filter_conv = nn.Conv2d(in_channels=1,out_channels=16,kernel_size=(3,1),stride=1, padding=(1, 0)).to(device)
            self.gate_conv = nn.Conv2d(in_channels=1,out_channels=16,kernel_size=(3,1),stride=1, padding=(1, 0)).to(device)
            self.mlp = nn.Conv2d(in_channels=16,out_channels=1,kernel_size=(1,1),stride=1)
        else:
            self.char_embedding_size = 128

        if self.usekeyword:
            self.keyword_embedding = nn.Embedding(len(self.keywords), self.keyword_embedding_size).to(device)
            self.keyword_embedding = self.keyword_embedding.to(device)
        else:
            self.keyword_embedding_size = len(self.keywords)  # one-hot
        reset_parameters(self.keyword_embedding)

        self.keyword_aline = nn.Linear(self.keyword_embedding_size,
                                       self.char_embedding_size).to(device)
        self.keyword_aline = self.keyword_aline.to(device)
        reset_parameters(self.keyword_aline)

        if self.usesequence:
            # encoding input schema
            self.lstm1 = nn.LSTM(input_size=self.char_embedding_size, num_layers=1, hidden_size=self.first_hidden_size,
                                 batch_first=True).to(device)
            self.lstm1 = self.lstm1.to(device)
            self.lstm2 = nn.LSTM(input_size=self.first_hidden_size + self.node_auxiliary_size, num_layers=1,
                                 hidden_size=self.second_hidden_size, batch_first=True).to(device)
            self.lstm2 = self.lstm2.to(device)
            reset_parameters(self.lstm1)
            reset_parameters(self.lstm2)
        else:
            self.second_hidden_size = self.char_embedding_size + self.node_auxiliary_size
        # resnet-block
        if self.replacedquery:
            self.resnet1 = nn.Sequential(
                nn.Linear(self.keyword_embedding_size * 2 + self.second_hidden_size * 2, 16).to(device),
                nn.Dropout(self.drop_rate),
                nn.ReLU().to(device),
                nn.Linear(16, self.keyword_embedding_size * 2 + self.second_hidden_size * 2).to(device),
                nn.Dropout(self.drop_rate),
                nn.ReLU().to(device)
            ).to(device)
        else:
            self.resnet1 = nn.Sequential(
                nn.Linear(self.keyword_embedding_size + self.second_hidden_size, 16).to(device),
                nn.Dropout(self.drop_rate),
                nn.ReLU().to(device),
                nn.Linear(16, self.keyword_embedding_size + self.second_hidden_size).to(device),
                nn.Dropout(self.drop_rate),
                nn.ReLU().to(device)
            ).to(device)
        self.resnet1 = self.resnet1.to(device)
        reset_parameters(self.resnet1)

        # self.resnet2 = nn.Sequential(
        #     nn.Linear(self.keyword_embedding_size * 2 + self.second_hidden_size * 2, 16),
        #     nn.Dropout(self.drop_rate),
        #     nn.ReLU(),
        #     nn.Linear(16, self.keyword_embedding_size * 2 + self.second_hidden_size * 2),
        #     nn.Dropout(self.drop_rate),
        #     nn.ReLU()
        # )
        if self.replacedquery:
            self.regressor = nn.Sequential(
                nn.Linear(self.keyword_embedding_size * 2 + self.second_hidden_size * 2, 32).to(
                    device),
                nn.ReLU(),
                nn.Linear(32, 16).to(device),
                nn.ReLU(),
                nn.Linear(16, 1)
            ).to(device)
        else:
            self.regressor = nn.Sequential(
                nn.Linear(self.keyword_embedding_size + self.second_hidden_size, 32).to(device),
                nn.ReLU(),
                nn.Linear(32, 16).to(device),
                nn.ReLU(),
                nn.Linear(16, 1)
            ).to(device)
        self.regressor = self.regressor.to(device)
        reset_parameters(self.regressor)

    def encoding_seq(self, input):
        # input : list[ tuple('words',type), tuple()...]
        # vector = [
        #     [self.keyword_aline(self.keyword_embedding(
        #       torch.LongTensor(self.key2index[att[0]]))).squeeze() if att[1] == 1 else
        #       torch.mean(self.cnn(self.char_embedding(torch.LongTensor(str2int(att[1])))).squeeze(), dim=0)
        #           for att in node] for node in input]
        vector = []
        for node in input:
            temp = []
            if isinstance(node, tuple):
                if self.usekeyword:
                    # 将keyword转成Nk*8的Tensor后，进行线性转换，变成Nk*4的矩阵，然后去掉所有维数为1的维度
                    temp.append(self.keyword_aline(
                        self.keyword_embedding(torch.LongTensor([self.key2index[node[0]]]).to(device)).to(
                            device)).squeeze())
                else:
                    position = self.key2index[node[0]]
                    temp.append(self.keyword_aline(torch.zeros(1, self.keyword_embedding_size).to(device).
                                                   scatter_(1, torch.LongTensor([[position]]).to(device), 1)).squeeze())
            else:
                if len(node) == 0:
                    node = [(None, 1)]
                for att in node:
                    if type(att) == str:
                        att = (att, 1)
                    if att[1] == 1:
                        # print(att[0],self.key2index[att[0]])
                        try:
                            position = self.key2index[att[0]]
                        except:
                            try:
                                position = self.key2index[att[0].upper()]
                            except:
                                try:
                                    print(self.key2index)
                                    position = self.key2index[att[0].lower()]
                                except:
                                    position = 0
                                
                        if self.usekeyword:
                            # 将keyword转成Nk*8的Tensor后，进行线性转换，变成Nk*4的矩阵，然后去掉所有维数为1的维度
                            v = self.keyword_aline(
                                self.keyword_embedding(torch.LongTensor([position]).to(device)).squeeze())
                        else:
                            v = self.keyword_aline(torch.zeros(1, self.keyword_embedding_size).to(device).
                                                   scatter_(1, torch.LongTensor([[position]]).to(device), 1)).squeeze()
                    else:
                        if self.usestring:
                            char_emb = self.char_embedding(torch.LongTensor(str2int(att[0])).to(device)).unsqueeze(0).unsqueeze(1)
                            filter = torch.tanh(self.filter_conv(char_emb))
                            gate = torch.sigmoid(self.gate_conv(char_emb))
                            v = filter * gate 
                            v = self.mlp(v)
                            v = torch.mean(v.reshape(len(att[0]), -1),dim = 0)
                            
                            # v = torch.mean(self.cnn(
                            #     self.char_embedding(torch.LongTensor(str2int(att[0])).to(device)).unsqueeze(0).unsqueeze(1))        #
                            #     .reshape(len(att[0]), -1), dim=0)


                            # oh_result = str2int(att[0])
                            # emb_out = []
                            # for arr in oh_result:
                            #     emb_out.append(self.char_embedding(torch.LongTensor(arr).to(device)))
                            # v = torch.mean(self.cnn(
                            #     torch.stack(emb_out).unsqueeze(1)).reshape(-1, self.char_embedding_size), dim=0)
                        else:
                            strs = str2int(att[0])
                            v = torch.mean(torch.zeros(len(strs), self.char_embedding_size).to(device).
                                           scatter_(1, torch.LongTensor([[s] for s in strs]).to(device), 1),
                                           dim=0)
                    # v:4
                    temp.append(v)
            # temp:len(node)*4
            vector.append(torch.stack(temp))

        # vector:len(input)*max(len(node))*4
        # 一个plan中node的个数*node中参数的个数*每个参数被编码为4
        if self.usesequence:
            max_length = np.max([len(v) for v in vector])

            index = [len(v) for v in vector]
            vector = torch.stack(
                [torch.cat((v, torch.FloatTensor([[0] * self.char_embedding_size] * (max_length - len(v))).to(device)))
                 for v in vector])
            out, _ = self.lstm1(vector, None)
            length_mask = torch.zeros(len(index), max_length).to(device).scatter_(1, torch.LongTensor(index).to(
                device).unsqueeze(1) - 1,
                                                                                  1).bool().reshape(len(index),
                                                                                                    max_length,
                                                                                                    1).repeat(1,
                                                                                                              1,
                                                                                                              self.first_hidden_size)
            real_out = torch.masked_select(out, length_mask).view(len(index), -1)
        else:
            real_out = torch.stack([torch.mean(v, dim=0) for v in vector])
        return real_out

    def encoding_input(self, input):
        if self.usekeyword:
            vector = self.keyword_embedding(torch.LongTensor([self.key2index[key] for key in input]).to(device))
        else:
            vector = torch.zeros(len(input), self.keyword_embedding_size).to(device).scatter_(1, torch.LongTensor(
                [[self.key2index[key]] for key in input]).to(device), 1)
        return torch.mean(vector, dim=0)

    def encoding_numerical_features(self, numerical):
        # tInput = torch.FloatTensor(numerical)
        mean = torch.zeros(self.other_size).to(device)
        div = torch.zeros(self.other_size).to(device)
        output = []
        for index in range(self.other_size):
            mean[index] = torch.gather(numerical, index).mean()
            div[index] = torch.gather(numerical, index).std()
        for index in range(self.other_size):
            for i in range(len(numerical[0])):
                output[index][i] = (numerical[index][i] - mean[index]) / div[index]

    '''
    json_seq: [{"seq":[[(keyword/str,1/2),()],...],"cost": float, "cardinality":float}, ]
    static_seq: [[[float,float],[],],]
    input_seq: [[str1,str2,..],]
    '''

    def forward(self, x):
        batch_size = len(x)

        if self.replacedquery:
            q_json_seq, q_static_seq, q_input_seq, mv_json_seq, mv_static_seq, mv_input_seq, other_seq = \
                [data[0] for data in x], [data[1] for data in x], [data[2] for data in x], [data[3] for data in x], \
                [data[4] for data in x], [data[5] for data in x], [data[6] for data in x]
        else:
            q_json_seq, q_static_seq, q_input_seq, other_seq = \
                [data[0] for data in x], [data[1] for data in x], [data[2] for data in x], [data[3] for data in x]

        # for q
        q_js_encoding = [
            torch.cat((self.encoding_seq(q_json_seq[i]), torch.FloatTensor(q_static_seq[i]).to(device)), dim=1) for i
            in range(batch_size)]
        if self.usesequence:
            # padding
            q_index = [len(v) for v in q_js_encoding]
            q_max_length = np.max(q_index)
            q_js_encoding = torch.stack(
                [torch.cat((v, torch.FloatTensor(
                    [[0] * (self.first_hidden_size + self.node_auxiliary_size)] * (q_max_length - len(v))).to(device)))
                 for v in
                 q_js_encoding])
            out, _ = self.lstm2(q_js_encoding, None)
            q_length_mask = torch.zeros(batch_size, q_max_length).to(device). \
                scatter_(1, torch.LongTensor(q_index).to(device).unsqueeze(1) - 1, 1).bool(). \
                reshape(batch_size, q_max_length, 1).repeat(1, 1, self.second_hidden_size)
            q_real_out = torch.masked_select(out, q_length_mask).view(batch_size, -1)
        else:
            q_real_out = torch.stack([torch.mean(v, dim=0) for v in q_js_encoding])

        q_input = torch.stack([self.encoding_input(v) for v in q_input_seq])

        if self.replacedquery:
            # for mv
            mv_js_encoding = [
                torch.cat((self.encoding_seq(mv_json_seq[i]), torch.FloatTensor(mv_static_seq[i]).to(device)), dim=1)
                for i in range(batch_size)]
            if self.usesequence:
                mv_index = [len(v) for v in mv_js_encoding]
                mv_max_length = np.max(mv_index)
                mv_js_encoding = torch.stack(
                    [torch.cat((v, torch.FloatTensor(
                        [[0] * (self.first_hidden_size + self.node_auxiliary_size)] * (mv_max_length - len(v))).to(
                        device)))
                     for v in mv_js_encoding])
                out, _ = self.lstm2(mv_js_encoding, None)
                mv_length_mask = torch.zeros(batch_size, mv_max_length).to(device). \
                    scatter_(1, torch.LongTensor(mv_index).to(device).unsqueeze(1) - 1, 1).bool(). \
                    reshape(batch_size, mv_max_length, 1).repeat(1, 1, self.second_hidden_size)
                mv_real_out = torch.masked_select(out, mv_length_mask).view(batch_size, -1)
            else:
                # print(mv_js_encoding)
                mv_real_out = torch.stack([torch.mean(v, dim=0) for v in mv_js_encoding])

            mv_input = torch.stack([self.encoding_input(v) for v in mv_input_seq])

            features_tensor = torch.cat([q_input, q_real_out, mv_input, mv_real_out], dim=1)
            # print(q_input.size(),q_real_out.size(),mv_input.size(),mv_real_out.size())
        else:
            features_tensor = torch.cat([q_input, q_real_out], dim=1)

        features_tensor = torch.add(features_tensor, self.resnet1(features_tensor))
        # features_tensor = torch.add(features_tensor,self.resnet2(features_tensor))

        # if other_seq is not None:
        #     # other_seq就是文章中的numerical features,对其做归一化处理
        #     # other_seq = self.encoding_numerical_features(torch.FloatTensor(other_seq))
        #     # features_tensor = torch.cat([features_tensor, torch.FloatTensor(other_seq).to(device)], dim=1)
        #     features_tensor = torch.cat([features_tensor, torch.FloatTensor(other_seq).to(device)], dim=1)
        
        out = self.regressor(features_tensor).squeeze()

        return out


class WideDeepDistributed(WideDeep):
    def __init__(self, args):
        super(WideDeepDistributed, self).__init__(args)
        self.index2string = args['index2string']

    def encoding_seq_distributed(self, input: Tensor, node_num, json_len: Tensor):
        vector = []
        for node_id in range(node_num):

            temp = []
            if int(json_len[node_id].item()) == 0:
                position = self.key2index[None]
                if self.usekeyword:
                    v = self.keyword_aline(
                        self.keyword_embedding(torch.LongTensor([self.key2index[None]]).to(device)).squeeze())
                else:
                    v = self.keyword_aline(torch.zeros(1, self.keyword_embedding_size).to(device).
                                           scatter_(1, torch.LongTensor([[position]]).to(device), 1)).squeeze()
                temp.append(v)
            for keyword_id in range(int(json_len[node_id].item())):
                # print("======================================")
                # print(keyword_id)
                if input[node_id][keyword_id][1].item() == 1.0:
                    position = input[node_id][keyword_id][0].item()
                    if self.usekeyword:
                        v = self.keyword_aline(
                            self.keyword_embedding(torch.LongTensor([input[node_id][keyword_id][0].item()])).squeeze()
                        )
                    else:
                        v = self.keyword_aline(torch.zeros(1, self.keyword_embedding_size).to(device)
                                               .scatter_(1, torch.LongTensor([[position]]).to(device), 1)).squeeze()
                else:
                    if self.usestring:
                        v = torch.mean(self.cnn(
                            self.char_embedding(torch.LongTensor(str2int(
                                self.index2string[input[node_id][keyword_id][0].item()])).to(device)).unsqueeze(
                                0).unsqueeze(
                                1)).reshape(len(self.index2string[input[node_id][keyword_id][0].item()]), -1), dim=0)
                    else:
                        strs = str2int(bytes.fromhex(hex(input[node_id][keyword_id][0].item())).decode())
                        v = torch.mean(torch.zeros(len(strs), self.char_embedding_size).to(device).
                                       scatter_(1, torch.LongTensor([[s] for s in strs]).to(device), 1),
                                       dim=0)
                # print(v)
                temp.append(v)
            vector.append(torch.stack(temp))

        if self.usesequence:
            max_length = np.max([len(v) for v in vector])

            index = [len(v) for v in vector]
            vector = torch.stack(
                [torch.cat((v, torch.FloatTensor([[0] * self.char_embedding_size] * (max_length - len(v))).to(device)))
                 for v in vector])
            out, _ = self.lstm1(vector, None)
            length_mask = torch.zeros(len(index), max_length).to(device).scatter_(1, torch.LongTensor(index).to(
                device).unsqueeze(1) - 1,
                                                                                  1).bool().reshape(len(index),
                                                                                                    max_length,
                                                                                                    1).repeat(1,
                                                                                                              1,
                                                                                                              self.first_hidden_size)
            real_out = torch.masked_select(out, length_mask).view(len(index), -1)
        else:
            real_out = torch.stack([torch.mean(v, dim=0) for v in vector])
        return real_out

    def encoding_input_distributed(self, input: Tensor, seq_len: Tensor):
        if self.usekeyword:
            vector = self.keyword_embedding(
                torch.LongTensor([input[i].item() for i in range(int(seq_len[2].item()))])).to(device)
        else:
            vector = torch.zeros(len(input), self.keyword_embedding_size).to(device).scatter_(1, torch.LongTensor(
                [[input[i].item()] for i in range(int(seq_len[2].item()))]).to(device), 1)
        return torch.mean(vector, dim=0)

    def forward(self, q_json_seq: Tensor, q_static_seq, q_input_seq,
                mv_json_seq, mv_static_seq, mv_input_seq, other_seq, seq_len, q_json_len, mv_json_len):
        # print(q_json_seq)
        batch_size = q_json_len.shape[0]
        # print("==============================")
        # print(seq_len)
        # print(seq_len.shape)
        q_json_seq = torch.squeeze(q_json_seq, dim=1)
        q_static_seq = torch.squeeze(q_static_seq, dim=1)
        q_input_seq = torch.squeeze(q_input_seq, dim=1)
        mv_json_seq = torch.squeeze(mv_json_seq, dim=1)
        mv_static_seq = torch.squeeze(mv_static_seq, dim=1)
        mv_input_seq = torch.squeeze(mv_input_seq, dim=1)
        other_seq = torch.squeeze(other_seq, dim=1)
        seq_len = torch.squeeze(seq_len, dim=1)
        q_json_len = torch.squeeze(q_json_len, dim=1)
        mv_json_len = torch.squeeze(mv_json_len, dim=1)
        # print("==============================")
        # print(seq_len)
        # print(seq_len.shape)
        # for q
        q_js_encoding = [
            torch.cat((self.encoding_seq_distributed(q_json_seq[i], int(seq_len[i][0].item()), q_json_len[i]),
                       torch.FloatTensor([[q_static_seq[i][x][0].item(), q_static_seq[i][x][1].item()]
                                          for x in range(int(seq_len[i][1].item()))])), dim=1)
            for i in range(batch_size)]
        if self.usesequence:
            q_index = [len(v) for v in q_js_encoding]
            q_max_length = np.max(q_index)
            q_js_encoding = torch.stack(
                [torch.cat((v, torch.FloatTensor(
                    [[0] * (self.first_hidden_size + self.node_auxiliary_size)] * (q_max_length - len(v))).to(device)))
                 for v in
                 q_js_encoding])
            out, _ = self.lstm2(q_js_encoding, None)
            q_length_mask = torch.zeros(batch_size, q_max_length).to(device). \
                scatter_(1, torch.LongTensor(q_index).to(device).unsqueeze(1) - 1, 1).bool(). \
                reshape(batch_size, q_max_length, 1).repeat(1, 1, self.second_hidden_size)
            q_real_out = torch.masked_select(out, q_length_mask).view(batch_size, -1)
        else:
            q_real_out = torch.stack([torch.mean(v, dim=0) for v in q_js_encoding])

        q_input = torch.stack([self.encoding_input_distributed(q_input_seq[i], seq_len[i]) for i in range(batch_size)])

        if self.replacedquery:
            mv_js_encoding = [
                torch.cat((self.encoding_seq_distributed(mv_json_seq[i], int(seq_len[i][3].item()), mv_json_len[i]),
                           torch.FloatTensor([[mv_static_seq[i][x][0].item(), mv_static_seq[i][x][1].item()]
                                              for x in range(int(seq_len[i][4].item()))])), dim=1)
                for i in range(batch_size)]
            if self.usesequence:
                mv_index = [len(v) for v in mv_js_encoding]
                mv_max_length = np.max(mv_index)
                mv_js_encoding = torch.stack(
                    [torch.cat((v, torch.FloatTensor(
                        [[0] * (self.first_hidden_size + self.node_auxiliary_size)] * (mv_max_length - len(v))).to(
                        device)))
                     for v in
                     mv_js_encoding])
                out, _ = self.lstm2(mv_js_encoding, None)
                mv_length_mask = torch.zeros(batch_size, mv_max_length).to(device). \
                    scatter_(1, torch.LongTensor(mv_index).to(device).unsqueeze(1) - 1, 1).bool(). \
                    reshape(batch_size, mv_max_length, 1).repeat(1, 1, self.second_hidden_size)
                mv_real_out = torch.masked_select(out, mv_length_mask).view(batch_size, -1)
            else:
                mv_real_out = torch.stack([torch.mean(v, dim=0) for v in mv_js_encoding])

            mv_input = torch.stack(
                [self.encoding_input_distributed(mv_input_seq[i], seq_len[i]) for i in range(batch_size)])

            features_tensor = torch.cat([q_input, q_real_out, mv_input, mv_real_out], dim=1)
        else:
            features_tensor = torch.cat([q_input, q_real_out], dim=1)

        features_tensor = torch.add(features_tensor, self.resnet1(features_tensor))

        features_tensor = torch.cat([features_tensor, torch.FloatTensor([[other_seq[i][j]
                                                                          for j in range(int(seq_len[i][6].item()))]
                                                                         for i in range(batch_size)])], dim=1)
        out = torch.squeeze(self.regressor(features_tensor), dim=1)
        # print("*******************************")
        # print(out)
        return out

    def predict(self, x):
        batch_size = len(x)

        if self.replacedquery:
            q_json_seq, q_static_seq, q_input_seq, mv_json_seq, mv_static_seq, mv_input_seq, other_seq = \
                [data[0] for data in x], [data[1] for data in x], [data[2] for data in x], [data[3] for data in x], \
                [data[4] for data in x], [data[5] for data in x], [data[6] for data in x]
        else:
            q_json_seq, q_static_seq, q_input_seq, other_seq = \
                [data[0] for data in x], [data[1] for data in x], [data[2] for data in x], [data[3] for data in x]

        # for q
        q_js_encoding = [
            torch.cat((self.encoding_seq(q_json_seq[i]), torch.FloatTensor(q_static_seq[i]).to(device)), dim=1) for i
            in range(batch_size)]
        if self.usesequence:
            q_index = [len(v) for v in q_js_encoding]
            q_max_length = np.max(q_index)
            q_js_encoding = torch.stack(
                [torch.cat((v, torch.FloatTensor(
                    [[0] * (self.first_hidden_size + self.node_auxiliary_size)] * (q_max_length - len(v))).to(device)))
                 for v in
                 q_js_encoding])
            out, _ = self.lstm2(q_js_encoding, None)
            q_length_mask = torch.zeros(batch_size, q_max_length).to(device). \
                scatter_(1, torch.LongTensor(q_index).to(device).unsqueeze(1) - 1, 1).bool(). \
                reshape(batch_size, q_max_length, 1).repeat(1, 1, self.second_hidden_size)
            q_real_out = torch.masked_select(out, q_length_mask).view(batch_size, -1)
        else:
            q_real_out = torch.stack([torch.mean(v, dim=0) for v in q_js_encoding])

        q_input = torch.stack([self.encoding_input(v) for v in q_input_seq])

        if self.replacedquery:
            # for mv
            mv_js_encoding = [
                torch.cat((self.encoding_seq(mv_json_seq[i]), torch.FloatTensor(mv_static_seq[i]).to(device)), dim=1)
                for i in range(batch_size)]
            if self.usesequence:
                mv_index = [len(v) for v in mv_js_encoding]
                mv_max_length = np.max(mv_index)
                mv_js_encoding = torch.stack(
                    [torch.cat((v, torch.FloatTensor(
                        [[0] * (self.first_hidden_size + self.node_auxiliary_size)] * (mv_max_length - len(v))).to(
                        device)))
                     for v in mv_js_encoding])
                out, _ = self.lstm2(mv_js_encoding, None)
                mv_length_mask = torch.zeros(batch_size, mv_max_length).to(device). \
                    scatter_(1, torch.LongTensor(mv_index).to(device).unsqueeze(1) - 1, 1).bool(). \
                    reshape(batch_size, mv_max_length, 1).repeat(1, 1, self.second_hidden_size)
                mv_real_out = torch.masked_select(out, mv_length_mask).view(batch_size, -1)
            else:
                # print(mv_js_encoding)
                mv_real_out = torch.stack([torch.mean(v, dim=0) for v in mv_js_encoding])

            mv_input = torch.stack([self.encoding_input(v) for v in mv_input_seq])

            features_tensor = torch.cat([q_input, q_real_out, mv_input, mv_real_out], dim=1)
            # print(q_input.size(),q_real_out.size(),mv_input.size(),mv_real_out.size())
        else:
            features_tensor = torch.cat([q_input, q_real_out], dim=1)

        features_tensor = torch.add(features_tensor, self.resnet1(features_tensor))
        # features_tensor = torch.add(features_tensor,self.resnet2(features_tensor))

        if other_seq is not None:
            # other_seq就是文章中的numerical features,对其做归一化处理
            # self.encoding_numerical_features(torch.FloatTensor(other_seq))
            # features_tensor = torch.cat([features_tensor, torch.FloatTensor(other_seq).to(device)], dim=1)
            features_tensor = torch.cat([features_tensor, torch.FloatTensor(other_seq).to(device)], dim=1)
            out = self.regressor(features_tensor).squeeze()

        return out

def train_distributed(data, keywords, usekeyword=True, usestring=True, usesequence=True, replacedquery=True,
                      result_file=None):
    if len(data) == 0:
        print("No data available")
        return
    train_rate = getTrainRate()
    batch_size = getBatchSize()
    if replacedquery:
        epoch = getEpoch()
    else:
        epoch = getEpoch()
    lr = getLearningRate()
    # lam = 0.01
    weight_decay = getWeightDecay()
    args = {}
    args['keywords'] = keywords
    args['keyword_embedding_size'] = getKeywordEmbeddingSize()
    args['char_embedding_size'] = getCharEmbeddingSize()
    args['node_auxiliary_size'] = getNodeAuxiliarySize()
    args['first_hidden_size'] = getFirstHiddenSize()
    args['second_hidden_size'] = getSecondHiddenSize()
    args['drop_rate'] = getDropRate()
    args['other_size'] = len(data[0][0][-1])
    args['key2index'] = {word: index for index, word in enumerate(keywords)}
    args['index2key'] = {value: key for key, value in args['key2index'].items()}
    args['usekeyword'] = usekeyword
    args['usestring'] = usestring
    args['usesequence'] = usesequence
    args['replacedquery'] = replacedquery
    args['num_proc'] = getNumProc()
    args['max_num_keyword_per_node'] = getMaxNumKeywordPerNode()
    args['max_num_node'] = getMaxNumNode()
    args['index2string'] = {}
    args['string2index'] = {}

    # early_stopping = EarlyStopping(10, verbose=True)

    factor = 1
    if replacedquery is False:
        factor = 3 * factor
    datatmp = []
    for i in range(factor):
        random.shuffle(data)
        datatmp += data
    data = datatmp
    random.shuffle(data)

    spark = get_distributed_spark_session(master=getSparkMaster(), num_proc=args['num_proc'])

    dict_list = []
    for line in data:
        q_json_seq_idxes = []
        q_json_seq_values = []
        for node_row_id in range(len(line[0][0])):
            for key_id in range(len(line[0][0][node_row_id])):
                q_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2)
                q_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2 + 1)
                if line[0][0][node_row_id][key_id][1] == 1:
                    q_json_seq_values.append(args['key2index'][line[0][0][node_row_id][key_id][0]])
                else:
                    if line[0][0][node_row_id][key_id][0] not in args['string2index']:
                        args['string2index'][line[0][0][node_row_id][key_id][0]] = len(args['string2index'])
                    q_json_seq_values.append(args['string2index'][line[0][0][node_row_id][key_id][0]])
                q_json_seq_values.append(line[0][0][node_row_id][key_id][1])
        q_static_seq_values = []
        for node in line[0][1]:
            q_static_seq_values.append(node[0])
            q_static_seq_values.append(node[1])

        mv_json_seq_idxes = []
        mv_json_seq_values = []
        for node_row_id in range(len(line[0][3])):
            for key_id in range(len(line[0][3][node_row_id])):
                mv_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2)
                mv_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2 + 1)
                if line[0][3][node_row_id][key_id][1] == 1:
                    mv_json_seq_values.append(args['key2index'][line[0][3][node_row_id][key_id][0]])
                else:
                    if line[0][3][node_row_id][key_id][0] not in args['string2index']:
                        args['string2index'][line[0][3][node_row_id][key_id][0]] = len(args['string2index'])
                    mv_json_seq_values.append(args['string2index'][line[0][3][node_row_id][key_id][0]])
                mv_json_seq_values.append(line[0][3][node_row_id][key_id][1])
        mv_static_seq_values = []
        for node in line[0][4]:
            mv_static_seq_values.append(node[0])
            mv_static_seq_values.append(node[1])

        seq_len_values = []
        for len_id in range(7):
            seq_len_values.append(len(line[0][len_id]))
        q_json_len_values = []
        for q_json_len_id in range(len(line[0][0])):
            q_json_len_values.append(len(line[0][0][q_json_len_id]))
        mv_json_len_values = []
        for mv_json_len_id in range(len(line[0][3])):
            mv_json_len_values.append(len(line[0][3][mv_json_len_id]))

        line_label = float(np.log2(line[1] + 1))

        dict_line = {
            'l': line[2],
            'q_json_seq': SparseVector(args['max_num_keyword_per_node'] * args['max_num_node'] * 2,
                                       q_json_seq_idxes, q_json_seq_values),
            'q_static_seq': SparseVector(args['max_num_node'] * 2, range(len(line[0][1]) * 2),
                                         q_static_seq_values),
            'q_input_seq': SparseVector(args['max_num_node'], range(len(line[0][2])),
                                        [args['key2index'][input_key] for input_key in line[0][2]]),
            'mv_json_seq': SparseVector(args['max_num_keyword_per_node'] * args['max_num_node'] * 2,
                                        mv_json_seq_idxes, mv_json_seq_values),
            'mv_static_seq': SparseVector(args['max_num_node'] * 2, range(len(line[0][4]) * 2),
                                          mv_static_seq_values),
            'mv_input_seq': SparseVector(args['max_num_node'], range(len(line[0][5])),
                                         [args['key2index'][input_key] for input_key in line[0][5]]),
            'other_seq': SparseVector(args['max_num_node'], range(len(line[0][6])),
                                      [x * 1.0 for x in line[0][6]]),
            'seq_len': SparseVector(7, range(7), seq_len_values),
            'q_json_len': SparseVector(args['max_num_node'], range(seq_len_values[0]),
                                       q_json_len_values),
            'mv_json_len': SparseVector(args['max_num_node'], range(seq_len_values[3]),
                                        mv_json_len_values),
            'label': line_label
        }
        # dict_line = {'features': DenseVector([1.0, 2.0, 3.0, 4.0]),
        #              'features2': DenseVector([1.0, 2.0, 3.0, 4.0]),
        #              'cost': DenseVector([1.0, 2.0, 3.0, 4.0])}
        dict_list.append(dict_line)
    data_df = spark.createDataFrame(dict_list)
    # data_train, data_test = data_df.randomSplit([train_rate, 1-train_rate])
    # print("========================================")
    # data_train.count()
    # data_test.count()
    data_df.show(10)

    # train_len = int(int(train_rate * len(data) + batch_size - 1) / int(batch_size) * batch_size)
    # print("{}".format(train_len))
    # train_data = data[:train_len]
    # test_data = data[train_len:]
    #
    # test_X = [data[0] for data in test_data]
    # test_Y = torch.FloatTensor(np.log([data[1] + 1 for data in test_data])).to(device)
    # # test_Y = torch.FloatTensor([data[1] + 1 for data in test_data])
    # # test_L = [data[2] for data in test_data]
    # print("train_data:{},test_data:{},test_X:{},test_Y:{}".format(len(train_data), len(test_data), len(test_X),
    #                                                               len(test_Y)))

    args['index2string'] = {value: key for key, value in args['string2index'].items()}

    # TODO: debug for pack source code
    widedeep = WideDeepDistributed(args).to(device)
    # widedeep = WideDeepDistributed(args).to(device)
    mape = mape_loss()
    mae = mae_loss()

    if getModelSavePath() is not None and os.path.exists(getModelSavePath()):
        print(getModelSavePath())
        widedeep.load_state_dict(torch.load(getModelSavePath()))


    spark_backend = SparkBackend(
        num_proc=args['num_proc'],
        stdout=sys.stdout, stderr=sys.stderr,
        prefix_output_with_timestamp=True
    )

    store = FilesystemStore.create(getHvdStorePath(), checkpoint_filename="ckpt")

    torch_estimator = hvd.TorchEstimator(
        backend=spark_backend,
        store=store,
        model=widedeep,
        optimizer=torch.optim.Adam(widedeep.parameters(), lr=lr, weight_decay=weight_decay),
        loss=nn.MSELoss(),
        input_shapes=[[-1, 1, args['max_num_node'], args['max_num_keyword_per_node'], 2],
                      [-1, 1, args['max_num_node'], 2],
                      [-1, 1, args['max_num_node']],
                      [-1, 1, args['max_num_node'], args['max_num_keyword_per_node'], 2],
                      [-1, 1, args['max_num_node'], 2],
                      [-1, 1, args['max_num_node']],
                      [-1, 1, args['max_num_node']],
                      [-1, 1, 7],
                      [-1, 1, args['max_num_node']],
                      [-1, 1, args['max_num_node']]],
        feature_cols=['q_json_seq', 'q_static_seq', 'q_input_seq',
                      'mv_json_seq', 'mv_static_seq', 'mv_input_seq', 'other_seq',
                      'seq_len', 'q_json_len', 'mv_json_len'],
        label_cols=['label'],
        batch_size=batch_size,
        epochs=epoch,
        validation=1 - train_rate,
        verbose=1,
        run_id=getRunId()
    )
    torch_model = torch_estimator.fit(data_df).setOutputCols(['cost_pred'])
    # pred_df = torch_model.transform(data_test)
    # print("------------------*********************---------------------")
    # pred_df.show(10)

    widedeep = torch_model.getModel()

    torch.save(widedeep.state_dict(), getModelSavePath())

    train_len = int(int(train_rate * len(data) + batch_size - 1) / int(batch_size) * batch_size)
    print("{}".format(train_len))
    train_data = data[:train_len]
    test_data = data[train_len:]
    test_X = [data[0] for data in test_data]
    test_Y = torch.FloatTensor(np.log([data[1] + 1 for data in test_data])).to(device)
    widedeep = WideDeepDistributed(args).to(device)
    widedeep.load_state_dict(torch.load(getModelSavePath()))

    # return
    # for key,param in widedeep.named_parameters():
    #     print(key,param)
    if result_file is not None:
        results = []
        with open(result_file, 'w', encoding='utf-8') as w:
            # if replacedquery:
            rst_data = test_data
            # else:
            # rst_data = data
            for d in rst_data:
                # for d in data:
                x, y, l = d
                y_ = widedeep.predict([x]).squeeze(-1)
                # mae_ = mae(y_, torch.log(torch.FloatTensor([y]).to(device)))
                # mape_ = mape(y_, torch.log(torch.FloatTensor([y]).to(device)))
                # results.append((l, np.exp(y_.squeeze().detach().cpu().numpy()), y,
                #                 mae_.squeeze().detach().cpu().numpy().flatten()[0],
                #                 mape_.squeeze().detach().cpu().numpy().flatten()[0],
                #                 x))
                mae_ = mae(y_, torch.FloatTensor([y]))
                mape_ = mape(y_, torch.FloatTensor([y]))
                results.append((l, y_.squeeze().detach().numpy(), y,
                                mae_.squeeze().detach().numpy().flatten()[0],
                                mape_.squeeze().detach().numpy().flatten()[0],
                                x))
            results = sorted(results, key=lambda x: x[3], reverse=True)
            for r in results:
                w.write("{}\n".format(r))



def train(data, keywords, replacedquery=True):
    if len(data) == 0:
        print("No data available")
        return
    train_rate = getTrainRate()
    batch_size = getBatchSize()
    epoch = getEpoch()
    lr = getLearningRate()
    # lam = 0.01
    weight_decay = getWeightDecay()
    args = {}
    args['keywords'] = keywords
    print("=====================================")
    print(len(keywords))
    print("======================================")
    args['keyword_embedding_size'] = getKeywordEmbeddingSize()
    args['char_embedding_size'] = getCharEmbeddingSize()
    args['node_auxiliary_size'] = getNodeAuxiliarySize()
    args['first_hidden_size'] = getFirstHiddenSize()
    args['second_hidden_size'] = getSecondHiddenSize()
    args['drop_rate'] = getDropRate()
    args['other_size'] = len(data[0][0][-1])
    args['key2index'] = {word: index for index, word in enumerate(keywords)}
    args['index2key'] = {value: key for key, value in args['key2index'].items()}
    args['usekeyword'] = True
    args['usestring'] = True
    args['usesequence'] = True
    args['replacedquery'] = replacedquery

    # early_stopping = EarlyStopping(10, verbose=True)
    # factor = 10
    factor = 1
    if replacedquery is False:
        factor = 3 * factor
    datatmp = []
    for i in range(factor):
        random.shuffle(data)
        datatmp += data
    data = datatmp
    random.shuffle(data)

    train_len = int(train_rate * len(data))
    print("{}".format(train_len))
    train_data = data[:train_len]
    test_data = data[train_len:]
    
    scale = 1

    test_X = [data[0] for data in test_data]
    test_Y = torch.FloatTensor([data[1] / scale for data in test_data]).to(device)
    # test_Y = torch.FloatTensor([data[1] + 1 for data in test_data])
    # test_L = [data[2] for data in test_data]
    print("train_data:{},test_data:{},test_X:{},test_Y:{}".format(len(train_data), len(test_data), len(test_X),
                                                                  len(test_Y)))
    # exit()
    # else:
    #     q_mvs = [q_mv for _, _, q_mv in q_mvdata]
    #     print(q_mvs)
    #     exit
    #     for x, y, l in data:
    #         pass
    widedeep = WideDeep(args).to(device)

    if getModelSavePath(replacedquery) is not None and os.path.exists(getModelSavePath(replacedquery)):
        print(getModelSavePath(replacedquery))
        widedeep = torch.load(getModelSavePath(replacedquery))

    step_per_epoch = int(np.ceil(len(train_data) / batch_size))
    loss_func = nn.MSELoss()
    optimizer = torch.optim.Adam(widedeep.parameters(), lr=lr, weight_decay=weight_decay)
    min_loss = None
    for i in range(epoch):
        print('Epoch: {}, lr:{}'.format(i, lr))
        random.shuffle(train_data)
        widedeep.train()
        for step in range(step_per_epoch):
            train_per_step = train_data[step * batch_size:(step + 1) * batch_size]
            # data[0]中包括q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others
            # q[1]是操作耗费的时间和涉及的行数
            # q[2]是表名、列名
            # others包括 [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
            X = [data[0] for data in train_per_step]
            # data[1]中是q_mv的cost
            # 取对数之后，会造成模型误差小，但求幂之后误差较大
            Y = torch.FloatTensor([data[1] / scale for data in train_per_step]).to(device)
            Y_ = widedeep(X)
            loss = loss_func(Y_, Y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            print('Epoch: {}, step: {}, train_y: {}, target_y: {}' \
                  .format(i, step, torch.mean(Y_).item(), torch.mean(Y).item()))
        # eval
        widedeep.eval()
        test_Y_ = widedeep(test_X)
        test_loss = loss_func(test_Y_, test_Y)
        # if pre_test_loss is not None and (test_loss - pre_test_loss) / pre_test_loss > 0.1:
        #     break
        # elif pre_test_loss is None or test_loss < pre_test_loss:
        #     pre_test_loss = test_loss

        # print('Epoch: ', i, 'Step: ', step,
        #       '| test loss: %.4f' % test_loss, '| train loss: %.4f' % loss)
        print('Epoch: {}, test loss: {}, train loss: {}, test_y: {}, pre_test_y: {}' \
              .format(i, test_loss, loss, test_Y_[0], test_Y[0]))
        if min_loss is None or test_loss < min_loss:
            min_loss = test_loss
        if min_loss < 6 and (i + 1) % 3 == 0 and lr > 0.006:
            lr /= 1.25

        # early_stopping(test_loss, widedeep)
        # if early_stopping.early_stop:
        #     print("EarlyStopping,Epoch:{}".format(i))
        #     break
        # widedeep.train()
        # if (i + 1) % 2 == 0:
        #     if step < 1:
        #         if (i + 1) % 10 == 0:
        #             if lr > 0.0001:
        #                 lr /= 1.6
        #             else:
        #                 lr = 0.001
        #             print('step: {}, test loss: {}, pre_test_loss: {}'.format(step, test_loss, pre_test_loss))
        #     else:
        #         if lr <= 0.0001:
        #             lr /= 0.8
        #         elif lr > 0.001:
        #             lr /= 1.25
        #     lr /= 1.25

        # for key, param in widedeep.named_parameters():
        #     print(key, param)
    if getModelSavePath(replacedquery) is not None:
        # torch.save(widedeep.state_dict(), model_pre)
        torch.save(widedeep, getModelSavePath(replacedquery))
    # find the result of invalid
    widedeep = torch.load(getModelSavePath(replacedquery))
    widedeep.eval()

    ALL_X = [d[0] for d in data]
    ALL_Y = torch.FloatTensor([d[1] / scale for d in data]).to(device)
    ALL_Y_ = widedeep(ALL_X)
    ALL_Y = ALL_Y.detach().numpy()
    ALL_Y_ = ALL_Y_.detach().numpy()
    MAPE = mape(ALL_Y, ALL_Y_)
    print("Finally, MAPE: {}%".format(MAPE))


def transfer(data, keywords, inc_data, inc_keywords, usekeyword=True, usestring=True, usesequence=True,
             replacedquery=True, model_pre=None,
             result_file=None):
    if len(data) == 0 or len(inc_data) == 0:
        print("No data available")
        return
    train_rate = 0.5
    batch_size = 2
    if replacedquery:
        epoch = 500
    else:
        epoch = 500
    lr = 0.01
    # lam = 0.01
    weight_decay = 1e-5
    args = {}
    args['keywords'] = keywords
    args['keyword_embedding_size'] = 8
    args['char_embedding_size'] = 4
    args['node_auxiliary_size'] = 2
    args['first_hidden_size'] = 8
    args['second_hidden_size'] = 8
    args['drop_rate'] = 0.2
    args['other_size'] = len(data[0][0][-1])
    args['key2index'] = {word: index for index, word in enumerate(keywords)}
    args['index2key'] = {value: key for key, value in args['key2index'].items()}
    args['usekeyword'] = usekeyword
    args['usestring'] = usestring
    args['usesequence'] = usesequence
    args['replacedquery'] = replacedquery

    # early_stopping = EarlyStopping(10, verbose=True)

    if model_pre is not None:
        model_pre += "{}_{}_{}_{}_{}-{}-{}-{}-{}-{}-{}.pth".format(usekeyword, usestring, usesequence, lr, weight_decay,
                                                                   args['keyword_embedding_size'],
                                                                   args['char_embedding_size'],
                                                                   args['node_auxiliary_size'],
                                                                   args['first_hidden_size'],
                                                                   args['second_hidden_size'], args['drop_rate'])
    # factor = 10
    factor = 1
    if replacedquery is False:
        factor = 3 * factor
    datatmp = []
    for i in range(factor):
        random.shuffle(data)
        datatmp += data
    data = datatmp
    random.shuffle(data)
    train_len = int(int(train_rate * len(data) + batch_size - 1) / int(batch_size) * batch_size)
    print("{}".format(train_len))
    train_data = data[:int(train_rate * len(data))]
    test_data = data[int(train_rate * len(data)):]
    test_X = [data[0] for data in test_data]
    # test_Y = torch.FloatTensor(np.log([data[1] + 1 for data in test_data])).to(device)
    test_Y = torch.FloatTensor([data[1] + 1 for data in test_data])
    # test_L = [data[2] for data in test_data]
    print("train_data:{},test_data:{},test_X:{},test_Y:{}".format(len(train_data), len(test_data), len(test_X),
                                                                  len(test_Y)))
    # exit()
    # else:
    #     q_mvs = [q_mv for _, _, q_mv in q_mvdata]
    #     print(q_mvs)
    #     exit
    #     for x, y, l in data:
    #         pass
    widedeep = WideDeep(args).to(device)
    mape = mape_loss()
    mae = mae_loss()

    if model_pre is not None and os.path.exists(model_pre):
        print(model_pre)
        widedeep.load_state_dict(torch.load(model_pre))
    else:
        step_per_epoch = int(np.ceil(len(train_data) / batch_size))
        loss_func = nn.MSELoss()
        # optimizer = torch.optim.Adam(widedeep.parameters(), lr=lr, weight_decay=weight_decay)
        pre_test_loss = None
        for i in range(epoch):
            print('Epoch: {}, lr:{}'.format(i, lr))
            optimizer = torch.optim.Adam(widedeep.parameters(), lr=lr, weight_decay=weight_decay)
            random.shuffle(train_data)
            widedeep.train()
            for step in range(step_per_epoch):
                train_per_step = train_data[step * batch_size:(step + 1) * batch_size]
                # data[0]中包括q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others
                # q[1]是操作耗费的时间和涉及的行数
                # q[2]是表名、列名
                # others包括 [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
                X = [data[0] for data in train_per_step]
                # data[1]中是q_mv的cost
                # 取对数之后，会造成模型误差小，但求幂之后误差较大
                # Y = torch.FloatTensor(np.log([data[1] for data in train_per_step])).to(device)
                Y = torch.FloatTensor([data[1] for data in train_per_step])
                Y_ = widedeep(X)
                loss = loss_func(Y_, Y)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                print('Epoch: {}, step: {}, train loss: {}, train_y: {}, target_y: {}' \
                      .format(i, step, loss, Y_[0], Y[0]))
            # eval
            widedeep.eval()
            test_Y_ = widedeep(test_X)
            test_loss = loss_func(test_Y_, test_Y)
            # if pre_test_loss is not None and (test_loss - pre_test_loss) / pre_test_loss > 0.1:
            #     break
            # elif pre_test_loss is None or test_loss < pre_test_loss:
            #     pre_test_loss = test_loss

            # print('Epoch: ', i, 'Step: ', step,
            #       '| test loss: %.4f' % test_loss, '| train loss: %.4f' % loss)
            print('Epoch: {}, test loss: {}, train loss: {}, test_y: {}, pre_test_y: {}' \
                  .format(i, test_loss, loss, test_Y_[0], test_Y[0]))
            # early_stopping(test_loss, widedeep)
            # if early_stopping.early_stop:
            #     print("EarlyStopping,Epoch:{}".format(i))
            #     break
        if model_pre is not None:
            # torch.save(widedeep.state_dict(), model_pre)
            torch.save(torch.load('checkpoint.pt'), model_pre)
    # find the result of invalid
    # widedeep.eval()
    widedeep.load_state_dict(torch.load(model_pre))
    # for key,param in widedeep.named_parameters():
    #     print(key,param)
    if result_file is not None:
        results = []
        with open(result_file, 'w', encoding='utf-8') as w:
            # if replacedquery:
            rst_data = test_data
            # else:
            # rst_data = data
            for d in rst_data:
                # for d in data:
                x, y, l = d
                y_ = widedeep([x]).squeeze(-1)
                # mae_ = mae(y_, torch.log(torch.FloatTensor([y]).to(device)))
                # mape_ = mape(y_, torch.log(torch.FloatTensor([y]).to(device)))
                # results.append((l, np.exp(y_.squeeze().detach().cpu().numpy()), y,
                #                 mae_.squeeze().detach().cpu().numpy().flatten()[0],
                #                 mape_.squeeze().detach().cpu().numpy().flatten()[0],
                #                 x))
                mae_ = mae(y_, torch.FloatTensor([y]))
                mape_ = mape(y_, torch.FloatTensor([y]))
                results.append((l, y_.squeeze().detach().numpy(), y,
                                mae_.squeeze().detach().numpy().flatten()[0],
                                mape_.squeeze().detach().numpy().flatten()[0],
                                x))
            results = sorted(results, key=lambda x: x[3], reverse=True)
            for r in results:
                w.write("{}\n".format(r))
    # return test_data


def test_distributed(data, keywords, usekeyword=True, usestring=True, usesequence=True, replacedquery=True,
                     result_file=None):
    if len(data) == 0:
        print("No data available")
        return
    args = {}
    args['keywords'] = keywords
    args['keyword_embedding_size'] = 8
    args['char_embedding_size'] = 4
    args['node_auxiliary_size'] = 2
    args['first_hidden_size'] = 8
    args['second_hidden_size'] = 8
    args['drop_rate'] = 0.2
    args['other_size'] = len(data[0][0][-1])
    args['key2index'] = {word: index for index, word in enumerate(keywords)}
    args['index2key'] = {value: key for key, value in args['key2index'].items()}
    args['usekeyword'] = usekeyword
    args['usestring'] = usestring
    args['usesequence'] = usesequence
    args['replacedquery'] = replacedquery
    args['num_proc'] = 1
    args['max_num_keyword_per_node'] = 1000
    args['max_num_node'] = 100
    args['index2string'] = {}
    args['string2index'] = {}

    widedeep = WideDeepDistributed(args).to(device)
    widedeep.load_state_dict(torch.load(getModelSavePath()))

    ALL_X = [d[0] for d in data]
    ALL_Y = torch.FloatTensor(np.log([d[1] + 1 for d in data])).to(device)
    ALL_Y_ = widedeep.predict(ALL_X)

    # print(ALL_Y)

    mse = mse_loss()
    all_loss = np.sqrt(mse(ALL_Y_, ALL_Y).detach().numpy())
    print("finally, all loss:{}".format(all_loss))

    if result_file is not None:
        results = []
        rst_data = data
        for d in rst_data:
            x, y, l = d
            y_ = widedeep.predict([x]).squeeze(-1)
            results.append((l[0] + "-" + l[1], y_.squeeze().detach().numpy()))
        appendCSVFile(results, DataType.Q)
        appendCSVFile(results, DataType.MV)
        appendCSVFile(results, DataType.Q_MV)


# q:querydata.csv mv:mvdata.csv q_mv:query-mvdata.csv
def test(data, data_type, replacedquery=True):
    if len(data) == 0:
        print("No data available")
        return

    scale = 1
    if data_type == DataType.Q_MV:
        widedeep = torch.load(getModelSavePath(replacedquery))

        ALL_X = [d[0] for d in data]
        ALL_Y = torch.FloatTensor([d[1] / scale for d in data]).to(device)
        ALL_Y_ = widedeep(ALL_X)

        # print(ALL_Y)

        mse = mse_loss()
        all_loss = np.sqrt(mse(ALL_Y_, ALL_Y).detach().numpy())
        print("finally, all loss:{}".format(all_loss))

        results_q_mv = []
        for d in data:
            x, y, l = d
            y_ = widedeep([x]).squeeze(-1)
            y_ *= scale
            results_q_mv.append((l[0] + "-" + l[1], y_.squeeze().detach().numpy()))
            # results_q_mv.append((l[0] + "-" + l[1], y))
        if results_q_mv:
            appendCSVFile(results_q_mv, DataType.Q_MV)
    elif data_type == DataType.Q:
        results_q = []
        for d in data:
            x, y, l = d
            results_q.append((l, y))
        if results_q:
            appendCSVFile(results_q, DataType.Q)
    else:
        results_mv = []

        widedeep = torch.load(getModelSavePath(replacedquery))

        for d in data:
            x, y, l = d
            y_ = widedeep([x]).squeeze(-1)
            results_mv.append((l, y_.squeeze().detach().numpy()))
            
        if results_mv:
            appendCSVFile(results_mv, DataType.MV)


def update(data, keywords, usekeyword=True, usestring=True, usesequence=True, replacedquery=True, model_pre=None,
           result_file=None):
    if len(data) == 0:
        print("No data available")
        return
    train_rate = 0.8
    batch_size = 8
    lr = 0.01
    epoch = 500
    # lam = 0.01
    weight_decay = 1e-5
    args = {}
    args['keywords'] = keywords
    args['keyword_embedding_size'] = 8
    args['char_embedding_size'] = 4
    args['node_auxiliary_size'] = 2
    args['first_hidden_size'] = 8
    args['second_hidden_size'] = 8
    args['drop_rate'] = 0.2
    args['other_size'] = len(data[0][0][-1])
    args['key2index'] = {word: index for index, word in enumerate(keywords)}
    args['index2key'] = {value: key for key, value in args['key2index'].items()}
    args['usekeyword'] = usekeyword
    args['usestring'] = usestring
    args['usesequence'] = usesequence
    args['replacedquery'] = replacedquery

    # early_stopping = EarlyStopping(10, verbose=True)

    if model_pre is not None:
        file_appendix = "{}_{}_{}_{}_{}-{}-{}-{}-{}-{}-{}.pth".format(usekeyword, usestring, usesequence, lr,
                                                                      weight_decay,
                                                                      args['keyword_embedding_size'],
                                                                      args['char_embedding_size'],
                                                                      args['node_auxiliary_size'],
                                                                      args['first_hidden_size'],
                                                                      args['second_hidden_size'], args['drop_rate'])
        model_pre += file_appendix

    widedeep = WideDeep(args).to(device)
    if model_pre is None or not os.path.exists(model_pre):
        print("No model named:{} available".format(model_pre))
        return
    else:
        min_loss = None
        # print(model_pre)
        train_len = int(np.ceil(int(train_rate * len(data)) / batch_size)) * batch_size
        # train_data = data[:int(train_rate * len(data))]
        # test_data = data[int(train_rate * len(data)):]
        train_data = data[:train_len]
        test_data = data[train_len:]
        test_X = [data[0] for data in test_data]
        test_Y = torch.FloatTensor(np.log([data[1] + 1 for data in test_data])).to(device)
        # test_Y = torch.FloatTensor([data[1] + 1 for data in test_data])
        # test_L = [data[2] for data in test_data]
        print("train_data:{},test_data:{},test_X:{},test_Y:{}".format(len(train_data), len(test_data), len(test_X),
                                                                      len(test_Y)))

        model_dict = widedeep.state_dict()
        pretrained_dict = torch.load(model_pre)
        pretrained_dict = {k: v for k, v in pretrained_dict.items() if
                           (k in model_dict and 'keyword_embedding' not in k)}
        model_dict.update(pretrained_dict)
        widedeep.load_state_dict(model_dict)

        fixed = True
        # 只对keyword_embedding层进行重新训练
        for k, v in widedeep.named_parameters():
            v.requires_grad = fixed  # 固定参数
            if k == 'keyword_embedding.weight' or k == 'keyword_embedding.bias':
                fixed = False

        # 将要训练的参数放入优化器
        # optimizer = torch.optim.Adam(params=[widedeep.keyword_embedding.weight],
        #                              lr=lr, betas=(0.9, 0.999), weight_decay=weight_decay)
        mape = mape_loss()
        mae = mae_loss()
        step_per_epoch = int(np.ceil(len(train_data) / batch_size))
        loss_func = nn.MSELoss()
        pre_test_loss = None
        for i in range(epoch):
            print('Epoch: {}, lr:{}'.format(i, lr))
            # 将要训练的参数放入优化器
            optimizer = torch.optim.Adam(params=[widedeep.keyword_embedding.weight],
                                         lr=lr, betas=(0.9, 0.999), weight_decay=weight_decay)
            # optimizer = torch.optim.Adam(widedeep.parameters(), lr=lr, betas=(0.9, 0.999), weight_decay=weight_decay)
            random.shuffle(train_data)
            widedeep.train()
            for step in range(step_per_epoch):
                train_per_step = train_data[step * batch_size:(step + 1) * batch_size]
                # data[0]中包括q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others
                # q[1]是操作耗费的时间和涉及的行数
                # q[2]是表名、列名
                # others包括 [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
                X = [data[0] for data in train_per_step]
                # data[1]中是q_mv的cost
                # 取对数之后，会造成模型误差小，但求幂之后误差较大
                Y = torch.FloatTensor(np.log([data[1] for data in train_per_step])).to(device)
                # Y = torch.FloatTensor([data[1] for data in train_per_step])
                Y_ = widedeep(X)
                loss = loss_func(Y_, Y)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()
                print('Epoch: {}, step: {}, train loss: {}, train_y: {}, target_y: {}' \
                      .format(i, step, loss, Y_[0], Y[0]))
            # eval
            widedeep.eval()
            test_Y_ = widedeep(test_X)
            test_loss = loss_func(test_Y_, test_Y)
            print('Epoch: {}, test loss: {}, train loss: {}, test_y: {}, pre_test_y: {}' \
                  .format(i, test_loss, loss, test_Y_[0], test_Y[0]))
            if min_loss is None or test_loss < min_loss:
                min_loss = test_loss
            if min_loss < 6 and (i + 1) % 3 == 0 and lr > 0.006:
                lr /= 1.25
            # early_stopping(test_loss, widedeep)
            # if early_stopping.early_stop:
            #     print("EarlyStopping,Epoch:{}".format(i))
            #     break
        torch.save(torch.load('checkpoint.pt'), "../incSql/resource/result/inc_" + file_appendix)
    # find the result of invalid
    widedeep.load_state_dict(torch.load('checkpoint.pt'))
    widedeep.eval()
    mse = mse_loss()
    test_Y_ = widedeep(test_X)
    test_loss = np.sqrt(mse(test_Y_, test_Y).detach().numpy())

    ALL_X = [d[0] for d in data]
    ALL_Y = torch.FloatTensor(np.log([d[1] + 1 for d in data])).to(device)
    ALL_Y_ = widedeep(ALL_X)
    all_loss = np.sqrt(mse(ALL_Y_, ALL_Y).detach().numpy())
    print("finally, test loss:{}, all loss:{}".format(test_loss, all_loss))
    return
    # # for key,param in widedeep.named_parameters():
    # #     print(key,param)
    # if result_file is not None:
    #     results = []
    #     with open(result_file, 'w', encoding='utf-8') as w:
    #         # if replacedquery:
    #         rst_data = test_data
    #         # else:
    #         # rst_data = data
    #         for d in rst_data:
    #             # for d in data:
    #             x, y, l = d
    #             y_ = widedeep([x]).squeeze(-1)
    #             mae_ = mae(y_, torch.log(torch.FloatTensor([y]).to(device)))
    #             mape_ = mape(y_, torch.log(torch.FloatTensor([y]).to(device)))
    #             results.append((l, np.exp(y_.squeeze().detach().cpu().numpy()), y,
    #                             mae_.squeeze().detach().cpu().numpy().flatten()[0],
    #                             mape_.squeeze().detach().cpu().numpy().flatten()[0],
    #                             x))
    #             # mae_ = mae(y_, torch.FloatTensor([y]))
    #             # mape_ = mape(y_, torch.FloatTensor([y]))
    #             # results.append((l, y_.squeeze().detach().numpy(), y,
    #             #                 mae_.squeeze().detach().numpy().flatten()[0],
    #             #                 mape_.squeeze().detach().numpy().flatten()[0],
    #             #                 x))
    #         results = sorted(results, key=lambda x: x[3], reverse=True)
    #         for r in results:
    #             w.write("{}\n".format(r))
    # # return test_data


def update_distributed(data, keywords, usekeyword=True, usestring=True, usesequence=True, replacedquery=True,
           result_file=None):
    if len(data) == 0:
        print("No data available")
        return
    train_rate = getTrainRate()
    batch_size = getBatchSize()
    if replacedquery:
        epoch = getEpoch()
    else:
        epoch = getEpoch()
    lr = getLearningRate()
    # lam = 0.01
    weight_decay = getWeightDecay()
    args = {}
    args['keywords'] = keywords
    args['keyword_embedding_size'] = getKeywordEmbeddingSize()
    args['char_embedding_size'] = getCharEmbeddingSize()
    args['node_auxiliary_size'] = getNodeAuxiliarySize()
    args['first_hidden_size'] = getFirstHiddenSize()
    args['second_hidden_size'] = getSecondHiddenSize()
    args['drop_rate'] = getDropRate()
    args['other_size'] = len(data[0][0][-1])
    args['key2index'] = {word: index for index, word in enumerate(keywords)}
    args['index2key'] = {value: key for key, value in args['key2index'].items()}
    args['usekeyword'] = usekeyword
    args['usestring'] = usestring
    args['usesequence'] = usesequence
    args['replacedquery'] = replacedquery
    args['num_proc'] = getNumProc()
    args['max_num_keyword_per_node'] = getMaxNumKeywordPerNode()
    args['max_num_node'] = getMaxNumNode()
    args['index2string'] = {}
    args['string2index'] = {}

    # early_stopping = EarlyStopping(10, verbose=True)

    factor = 1
    if replacedquery is False:
        factor = 3 * factor
    datatmp = []
    for i in range(factor):
        random.shuffle(data)
        datatmp += data
    data = datatmp
    random.shuffle(data)

    spark = get_distributed_spark_session(master=getSparkMaster(), num_proc=args['num_proc'])

    dict_list = []
    for line in data:
        q_json_seq_idxes = []
        q_json_seq_values = []
        for node_row_id in range(len(line[0][0])):
            for key_id in range(len(line[0][0][node_row_id])):
                q_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2)
                q_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2 + 1)
                if line[0][0][node_row_id][key_id][1] == 1:
                    q_json_seq_values.append(args['key2index'][line[0][0][node_row_id][key_id][0]])
                else:
                    if line[0][0][node_row_id][key_id][0] not in args['string2index']:
                        args['string2index'][line[0][0][node_row_id][key_id][0]] = len(args['string2index'])
                    q_json_seq_values.append(args['string2index'][line[0][0][node_row_id][key_id][0]])
                q_json_seq_values.append(line[0][0][node_row_id][key_id][1])
        q_static_seq_values = []
        for node in line[0][1]:
            q_static_seq_values.append(node[0])
            q_static_seq_values.append(node[1])

        mv_json_seq_idxes = []
        mv_json_seq_values = []
        for node_row_id in range(len(line[0][3])):
            for key_id in range(len(line[0][3][node_row_id])):
                mv_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2)
                mv_json_seq_idxes.append(node_row_id * args['max_num_keyword_per_node'] * 2 + key_id * 2 + 1)
                if line[0][3][node_row_id][key_id][1] == 1:
                    mv_json_seq_values.append(args['key2index'][line[0][3][node_row_id][key_id][0]])
                else:
                    if line[0][3][node_row_id][key_id][0] not in args['string2index']:
                        args['string2index'][line[0][3][node_row_id][key_id][0]] = len(args['string2index'])
                    mv_json_seq_values.append(args['string2index'][line[0][3][node_row_id][key_id][0]])
                mv_json_seq_values.append(line[0][3][node_row_id][key_id][1])
        mv_static_seq_values = []
        for node in line[0][4]:
            mv_static_seq_values.append(node[0])
            mv_static_seq_values.append(node[1])

        seq_len_values = []
        for len_id in range(7):
            seq_len_values.append(len(line[0][len_id]))
        q_json_len_values = []
        for q_json_len_id in range(len(line[0][0])):
            q_json_len_values.append(len(line[0][0][q_json_len_id]))
        mv_json_len_values = []
        for mv_json_len_id in range(len(line[0][3])):
            mv_json_len_values.append(len(line[0][3][mv_json_len_id]))

        line_label = float(np.log2(line[1] + 1))

        dict_line = {
            'l': line[2],
            'q_json_seq': SparseVector(args['max_num_keyword_per_node'] * args['max_num_node'] * 2,
                                       q_json_seq_idxes, q_json_seq_values),
            'q_static_seq': SparseVector(args['max_num_node'] * 2, range(len(line[0][1]) * 2),
                                         q_static_seq_values),
            'q_input_seq': SparseVector(args['max_num_node'], range(len(line[0][2])),
                                        [args['key2index'][input_key] for input_key in line[0][2]]),
            'mv_json_seq': SparseVector(args['max_num_keyword_per_node'] * args['max_num_node'] * 2,
                                        mv_json_seq_idxes, mv_json_seq_values),
            'mv_static_seq': SparseVector(args['max_num_node'] * 2, range(len(line[0][4]) * 2),
                                          mv_static_seq_values),
            'mv_input_seq': SparseVector(args['max_num_node'], range(len(line[0][5])),
                                         [args['key2index'][input_key] for input_key in line[0][5]]),
            'other_seq': SparseVector(args['max_num_node'], range(len(line[0][6])),
                                      [x * 1.0 for x in line[0][6]]),
            'seq_len': SparseVector(7, range(7), seq_len_values),
            'q_json_len': SparseVector(args['max_num_node'], range(seq_len_values[0]),
                                       q_json_len_values),
            'mv_json_len': SparseVector(args['max_num_node'], range(seq_len_values[3]),
                                        mv_json_len_values),
            'label': line_label
        }
        # dict_line = {'features': DenseVector([1.0, 2.0, 3.0, 4.0]),
        #              'features2': DenseVector([1.0, 2.0, 3.0, 4.0]),
        #              'cost': DenseVector([1.0, 2.0, 3.0, 4.0])}
        dict_list.append(dict_line)
    data_df = spark.createDataFrame(dict_list)
    # data_train, data_test = data_df.randomSplit([train_rate, 1-train_rate])
    # print("========================================")
    # data_train.count()
    # data_test.count()
    data_df.show(10)
    args['index2string'] = {value: key for key, value in args['string2index'].items()}

    widedeep_distributed = WideDeepDistributed(args).to(device)
    if getModelSavePath() is None or not os.path.exists(getModelSavePath()):
        print("No model named:{} available".format(getModelSavePath()))
        return
    else:
        train_len = int(np.ceil(int(train_rate * len(data)) / batch_size)) * batch_size
        # train_data = data[:int(train_rate * len(data))]
        # test_data = data[int(train_rate * len(data)):]
        train_data = data[:train_len]
        test_data = data[train_len:]
        test_X = [data[0] for data in test_data]
        test_Y = torch.FloatTensor(np.log([data[1] + 1 for data in test_data])).to(device)
        # test_Y = torch.FloatTensor([data[1] + 1 for data in test_data])
        # test_L = [data[2] for data in test_data]
        print("train_data:{},test_data:{},test_X:{},test_Y:{}".format(len(train_data), len(test_data), len(test_X),
                                                                      len(test_Y)))

        model_dict = widedeep_distributed.state_dict()
        pretrained_dict = torch.load(getModelSavePath())
        pretrained_dict = {k: v for k, v in pretrained_dict.items() if
                           (k in model_dict and 'keyword_embedding' not in k)}
        model_dict.update(pretrained_dict)
        widedeep_distributed.load_state_dict(model_dict)

        fixed = True
        # 只对keyword_embedding层进行重新训练
        for k, v in widedeep_distributed.named_parameters():
            v.requires_grad = fixed  # 固定参数
            if k == 'keyword_embedding.weight' or k == 'keyword_embedding.bias':
                fixed = False

        mape = mape_loss()
        mae = mae_loss()
        loss_func = nn.MSELoss()

        spark_backend = SparkBackend(
            num_proc=args['num_proc'],
            stdout=sys.stdout, stderr=sys.stderr,
            prefix_output_with_timestamp=True
        )

        store = FilesystemStore.create(getHvdStorePath(), checkpoint_filename="ckpt")

        torch_estimator = hvd.TorchEstimator(
            backend=spark_backend,
            store=store,
            model=widedeep_distributed,
            optimizer=torch.optim.Adam(widedeep_distributed.parameters(), lr=lr, weight_decay=weight_decay),
            loss=nn.MSELoss(),
            input_shapes=[[-1, 1, args['max_num_node'], args['max_num_keyword_per_node'], 2],
                          [-1, 1, args['max_num_node'], 2],
                          [-1, 1, args['max_num_node']],
                          [-1, 1, args['max_num_node'], args['max_num_keyword_per_node'], 2],
                          [-1, 1, args['max_num_node'], 2],
                          [-1, 1, args['max_num_node']],
                          [-1, 1, args['max_num_node']],
                          [-1, 1, 7],
                          [-1, 1, args['max_num_node']],
                          [-1, 1, args['max_num_node']]],
            feature_cols=['q_json_seq', 'q_static_seq', 'q_input_seq',
                          'mv_json_seq', 'mv_static_seq', 'mv_input_seq', 'other_seq',
                          'seq_len', 'q_json_len', 'mv_json_len'],
            label_cols=['label'],
            batch_size=batch_size,
            epochs=epoch,
            validation=1 - train_rate,
            verbose=1,
            run_id=getRunId()
        )
        torch_model = torch_estimator.fit(data_df).setOutputCols(['cost_pred'])
        # pred_df = torch_model.transform(data_test)
        # print("------------------*********************---------------------")
        # pred_df.show(10)

        widedeep_distributed = torch_model.getModel()

        torch.save(widedeep_distributed.state_dict(), getModelSavePath())