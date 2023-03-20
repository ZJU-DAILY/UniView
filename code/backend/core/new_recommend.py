import csv
from procConf import *


def recommend(topk=False, k=getMvCntLimit(), threshold=100):
    memory_overhead = {}
    mv_benefit = []

    memory_data = list(csv.reader(open('./resources/data/mvdata.csv', 'r')))

    for data in memory_data:
        try:
            mv_id = data[0].split('.')[1][2:]
        except:
            mv_id = data[0][2:]
        memory = float(data[1]) / 1024 / 1024
        memory_overhead[mv_id] = memory
    
    benefit_data = list(csv.reader(open('./resources/data/clusters_data.csv', 'r')))

    for data in benefit_data:
        mv_id = data[0]
        benefit = float(data[1])
        mv_benefit.append((mv_id, benefit))

    mv_benefit.sort(key=lambda k:k[1], reverse=True)
    
    recommend_results = []

    if topk:
        for i in range(k):
            if i >= len(mv_benefit):
                break
            recommend_results.append(mv_benefit[i][0])
    else:
        cur_overhead = 0
        i = 0
        while True:
            cur_overhead += memory_overhead[mv_benefit[i][0]]
            if cur_overhead > threshold:
                break
            recommend_results.append(mv_benefit[i][0])
            i += 1
    
    output_path = './resources/data/recommend-mvdata.csv'
    with open(output_path, 'w', newline='') as write_file:
        for mv in recommend_results:
            write_file.write(mv)
            write_file.write('\n')

# recommend()
