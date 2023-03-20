from functools import reduce
from collections import Counter, defaultdict
from typing import DefaultDict, Dict, List, Tuple
from itertools import combinations
import getData
import time

class Node(object):
    def __init__(self, name, fa) -> None:
        self.name = name
        self.fa = fa
        self.children = []
        self.children_name = []
        self.__count = 1

    def add_count(self):
        self.__count += 1

    @property
    def count(self):
        return self.__count


# return true and path if tree has only one path
def get_single_path(root: Node) -> Tuple[bool, List[Node]]:
    cur_node = root
    path = []
    while True:
        # 处理路径
        if cur_node.name != 'root':
            path.append(cur_node.name)
        # 判断是否单路径,即从root开始往下都只有一个孩子
        if len(cur_node.children) == 0:
            return True, path
        elif len(cur_node.children) == 1:
            cur_node = cur_node.children[0]
        else:
            return False, []


def all_combination(array) -> list:
    for i, _ in enumerate(array):
        for elements in combinations(array, i + 1):
            yield list(elements)


# 打印item_table
def printItemTable(item_table: Dict):
    for k, v in item_table.items():
        print(k)
        for i in v['node']:
            print(i.fa.name + ', ')
        print()
        # for count, node in v.values():
        #   print(count)
        #  print(node)


# 返回FP-tree的根节点 和 item表（记录每个item的数目和每个item出现的位置）
def build_FP_Tree(itemsets: List[List[str]], n_sup: int) -> Tuple[Node, Dict]:
    counter = Counter(reduce(lambda x, y: x + y, itemsets))
    item_table = sorted(counter, key=lambda x: counter[x])
    item_table = {item: {'count': counter[item], 'node': []} for item in item_table if counter[item] >= n_sup}
    # 此时的item_table是个Dict，元素也是Dict，元素按Dict中的count从小到大排序

    root = Node('root', None)
    for itemset in itemsets:
        cur_fa_node = root
        # itemset: 返回一个List，其中item按其数量从大到小排列
        itemset = sorted((item for item in itemset if counter[item] >= n_sup), key=lambda x: counter[x], reverse=True)

        for item in itemset:
            if item in cur_fa_node.children_name:
                # 根据item找到其下标
                i = cur_fa_node.children_name.index(item)
                # 找到子节点，并修改cur_fa_node为子节点
                cur_fa_node: Node = cur_fa_node.children[i]
                # 个数增加
                cur_fa_node.add_count()
            else:
                new_node = Node(item, cur_fa_node)
                cur_fa_node.children_name.append(item)
                cur_fa_node.children.append(new_node)
                cur_fa_node = new_node

            # 将元素加入item_table
            # 根节点和第一层结点不用加入，因为这些结点没有CPB（条件模式基）
            if cur_fa_node.name != 'root' and cur_fa_node.fa.name != 'root':
                item_table[item]['node'].append(cur_fa_node)

    return root, item_table


# CPB ： conditional pattern base（条件模式基）
def find_CPB(item_table: Dict, item_name: str) -> List[List[str]]:
    cpb = []
    log = {}
    for node in item_table[item_name]['node']:
        if node not in log:
            # 寻找到其父的路径
            suffix = []
            cur_node: Node = node.fa
            while cur_node.name != 'root':
                suffix.append(cur_node.name)
                cur_node = cur_node.fa
            if suffix:
                log[node] = suffix
            else:
                continue
        cpb.append(log[node][:])

    return cpb


# 打印路径
def printPath(path: List[Node]):
    str = ''
    for i in range(len(path)):
        if i != 0:
            str += '->'
        str += path[i]
    print(str)


# 获得MFI_tree的头表元素排序方式
def getSortHead(itemsets: List[List[str]], n_sup: int):
    counter = Counter(reduce(lambda x, y: x + y, itemsets))
    item_tables = sorted(counter, key=lambda x: counter[x], reverse=True)
    head_count = [item for item in item_tables if counter[item] >= n_sup]
    head_sort: Dict = {}
    for i in range(len(head_count)):
        # head_sort.update(head_count[i], i)
        head_sort[head_count[i]] = i

    return head_count, head_sort
    # 测试
    # arr = ['g', 'a', 'd', 'e']
    # arr = sorted(arr, key=lambda x: head_sort[x])
    # print(arr)


def MFI_table_init():
    global MFI_table, MFI_item
    MFI_table = {item: [] for item in MFI_item}


ans_path = []
tmp_path = []


# 深度优先搜索，找到根到尾路径上的所有元素
def MFI_print(root: Node):
    if len(root.children) == 0:
        tmp_path.append(root.name)
        cur_path = []
        for item in tmp_path:
            cur_path.append(item)
        ans_path.append(cur_path)
        tmp_path.remove(root.name)
        return
    if root.name != 'root':
        tmp_path.append(root.name)
    # print(tmp_path)
    for node in root.children:
        MFI_print(node)
    if root.name != 'root':
        tmp_path.remove(root.name)


# 全局变量
head: List = []  # 论文中全局的第一个参数
MFI_root: Node = Node('root', None)  # MFI_tree的头部
MFI_table: Dict = {}  # MFI_tree的头表
MFI_item: List = {}  # 头表包含的元素
MFI_hashMap: Dict = {}  # 头表元素的排序哈希表

transactions = {
    'T100': ['a', 'b', 'c', 'e', 'f', 'o'],
    'T200': ['a', 'c', 'g'],
    'T300': ['e', 'i'],
    'T400': ['a', 'c', 'd', 'e', 'g'],
    'T500': ['a', 'c', 'e', 'g', 'l'],
    'T600': ['e', 'j'],
    'T700': ['a', 'b', 'c', 'e', 'f', 'p'],
    'T800': ['a', 'c', 'd'],
    'T900': ['a', 'c', 'e', 'g', 'm'],
    'T1000': ['a', 'c', 'e', 'g', 'n']
}


def build_MFI_tree(path: List[str]):
    global MFI_root, MFI_table
    cur_fa_node = MFI_root
    for item in path:
        if item in cur_fa_node.children_name:
            i = cur_fa_node.children_name.index(item)
            cur_fa_node: Node = cur_fa_node.children[i]
        else:
            new_node = Node(item, cur_fa_node)
            cur_fa_node.children_name.append(item)
            cur_fa_node.children.append(new_node)
            cur_fa_node = new_node

        # 加入头表
        if cur_fa_node.name != 'root':
            MFI_table[item].append(cur_fa_node)


def FP_grouth(root: Node, item_table: Dict, n_sup: int):
    ret, path = get_single_path(root)
    # print(path)
    global head, MFI_hashMap
    if ret:  # 只有一条路径
        path = path[::-1]  # 反转
        if len(head) > 0:
            path.append(head[0])
        path = sorted(path, key=lambda x: MFI_hashMap[x])
        # printPath(path)
        build_MFI_tree(path)

    else:
        for item_name in item_table:
            head.append(item_name)
            # 生成子数据集，它就是条件模式基
            sub_itemsets = find_CPB(item_table, item_name)
            if sub_itemsets:
                # sub_track
                counter = Counter(reduce(lambda x, y: x + y, sub_itemsets))
                item_tables = [item for item in counter if counter[item] >= n_sup]
                item_tables.append(item_name)
                # item_tables按照头表顺序排序
                item_tables = sorted(item_tables, key=lambda x: MFI_hashMap[x])
                # print(item_tables)
                size = len(item_tables)
                itemStr = item_tables[size - 1]
                flag = True
                for item in MFI_table[itemStr]:  # 找到存在最后一个元素的头表中结点
                    k = size - 2  # 倒数第二元素比较
                    while k >= 0 and item.name != "root":
                        if item.fa.name == item_tables[k]:
                            k -= 1
                            item = item.fa
                        else:
                            item = item.fa
                    if k < 0:
                        flag = False
                        break
                # 检查出子集就不再执行了
                if flag:
                    sub_root, sub_item_table = build_FP_Tree(sub_itemsets, n_sup)
                    if sub_root.children:
                        FP_grouth(sub_root, sub_item_table, n_sup)
            head.remove(item_name)


def FPGrowth(transaction: dict, min_sup: float) -> Dict[str, List[List[str]]]:
    # n_sup = round(min_sup * len(transaction))
    n_sup = min_sup
    root, itemtable = build_FP_Tree(list(transaction.values()), n_sup)

    result = defaultdict(set)
    FP_grouth(root, itemtable, n_sup)

    for item, itemsets in result.items():
        result[item] = [list(itemset) for itemset in itemsets]

    return dict(result)


'''
transactions = {
    'T100' : ['I1', 'I2', 'I5'],
    'T200' : ['I2', 'I4'],
    'T300' : ['I2', 'I3'],
    'T400' : ['I1', 'I2', 'I4'],
    'T500' : ['I1', 'I3'],
    'T600' : ['I2', 'I3'],
    'T700' : ['I1', 'I3'],
    'T800' : ['I1', 'I2', 'I3', 'I5'],
    'T900' : ['I1', 'I2', 'I3']
}
'''

'''
transactions = {'T10000': ['person_role_id', 'role_id', 'note', 'movie_id'],
                'T10001': ['person_role_id', 'role_id', 'note', 'movie_id'],
                'T10002': ['person_role_id', 'role_id', 'note', 'movie_id'],
                'T10003': ['person_id', 'movie_id'],
                'T10004': ['person_id', 'movie_id'],
                'T10005': ['person_id', 'movie_id'],
                'T10006': ['person_id', 'movie_id'],
                'T10007': ['person_id', 'movie_id'],
                'T10008': ['person_id', 'movie_id'],
                'T10009': ['person_id', 'movie_id'],
                'T10010': ['person_id', 'movie_id'],
                'T10011': ['person_id', 'movie_id'],
                'T10012': ['person_id', 'movie_id'],
                'T10013': ['person_id', 'note', 'movie_id'],
                'T10014': ['person_id', 'note', 'movie_id'],
                'T10015': ['person_id', 'note', 'movie_id'],
                'T10016': ['role_id', 'note', 'movie_id',
                           'person_id', 'person_role_id'],
                'T10017': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'],
                'T10018': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'],
                'T10019': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'],
                'T10020': ['person_id', 'person_role_id', 'movie_id'],
                'T10021': ['person_id', 'person_role_id', 'movie_id'],
                'T10022': ['person_id', 'person_role_id', 'movie_id'],
                'T10023': ['role_id', 'note', 'movie_id', 'person_id',
                           'person_role_id'],
                'T10024': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'],
                'T10025': ['person_id', 'note', 'movie_id'],
                'T10026': ['person_id', 'note', 'movie_id'],
                'T10027': ['person_id', 'note', 'movie_id'],
                'T10028': ['person_id', 'person_role_id', 'movie_id'],
                'T10029': ['person_id', 'person_role_id', 'movie_id'], 'T10030': ['person_id', 'person_role_id', 'movie_id'], 'T10031': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'], 'T10032': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'], 'T10033': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'], 'T10034': ['person_id', 'note', 'movie_id'], 'T10035': ['person_id', 'note', 'movie_id'], 'T10036': ['person_id', 'note', 'movie_id'], 'T10037': ['person_id', 'note', 'movie_id'], 'T10038': ['person_id', 'note', 'movie_id'], 'T10039': ['person_id', 'note', 'movie_id'], 'T10040': ['person_id', 'movie_id'], 'T10041': ['person_id', 'movie_id'], 'T10042': ['person_id', 'movie_id'], 'T10043': ['person_id', 'movie_id'], 'T10044': ['person_id', 'movie_id'], 'T10045': ['person_id', 'movie_id'], 'T10046': ['person_id', 'movie_id'], 'T10047': ['person_id', 'movie_id'], 'T10048': ['person_id', 'movie_id'], 'T10049': ['person_id', 'role_id', 'note', 'movie_id'], 'T10050': ['person_id', 'role_id', 'note', 'movie_id'], 'T10051': ['person_id', 'role_id', 'movie_id'], 'T10052': ['person_id', 'role_id', 'movie_id'], 'T10053': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'], 'T10054': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'], 'T10055': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id'], 'T10056': ['role_id', 'note', 'movie_id', 'person_id', 'person_role_id']
                }
'''


# 清理所有全局变量
def allClear():
    global head, MFI_root, MFI_table, MFI_item, MFI_hashMap, ans_path, tmp_path
    head.clear()
    MFI_root = Node('root', None)
    MFI_table.clear()
    MFI_item.clear()
    MFI_hashMap.clear()
    ans_path.clear()
    tmp_path.clear()


# def getFMax(prePath, schemaDict):
#     global MFI_root, MFI_item, MFI_hashMap, ans_path
#     attrDict = {}
#     itemAll, condAll = getData.get_trans(prePath + "/sql/", schemaDict)
#     for item_name, item in itemAll.items():
#         allClear()
#         if 0:
#             # 测试，只要出现就放入select，不计算频繁项
#             newItem = set()
#             for col in item:
#                 newItem = newItem.union(set(col))
#             attrDict[item_name] = newItem
#             # print("=========={}=======".format(item_name))
#             # print(item)
#             continue
#         transactions = {k:v for k, v in item.items()}
#         MFI_item, MFI_hashMap = getSortHead(list(transactions.values()), 2)
#         # print(MFI_item)
#         MFI_table_init()
#         result = FPGrowth(transactions, 2)
#         MFI_print(MFI_root)
#         # for item in ans_path:
#         #     print(item)
#         # print(ans_path[-1])
#         # attrs = str(ans_path[-1])[1:-2].replace("'", "")
#         attrDict[item_name] = ans_path[-1]
#     return attrDict, condAll


def getFMax(prePath, schemaDict):
    global MFI_root, MFI_item, MFI_hashMap, ans_path
    attrDict = {}
    itemAll, condAll = getData.get_trans(prePath + "/sql/", schemaDict)
    for item_name, item in itemAll.items():
        allClear()
        if 0:
            # 测试，只要出现就放入select，不计算频繁项
            newItem = set()
            for col in item:
                newItem = newItem.union(set(col))
            attrDict[item_name] = newItem
            # print("=========={}=======".format(item_name))
            # print(item)
            continue
        transactions = {k:v for k, v in item.items()}
        MFI_item, MFI_hashMap = getSortHead(list(transactions.values()), 2)
        # print(MFI_item)
        MFI_table_init()
        result = FPGrowth(transactions, 2)
        MFI_print(MFI_root)
        # for item in ans_path:
        #     print(item)
        # print(ans_path[-1])
        # attrs = str(ans_path[-1])[1:-2].replace("'", "")
        tmp_set = set()
        for cols in ans_path:
            tmp_set = tmp_set.union(set(cols))
        # attrDict[item_name] = ans_path[-1]
        attrDict[item_name] = list(tmp_set)
    return attrDict, condAll


if __name__ == '__main__':
    start = time.perf_counter()
    '''
    MFI_item, MFI_hashMap = getSortHead(list(transactions.values()), 2)
    MFI_table_init()
    result = FPGrowth(transactions, 2)
    MFI_print(MFI_root)
    for item in ans_path:
        print(item)
    '''

    itemAll = getData.get_trans()
    for item_name, item in itemAll.items():
        allClear()
        print(item_name)
        transactions = item
        MFI_item, MFI_hashMap = getSortHead(list(transactions.values()), 2)
        # print(MFI_item)
        MFI_table_init()
        result = FPGrowth(transactions, 2)
        MFI_print(MFI_root)
        # for item in ans_path:
        #     print(item)
        print(ans_path[-1])

    # 计时
    end = time.perf_counter()
    print('运行时间 = %ss' % (end - start))
