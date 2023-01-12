from parseFilterSpecialCase_SP import *


cur_node_step = 0
cur_node_cnt = 0
def check_skip_node():
    global cur_node_step, cur_node_cnt
    cur_node_cnt += 1
    return cur_node_cnt % 2 == 1


def find_hash_from_table(check_str):
    for table, data_dict in g_table.data.items():
        for field, type in data_dict.items():
            field = field.lower()
            if check_str == field:
                return table + "." + field
    return ""


def parse_HashAggregate(line, relationDict, aliasDict, tableField):
    if not check_skip_node():
        return False, [], []
    group_by_list = []
    agg_list = []
    line = line.strip().split("HashAggregate(")[1].strip().strip(")")
    keys, functions = line.split(", functions=[")[0:2]
    keys = keys.strip().split("keys=[")[1].strip("]")
    functions = functions.strip().strip("]")
    # parse keys
    key_list = parseBracketFilter(keys)
    for key in key_list:
        key = key.strip()
        # substr
        if key.find(" AS ") != -1:
            key = key.split(" AS ")[0].strip()
        pattern = r"substr\(([\w0-9#]+), [0-9]+, [0-9]+\)"
        k_p = re.findall(pattern, key)
        if len(k_p) > 0:
            k_p = k_p[0]
            field, num = k_p.split("#")[0:2]
            field, num = field.strip(), num.strip()
            if num in relationDict:
                display_name = relationDict[num].displayName
            else:
                if num in aliasDict:
                    display_name = aliasDict[num]["displayName"]
                else:
                    display_name = find_hash_from_table(field)
                    if display_name == "":
                        print(f"[parse_HashAggregate] cant find {num}")
                        continue
            group_by_key = re.sub(pattern, display_name.lower(), key)
        else:
            field, num = key.split("#")[0:2]
            field, num = field.strip(), num.strip()
            if num in relationDict:
                display_name = relationDict[num].displayName
            else:
                if num in aliasDict:
                    display_name = aliasDict[num]["displayName"]
                else:
                    display_name = find_hash_from_table(field)
                    if display_name == "":
                        print(f"[parse_HashAggregate] cant find {num}")
                        continue
            group_by_key = display_name.lower()
        if group_by_key not in group_by_list:
            group_by_list.append(group_by_key)

    # parse functions
    functions = remove_unuse_parameter(functions)
    functions = remove_unuse_agg(functions)
    functions_list = parseBracketFilter(functions)
    for func in functions_list:
        pattern = r"([\w]+)\#([0-9L]+)"
        key_list = re.findall(pattern, func)
        for key in key_list:
            field, num = key[0], key[1]
            if num in aliasDict:
                display_name = aliasDict[num]["displayName"]
            else:
                display_name = find_hash_from_table(field)
                if display_name == "":
                    print(f"[parse_HashAggregate] functions find {num}")
                    continue
            func = func.replace(field + "#" + num, display_name.lower())
        # 处理字符的情况
        pattern = r"([\w]+)\.([\w]+) = ([\w]+)"
        key_list = re.findall(pattern, func)
        for key in key_list:
            table, field, key_str = key[0], key[1], key[2]
            if tableField[table.lower()][field.lower()]['type'] == 'string':
                func = func.replace(key_str, "\'" + key_str + "\'")
        if func not in agg_list:
            agg_list.append(func)

    return True, group_by_list, agg_list



def parse_SortAggregate(line):
    pass