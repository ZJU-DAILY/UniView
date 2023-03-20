import pickle
import re
import json

def normalize_json(file_name):
    new_json_file = ""
    with open(file_name, 'r', encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            if line.find("QUERY PLAN") != -1:
                continue
            if line == "\n":
                continue
            if line.find("-------------") != -1:
                continue
            if re.findall(r"\(.*row\)", line):
                continue
            line = re.sub(r"[ ]*\+", "", line)
            new_json_file += line
    return new_json_file






if __name__ == "__main__":
    # normalize_json("1b_52 (3).json")
    file_name = open("1b_52 (5).json", "r", encoding='utf-8')
    f = json.load(file_name)
    print(f)