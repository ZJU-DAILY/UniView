import time
import os
import shutil


def get_current_time():
    now = time.localtime()
    nowTime = time.strftime("%Y-%m-%d %H:%M", now)
    return nowTime, now


def remove_directory_file(path):
    try:
        shutil.rmtree(path)
        os.mkdir(path)
    except OSError as e:
        print("error: {0}; path: {1}".format(path, e.strerror))

def load_file(file_path):
    f = open(file_path)
    content = f.read()
    f.close()
    return content