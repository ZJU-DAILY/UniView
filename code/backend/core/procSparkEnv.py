import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from procConf import *


def get_distributed_spark_session(master=None, num_proc=1):
    os.environ['PYSPARK_PYTHON'] = getPySparkPython()
    os.environ['PYSPARK_DRIVER_PYTHON'] = getPySparkDrivePython()
    # os.environ['HADOOP_CONF_DIR'] = '/usr/local/hadoop/etc/hadoop'

    conf = SparkConf().setAppName(getAppName())
    if master:
        print("on spark cluster")
        conf.setMaster(master)
        conf.set('spark.num.executors', getSparkNumExecutors()) \
            .set('spark.executor.cores', str(num_proc)) \
            .set('spark.executor.memory', getSparkExecutorMemory()) \
            .set('spark.driver.memory', getDriverMemory())
        # conf.set('spark.files', './upload.zip')
        # conf.set('spark.py.files', './upload.zip')
        # if master.startswith('yarn'):
        #     conf.set('spark.deploy.mode', 'client')

    else:
        conf.setMaster(f'local[{num_proc}]')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    if master:
        print('uploading code files')
        sc.addPyFile('./upload.zip')

    return spark
