import os

from setuptools import setup
from Cython.Build import cythonize

all_files = os.listdir(".")
pyx_to_cythonize = [x for x in all_files if x.endswith(".pyx")]

setup(
    name='Materialized View',
    ext_modules=cythonize(pyx_to_cythonize),
    zip_safe=False,
)

# if __name__ == '__main__':
#     so_to_cythonize = [x for x in all_files if x.endswith(".so")]
#     for so in so_to_cythonize:
#         print(so, end=',')


"encodeFeatures_CH.cpython-38-aarch64-linux-gnu.so,basePlan.cpython-38-aarch64-linux-gnu.so,fpgrowth.cpython-38-aarch64-linux-gnu.so,encodeFeatures.cpython-38-aarch64-linux-gnu.so,FPMAX.cpython-38-aarch64-linux-gnu.so,getData.cpython-38-aarch64-linux-gnu.so,jobRL.cpython-38-aarch64-linux-gnu.so,jobWideDeep.cpython-38-aarch64-linux-gnu.so,getCandidate.cpython-38-aarch64-linux-gnu.so,joinCandidate.cpython-38-aarch64-linux-gnu.so,parsePipelineTime_CH.cpython-38-aarch64-linux-gnu.so,parsePlanJson.cpython-38-aarch64-linux-gnu.so,parsePlanJson_CH.cpython-38-aarch64-linux-gnu.so,parsePlanJson_PG.cpython-38-aarch64-linux-gnu.so,parseSchema.cpython-38-aarch64-linux-gnu.so,procCondition_CH.cpython-38-aarch64-linux-gnu.so,procOpt.cpython-38-aarch64-linux-gnu.so,procConf.cpython-38-aarch64-linux-gnu.so,procSQL.cpython-38-aarch64-linux-gnu.so,ress.cpython-38-aarch64-linux-gnu.so,trainTest.cpython-38-aarch64-linux-gnu.so,procCondition_PG.cpython-38-aarch64-linux-gnu.so,procCSVFile.cpython-38-aarch64-linux-gnu.so,procSparkEnv.cpython-38-aarch64-linux-gnu.so,recommend.cpython-38-aarch64-linux-gnu.so,train.cpython-38-aarch64-linux-gnu.so,encodeFeatures_SP.cpython-38-aarch64-linux-gnu.so,joinCandidate_SP.cpython-38-aarch64-linux-gnu.so,parseDotFile_SP.cpython-38-aarch64-linux-gnu.so,parseFilterSpecialCase_SP.cpython-38-aarch64-linux-gnu.so,parseNodeFile_SP.cpython-38-aarch64-linux-gnu.so,parsePlanFile_SP.cpython-38-aarch64-linux-gnu.so,printTree_SP.cpython-38-aarch64-linux-gnu.so"