# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import combine

from model import feature
from model.mylog import *
from model import bagging
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.classification import *
from numpy import ndarray, float64, int64, int32, array_equal, array
from pyspark.mllib.regression import LinearRegressionModel
import time
if __name__ == "__main__":

    logging.info("train model begin:")
    model = lr.LR()
    # model.read_model("/home/wangzhe/ccf/data/feature/weight.txt")


    #
    # # model = svm.SVM()
    #
    feature_lists = [
        ['us00']
        ]
    result = []
    with open("/home/wangzhe/ccf/data/feature/weight.txt",'r') as f:
        for line in f:
            line = line.strip()
            result.append(0-float(line))

    for feature_list in feature_lists:
        train_data = model.file_to_data("/home/wangzhe/ccf/data/feature/train/spid.txt")
        transform_set = feature.read_transform("/home/wangzhe/ccf/data/feature/transform.txt")
        transform_broadcast = model.get_sc().broadcast(transform_set)

        def handle(line):
            uid = 'mzid1000572'
            label,values = line.strip().split("\t")
            values = values.split(" ")
            value_map = {}
            for item in values:
                key,value = item.split(":")
                value_map[int(key)-1] = float(value)
            return (uid,LabeledPoint(label,SparseVector(23639,value_map)))

        mtrain_data = train_data.map(handle)
        logging.info([(x[0],x[1].label,x[1].features) for x in mtrain_data.take(10)])

        test_data = model.file_to_data("/home/wangzhe/ccf/data/feature/validation/test.txt")

        def handle_test(line):
            uid,values = line.strip().split("\t")
            label = '1' if uid in transform_broadcast.value else '0'
            values = values.split(" ")
            value_map = {}
            for item in values:
                key,value = item.split(":")
                value_map[int(key)-1] = float(value)
            return (uid,LabeledPoint(label,SparseVector(23639,value_map)))


        mtest_data = test_data.map(handle_test)


        logging.info([(x[0],x[1].label,x[1].features) for x in mtest_data.take(10)])

        balance_scales = [0.08]
        for balance_scale in balance_scales:
            model.train_args['scale'] = balance_scale
            model.train_mdata(mdata=mtest_data,initialWeights=array([0]*23639),regType='none',regParam=0.001,iterations=1000,step = 0.1,miniBatchFraction=1.0,intercept=False)
            # model.save_model("lr1")
            for result_scale in [0,0.0005,0.0006,0.006,0.007,0.003,0.004,0.005,0.006,0.007,0.008,0.009,0.01]:
                model.evaluate_mdata(mtest_data,result_scale)
        logging.info("train model end:")
