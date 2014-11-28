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
import time
if __name__ == "__main__":

    logging.info("train model begin:")
    model = lr.LR()
    model.read_model("/home/wangzhe/ccf/data/feature/weight.txt")

    #
    # # model = svm.SVM()
    #
    feature_lists = [
        ['us00']
        ]
    for feature_list in feature_lists:
    #     train_data = model.file_to_data("/home/wangzhe/ccf/data/feature/train/spid.txt")
    #     def handle(line):
    #         uid = '1'
    #         label,values = line.strip().split("\t")
    #         values = values.split(" ")
    #         value_map = {}
    #         for item in values:
    #             key,value = item.split(":")
    #             value_map[int(key)] = float(value)
    #             return (uid,LabeledPoint(label,SparseVector(23639,value_map)))
    #     mtrain_data = train_data.map(handle)

        # ftest_data = model.features_to_fdata("/home/wangzhe/ccf/data/feature/validation/",*feature_list)
        #
        # def handle_test(line):
        #     uid,label,values = line
        #     new_values = {}
        #     for key,value in values.iteritems():
        #         new_key = int(key[8:])-1
        #         if new_key < 23639:
        #             new_values[new_key] = value
        #     return (uid,LabeledPoint(label,SparseVector(23639,new_values)))
        #
        # mtest_data = ftest_data.map(handle_test)

        test_data = model.file_to_data("/home/wangzhe/ccf/data/feature/validation/test.txt")
        transform_set = feature.read_transform("/home/wangzhe/ccf/data/feature/transform.txt")
        transform_broadcast = model.get_sc().broadcast(transform_set)

        def handle(line):
            uid,values = line.strip().split("\t")
            label = '1' if uid in transform_broadcast.value else '0'
            values = values.split(" ")
            value_map = {}
            for item in values:
                key,value = item.split(":")
                value_map[int(key)-1] = float(value)
            return (uid,LabeledPoint(label,SparseVector(23639,value_map)))


        mtest_data = test_data.map(handle)
        logging.info(mtest_data.take(10))
        balance_scales = [0.08]
        for balance_scale in balance_scales:
            model.map = {}
            model.train_args['scale'] = balance_scale
            # model.train_mdata(mdata=mtrain_data,regType='none',regParam=0.1,iterations=300,step = 1.0,miniBatchFraction=1.0,intercept=False)
            for result_scale in [0,0.0005,0.006,0.007,0.003,0.004,0.005,0.006,0.007,0.008,0.009,0.01]:
                model.evaluate_mdata(mtest_data,result_scale)
                logging.info("train model end:")
