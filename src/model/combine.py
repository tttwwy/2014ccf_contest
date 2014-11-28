__author__ = 'WangZhe'
# coding=utf-8
import spark
from mylog import *
import os
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector

class MIX():
    def __init__(self,core_model_name='lr'):
        self.core_model_name = core_model_name

    # def train(self,data,model_list,**kwargs):
    #     def handle(line):
    #         uid,label,features = line
    #         values = {}
    #         size = len(model_list)
    #         for index,model in enumerate(model_list):
    #             map = model.map
    #             model = model.model
    #             new_features = {}
    #             for key,value in features.iteritems():
    #                 if key in map:
    #                     new_features[map[key]] = value
    #             values[index] = model.predict(SparseVector(len(map),new_features))
    #
    #         return LabeledPoint(label,SparseVector(size,values))
    #
    #     new_train_mdata = data.map(handle)
    #     kwargs['data'] = new_train_mdata
    #     if self.core_model_name == "lr":
    #         self.core_model = LogisticRegressionWithSGD.train(**kwargs)
    #     else:
    #         self.core_model = SVMWithSGD.train(**kwargs)
    #     logging.info([x for x in self.core_model.weights])
    #     self.model_list = model_list


    def train(self,data,model_list,**kwargs):
        self.model_list = model_list

    # def predict(self,fdata_features):
    #     values = {}
    #     size = len(self.model_list)
    #     for index,model in enumerate(self.model_list):
    #         new_features = {}
    #         map = model.map
    #         model = model.model
    #         # logging.info(fdata_features)
    #         for key,value in fdata_features.iteritems():
    #             if key in map:
    #                 new_features[map[key]] = value
    #         values[index] = model.predict(SparseVector(len(map),new_features))
    #
    #
    #     return self.core_model.predict(SparseVector(size,values))

    def predict(self,fdata_features):
        values = {}
        size = len(self.model_list)
        for index,model in enumerate(self.model_list):
            new_features = {}
            map = model.map
            model = model.model
            # logging.info(fdata_features)
            for key,value in fdata_features.iteritems():
                if key in map:
                    new_features[map[key]] = value
            values[index] = model.predict(SparseVector(len(map),new_features))

        A = 0
        B = 0
        for value in values.values():
            if int(value) == 1:
                A += 1
            else:
                B += 1
        # if A != 0:
        #     logging.info("{0} : {1}".format(values,"1" if A > B else '0'))
        return "1" if A > 1 else '0'

class Combine(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)
        self.model_name = 'MIX'

    def init_model(self):
        self.data_format_type = 1

    @run_time
    def train_model(self,core_model,**kwargs):
        self.model = MIX(core_model)
        self.model.train(**kwargs)

    def train_fdata(self,fdata,balance_scale,core_model='svm',**kwargs):
        balance_fdata = self.fdata_to_balance_fdata(fdata,balance_scale)
        # logging.info(balance_fdata.take(3))
        kwargs["data"] = balance_fdata
        self.train_model(core_model,**kwargs)
        self.train_args["scale"] = balance_scale
        self.train_args.update(kwargs)

    @run_time
    def predict_fdata(self,fdata):

        model = self.model
        uid_predict = fdata.map(lambda p:(p[0],p[1],model.predict(p[2])))
        result = uid_predict.collect()
        return result




