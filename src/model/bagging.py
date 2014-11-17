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



    def train(self,**kwargs):
        self.model_list = kwargs['model_list']
        self.scale = kwargs['bagging_scale']


    def predict(self,fdata_features):
        values = {}
        size = len(self.model_list)
        for index,model in enumerate(self.model_list):
            new_features = {}
            map = model.map
            F = model.F
            model = model.model
            for key,value in fdata_features.iteritems():
                if key in map:
                    new_features[map[key]] = value
            values[index] = 1.0 if int(model.predict(SparseVector(len(map),new_features))) == 1 else -1.0
            values[index] = values[index] * F
        A = 0
        B = 0
        for value in values.values():
            if value > 0:
                A += value
            else:
                B += -value
        # logging.info("{0} {1}".format(A,B))
        if float(A) / (A + B) > self.scale:
            return '1'
        else:
            return '0'

class Bagging(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)
        self.model_name = 'MIX'

    def init_model(self):
        self.data_format_type = 1

    def train_fdata(self,fdata,**kwargs):
        self.train_model(**kwargs)
        kwargs['scale'] = kwargs['bagging_scale']
        self.train_args = kwargs

    @run_time
    def train_model(self,**kwargs):
        self.model = MIX()
        self.model.train(**kwargs)

    @run_time
    def predict_fdata(self,fdata):
        model = self.model
        uid_predict = fdata.map(lambda p:(p[0],p[1],model.predict(p[2])))
        result = uid_predict.collect()
        return result




