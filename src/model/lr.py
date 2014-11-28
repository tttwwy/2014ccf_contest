__author__ = 'WangZhe'
# coding=utf-8
import spark

from mylog import *
from pyspark.mllib.classification import *
from numpy import ndarray, float64, int64, int32, array_equal, array
from pyspark.mllib.regression import LinearRegressionModel

class LR(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)
        self.model_name = 'LR'

    def init_model(self):
        self.data_format_type = 1

    def read_model(self,file_name):
        result = []
        with open(file_name,'r') as f:
            for line in f:
                line = line.strip()
                result.append(0-float(line))


        self.model = LogisticRegressionModel(array(result),0)


    def train_model(self,**kwargs):
        self.model = LogisticRegressionWithSGD.train(**kwargs)







