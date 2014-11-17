__author__ = 'WangZhe'
# coding=utf-8
import spark
from mylog import *
from pyspark.mllib.classification import SVMWithSGD


class SVM(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)
        self.model_name = 'SVM'

    def init_model(self):
        self.data_format_type = 1

    def train_model(self,**kwargs):
        self.model = SVMWithSGD.train(**kwargs)




