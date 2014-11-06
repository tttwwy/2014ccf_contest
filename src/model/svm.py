__author__ = 'WangZhe'
# coding=utf-8
import spark
import logging
from pyspark.mllib.classification import SVMWithSGD


class SVM(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)

    def init_model(self):
        self.sc = spark.SparkContext(appName="svm")
        self.data_format_type = 1

    @logging.run_time
    def train_mdata(self,mdata,**kwargs):
        train_data = mdata.map(lambda x:x[1])
        kwargs["data"] = train_data
        self.model = SVMWithSGD.train(**kwargs)




