__author__ = 'WangZhe'
# coding=utf-8
import spark
from mylog import *
from pyspark.mllib.classification import LogisticRegressionWithSGD

class LR(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)

    def init_model(self):
        self.sc = spark.SparkContext(appName="lr")
        self.data_format_type = 1

    @run_time
    def train_mdata(self,mdata,balance_scale,**kwargs):
        balance_mdata = self.mdata_to_balance_mdata(mdata,balance_scale)
        train_data = balance_mdata.map(lambda x:x[1])
        kwargs["data"] = train_data
        self.model = LogisticRegressionWithSGD.train(**kwargs)




