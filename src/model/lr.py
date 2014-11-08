__author__ = 'WangZhe'
# coding=utf-8
import spark
from model.mylog import *
from pyspark.mllib.classification import LogisticRegressionWithSGD

class LR(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)

    def init_model(self):
        self.sc = spark.SparkContext(appName="lr")
        self.data_format_type = 1

    @run_time
    def train_mdata(self,mdata,**kwargs):
        mdata_buy = self.fdata_filter(mdata,lambda x:x[1].label == '1')
        mdata_nobuy = self.fdata_filter(mdata,lambda x:x[1].label == '0')
        train_mdata = self.balance(mdata_buy,mdata_nobuy)
        train_data = train_mdata.map(lambda x:x[1])
        kwargs["data"] = train_data
        self.model = LogisticRegressionWithSGD.train(**kwargs)



