__author__ = 'WangZhe'
# coding=utf-8
import spark

class lr(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)

    def init_model(self):
        self.sc = spark.SparkContext(appName="svm")
        self.data_format_type = 1

    def train_data(self,data,*args):
        train_data = self.data.map(lambda x:x[1])
        self.model = spark.LogisticRegressionWithSGD.train(self.train_data)



