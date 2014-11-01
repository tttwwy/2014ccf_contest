__author__ = 'WangZhe'
# coding=utf-8

import base
from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
import sys
import random
import cPickle

class SparkModel(base.BaseModel):

    def __init__(self,model_fie_name = ""):
        self.init_model()
        if not model_fie_name:
            self.load_model(model_fie_name)

    def divide_file(self,file_name,scale):
        data = self.sc.textFile(file_name)
        return self.divide_data(data,scale)

    def divide_data(self,data,scale):
        count_num = data.count()
        seed = random.randint(1,99999)
        rdd1 = data.takeSample(False,count_num * scale,seed)
        rdd2 = data.subtract(rdd1)
        return rdd1,rdd2

    # 读取文件到RDD,并转化为labelpoint格式
    def format_file(self,file_name):
        data = self.sc.textFile(file_name)
        return data

    def format_data(self,data):
        def parse_line(line):
            line_list = line.strip().split("\t")
            uid = line_list[0]
            label = line_list[1]
            if self.data_format_type == 0:
                values = [float(x[1]) for x in line_list[2:]]
            else:
                values = [(x[0],float(x[1])) for x in line_list[2:]]
            return ( uid,LabeledPoint(label,values) )

        result_data = data.map(parse_line)
        return result_data

    # 读取特征文件,进行训练
    def train_file(self,feature_file_name,*args):
        data = self.format_data(feature_file_name)
        self.train_data(data,*args)

    def save_model(self,model_file_name):
        with open(model_file_name,"w") as f:
            cPickle.dump(self.model,f)

    def load_model(self,model_file_name):
        with open(model_file_name,"r") as f:
            cPickle.load(self.model,f)

    # 读取测试特征数据,得到准确率P,召回率R,F值
    def evaluate_data(self,data):
        uid_label_predict = data.map(lambda p:(p[0],p[1].label,self.model.predict(p[1].features)))
        A = 0
        B = 0
        C = 0
        for uid,label,predict in uid_label_predict.collect():
            if label == predict == '1':
                A += 1
            elif label == '1' and predict == '0':
                B += 1
            elif label == '0' and predict == '1':
                C += 1

        P = float(A )/ (A + B)
        R = float(A) / (A + C)
        F = 2*P*R/(P+R)
        return P,R,F

    def evaluate_file(self,file_name):
        data = self.format_data(file_name)
        return self.evaluate_data(data)

    # 得到数据的分类结果
    def predict_data(self,data):
        uid_predict = data.map(lambda p:(p[0],self.model.predict(p[1].features)))
        return uid_predict.collect()


    def predict_file(self,file_name):
        data = self.format_data(file_name)
        return self.predict_data(data)




