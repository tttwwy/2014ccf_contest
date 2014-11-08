__author__ = 'WangZhe'
# coding=utf-8

import base
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector

import sys
from model.mylog import *
import random
import cPickle
import feature
class SparkModel(base.BaseModel):

    def __init__(self,model_file_name = ""):
        base.BaseModel.__init__(self,model_file_name)
        self.map = {}

    @run_time
    def feature_to_fdata(self,file_name):
        transform_set = feature.read_transform("/home/wangzhe/ccf/data/feature/train/wz/transform.txt")
        def handle(x):
            line = x.split("\t")
            return line[0],line[1:]

        def handle2(x):
            uid,values = x
            label = '1' if uid in transform_broadcast.value else '0'
            value_map = {}
            for item in values:
                key,value = item.split(":")
                value_map[key] = float(value)
            return uid,label,value_map

        data = self.sc.textFile(file_name)
        result = data.map(handle).reduceByKey(lambda x,y:list(x)+list(y))
        transform_broadcast = self.sc.broadcast(transform_set)
        fdata = result.map(handle2)

        return fdata

    def divide_file(self,file_name,scale):
        data = self.sc.textFile(file_name)
        return self.divide_data(data,scale)

    def divide_data(self,data,scale):
        # count_num = data.count()
        seed = random.randint(0,10000)
        # logging.info(seed)
        rdd1 = data.sample(False,scale,seed)
        rdd2 = data.subtract(rdd1)
        return rdd1,rdd2

    def file_to_data(self,file_name):
        data = self.sc.textFile(file_name)
        return data

    def file_to_fdata(self,file_name):
        data = self.file_to_data(file_name)
        return self.data_to_fdata(data)

    def data_to_fdata(self,data):
        def parse_line(line):
            line_list = line.strip().split("\t")
            uid = line_list[0]
            label = line_list[1]
            values = {}
            for item in line_list[2:]:
                key,value = item.split(":")
                values[key] = float(value)
            return (uid,label,values)

        fdata = data.map(parse_line)
        return fdata

    def fdata_filter(self,fdata,f):
        filter_data = fdata.filter(f)
        return filter_data


    def get_fdata_map(self,fdata):
        if self.map:
            return self.map
        feature_data = fdata.flatMap(lambda x:x[2].keys())
        index = 0
        new_map = {}
        for feature_name in feature_data.distinct().collect():
            new_map[feature_name] = index
            index += 1
        self.map = new_map
        return new_map

    @run_time
    def fdata_to_mdata(self,fdata):
        map = self.get_fdata_map(fdata)
        broadcast_map = self.sc.broadcast(map)
        broadcast_size = self.sc.broadcast(len(map))
        def parse(line):
            uid,label,values = line
            new_values = {}
            for key,value in values.iteritems():
                if key in broadcast_map.value:
                    new_values[broadcast_map.value[key]] = value
            return (uid,LabeledPoint(label,SparseVector(broadcast_size.value,new_values)))
        mdata = fdata.map(parse)
        return mdata

    def balance(self,data1,data2):
        data1_count = data1.count()
        data2_count = data2.count()
        mylog.info("{0} {1}".format(data1_count,data2_count))
        max_data,min_data = (data1,data2) if data1_count > data2_count else (data2,data1)
        scale = int(max(data1_count,data2_count)/min(data1_count,data2_count)) - 1
        new_min_data = min_data.flatMap(lambda x:[x] * int(scale*0.1))
        return max_data + new_min_data
    # # 读取文件到RDD,并转化为labelpoint格式
    #
    # @logging.run_time
    # def format_file(self,file_name):
    #     data = self.sc.textFile(file_name)
    #     # logging.info(data.collect())
    #     result_data =  self.format_data(data)
    #     # logging.info(result_data.collect())
    #     return result_data

    # @logging.run_time
    # def count_data_size(self,data):
    #     def parse_line(line):
    #         line_list = line.strip().split("\t")
    #         indexs = [int(x.split(":")[0]) for x in line_list[2:]]
    #         return max(indexs)
    #     size = data.map(parse_line).reduce(lambda x,y:max((x,y))) + 1
    #     return size
    #
    # @logging.run_time
    # def count_fdata_size(self,fdata):
    #     def parse_line(line):
    #         line_list = line.strip().split("\t")
    #         indexs = [int(x.split(":")[0]) for x in line_list[2:]]
    #         return max(indexs)
    #     size = data.map(parse_line).reduce(lambda x,y:max((x,y))) + 1
    #     return size

    # @logging.run_time
    # def format_data(self,data):
    #     size = self.count_data_size(data)
    #     logging.info(size)
    #     if self.data_format_type == 0:
    #         def parse_line(line):
    #             line_list = line.strip().split("\t")
    #             uid = line_list[0]
    #             label = line_list[1]
    #             values = [float(x.split(":")[1]) for x in line_list[2:]]
    #             return ( uid,LabeledPoint(label,values) )
    #     else:
    #         def parse_line(line):
    #             line_list = line.strip().split("\t")
    #             uid = line_list[0]
    #             label = line_list[1]
    #             values = {}
    #             for item in line_list[2:]:
    #                 key,value = item.split(":")
    #                 values[int(key)] = float(value)
    #             logging.info("{0} {1}:{2}".format(uid,label,values))
    #             return ( uid,LabeledPoint(label,SparseVector(size,values)) )
    #
    #     result_data = data.map(parse_line)
    #     return result_data


    # 读取特征文件,进行训练
    @run_time
    def train_file(self,feature_file_name,**kwargs):
        fdata = self.file_to_fdata(feature_file_name)
        mdata = self.fdata_to_mdata(fdata)
        self.train_mdata(mdata,**kwargs)


    def save_model(self,model_file_name):
        model = self.model
        with open(model_file_name,"w") as f:
            cPickle.dump(model,f)

    def load_model(self,model_file_name):
        with open(model_file_name,"r") as f:
            self.model = cPickle.load(f)

    # 读取测试特征数据,得到准确率P,召回率R,F值
    @run_time
    def evaluate_mdata(self,mdata):
        try:
            model = self.model
            def handle(p):
                # logging.info("{0} {1} {2}".format(p[0],p[1].label,p[1].features))
                uid = p[0]
                label = p[1].label
                features = p[1].features
                return uid,label,model.predict(features)

            uid_label_predict = mdata.map(handle)
            A = 0
            B = 0
            C = 0
            for uid,label,predict in uid_label_predict.collect():
                # logging.info("predict:{0}:{1}".format(label,predict),'blue')

                label = str(label)
                predict = str(predict)
                # if predict == '1' or label == '1':
                #     logging.info("predict:{0}:{1}".format(label,predict),'blue')
                if label == predict == '1':
                    A += 1
                elif label != '1' and predict == '1':
                    B += 1
                elif label == '1' and predict != '1':
                    C += 1
            mylog.info("{0} {1} {2}".format(A,B,C),'blue')
            P = float(A )/ (A + B)
            R = float(A) / (A + C)
            F = 2*P*R/(P+R)
            mylog.info("{0} {1} {2}".format(P,R,F))
            return P,R,F
        except Exception:
            return 0,0,0

    def evaluate_file(self,file_name):
        fdata = self.file_to_fdata(file_name)
        mdata = self.fdata_to_mdata(fdata)
        return self.evaluate_mdata(mdata)

    # 得到数据的分类结果

    @run_time
    def predict_mdata(self,mdata):
        model = self.model
        # logging.info(data.collect())
        uid_predict = mdata.map(lambda p:(p[0],model.predict(p[1].features)))
        # uid_predict = data.map( lambda p:(p[0],p[1].features,model.predict(p[1].features)) )

        # logging.info(uid_predict.collect())
        return uid_predict.collect()

    # @logging.run_time
    # def predict_file(self,file_name):
    #     data = self.format_file(file_name)
    #     return self.predict_data(data)

    @run_time
    def submit_file(self,input_file_name,save_file_name):
        fdata = self.file_to_fdata(input_file_name)
        mdata = self.fdata_to_mdata(fdata)
        result_list = self.predict_mdata(mdata)
        self.submit_data(result_list,save_file_name)


