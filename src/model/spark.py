__author__ = 'WangZhe'
# coding=utf-8

import base
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector

import sys
from mylog import *

import random
import cPickle
import feature
import os
class SparkModel(base.BaseModel):

    def __init__(self,model_file_name = ""):
        self.map = {}

        base.BaseModel.__init__(self,model_file_name)
        # if not SparkModel.sc:
        #     SparkModel.sc = SparkContext(appName="ccf")
        try:
            SparkModel.sc = SparkContext(appName="ccf")
        except:
            pass

    @run_time
    def features_to_fdata(self,work_dir,*args):
        logging.info(args)
        new_data = self.file_to_data(os.path.join(work_dir,args[0] + ".txt"))
        for feature_name in args[1:]:
            file_name = os.path.join(work_dir,feature_name + ".txt")
            data = self.file_to_data(file_name)
            new_data = new_data + data
        self.feature_names = args

        fdata = self.data_to_fdata(new_data)

        return fdata

    def feature_to_fdata(self,file_name):
        data = SparkModel.sc.textFile(file_name)
        fdata = self.data_to_fdata(data)
        return fdata
    def get_sc(self):
        return SparkModel.sc

    @run_time
    def data_to_fdata(self,data):
        transform_set = feature.read_transform("/home/wangzhe/ccf/data/feature/transform.txt")
        transform_broadcast = SparkModel.sc.broadcast(transform_set)

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
            # print uid,label,value_map
            return (uid,label,value_map)

        result = data.map(handle).reduceByKey(lambda x,y:list(x)+list(y))
        # logging.info(result.take(10))
        fdata = result.map(handle2)
        # logging.info(fdata.take(10))
        return fdata

    def divide_file(self,file_name,scale):
        data = SparkModel.sc.textFile(file_name)
        return self.divide_data(data,scale)

    def divide_data(self,data,scale):
        seed = random.randint(0,10000)
        data = data.map(lambda x:(x[0],x))
        rdd1 = data.sample(False,scale,seed)
        rdd2 = data.subtractByKey(rdd1)
        rdd1 = rdd1.map(lambda x:x[1])
        rdd2 = rdd2.map(lambda x:x[1])
        return rdd1,rdd2

    def file_to_data(self,file_name):
        data = SparkModel.sc.textFile(file_name)
        return data

    def file_to_fdata(self,file_name):
        data = self.file_to_data(file_name)
        return self.data_to_fdata(data)


    @run_time
    def fdata_filter(self,fdata,f):
        logging.info("fdata count start:{0}".format(fdata.count()))
        filter_data = fdata.filter(f)
        logging.info("filter_data count end:{0}".format(filter_data.count()))
        return filter_data

    @run_time
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
        broadcast_map = SparkModel.sc.broadcast(map)
        broadcast_size = SparkModel.sc.broadcast(len(map))
        def parse(line):
            uid,label,values = line
            new_values = {}
            for key,value in values.iteritems():
                if key in broadcast_map.value:
                    new_values[broadcast_map.value[key]] = value
            return (uid,LabeledPoint(label,SparseVector(broadcast_size.value,new_values)))
        mdata = fdata.map(parse)
        return mdata

    # @run_time
    # def fdata_to_mdata(self,fdata):
    #     def parse(line):
    #         uid,label,values = line
    #         return (uid,LabeledPoint(label,SparseVector(23640,values)))
    #     mdata = fdata.map(parse)
    #     return mdata

    @run_time
    def balance(self,data1,data2,balance_scale=1.0):
        data1_count = data1.count()
        data2_count = data2.count()
        logging.info("{0} {1}".format(data1_count,data2_count))
        logging.info(balance_scale)
        max_data,min_data = (data1,data2) if data1_count > data2_count else (data2,data1)
        if balance_scale <= 1:
            logging.info("nono")
            scale = int(max(data1_count,data2_count)/min(data1_count,data2_count)) - 1
            new_min_data = min_data.flatMap(lambda x:[x] * int(scale*balance_scale))
            return max_data + new_min_data

        else:
            logging.info("lalala")
            scale = min(data1_count,data2_count)*balance_scale*1.0/(max(data1_count,data2_count))
            seed = random.randint(0,10000)
            new_max_data = max_data.sample(False,scale,seed)
            logging.info("{0} {1}".format(min_data.count(),new_max_data.count()))

            return min_data + new_max_data


    def mdata_to_balance_mdata(self,mdata,balance_scale):
        mdata_buy = self.fdata_filter(mdata,lambda x:x[1].label == '1')
        mdata_nobuy = self.fdata_filter(mdata,lambda x:x[1].label == '0')
        balance_mdata = self.balance(mdata_buy,mdata_nobuy,balance_scale)
        return balance_mdata

    @run_time
    def fdata_to_balance_fdata(self,fdata,balance_scale):
        if balance_scale == 0:
            return fdata
        fdata_buy = self.fdata_filter(fdata,lambda x:x[1] == '1')
        fdata_nobuy = self.fdata_filter(fdata,lambda x:x[1] == '0')
        balance_fdata = self.balance(fdata_buy,fdata_nobuy,balance_scale)
        return balance_fdata

    @run_time
    def mdata_to_file(self,mdata,save_file_name):
        logging.info(save_file_name)
        with open(save_file_name,'w') as f:
            for line in mdata.collect():
                uid = line[0]
                label = line[1].label
                features = line[1].features
                f.write("{0},{1},{2}\n".format(uid,label,",".join([str(x) for x in features.toArray()])))

    def train_fdata(self,fdata,balance_scale,**kwargs):
        self.map = {}
        mdata = self.fdata_to_mdata(fdata)
        balance_mdata = self.mdata_to_balance_mdata(mdata,balance_scale)
        train_data = balance_mdata.map(lambda x:x[1])
        self.train_args["scale"] = balance_scale
        self.train_mdata(mdata,balance_scale,**kwargs)

    @run_time
    def train_mdata(self,mdata,**kwargs):
        mtrain_data = mdata.map(lambda x:x[1])
        kwargs["data"] = mtrain_data
        self.train_model(**kwargs)
        self.train_args.update(kwargs)



    @run_time
    def save_model(self,model_file_name):
        model = self.model
        map = self.map
        with open(model_file_name + ".model","w") as f:
            cPickle.dump(model,f)
        with open(model_file_name + ".index","w") as f:
            cPickle.dump(map,f)

    @run_time
    def load_model(self,model_file_name):
        logging.info(model_file_name)
        with open(model_file_name+".model","r") as f:
            self.model = cPickle.load(f)
        with open(model_file_name+".index","r") as f:
            self.map = cPickle.load(f)

    @run_time
    def evaluate_fdata(self,fdata,result_num = 2000):
        uid_label_predict = self.predict_fdata(fdata)
        return self.get_score(uid_label_predict, result_scale=result_num)

    # 读取测试特征数据,得到准确率P,召回率R,F值
    @run_time
    def evaluate_mdata(self,mdata,result_num = 2000):
        uid_label_predict = self.predict_mdata(mdata)
        return self.get_score(uid_label_predict,result_scale=result_num)

    @run_time
    def get_score(self,uid_label_predict,result_scale):
        A = 0
        B = 0
        C = 0
        result_num = int(len(uid_label_predict) * result_scale)
        try:
            for index,(uid,label,predict) in enumerate(uid_label_predict):
                label = str(label)

                # predict = '1' if predict > 0.5 else '0'
                predict = '1' if index < result_num or (result_num == 0) else '0'
                if label == predict == '1':
                    A += 1
                elif label != '1' and predict == '1':
                    B += 1
                elif label == '1' and predict != '1':
                    C += 1


            logging.info("{0} {1} {2}".format(A,B,C))
            P = round(float(A )/ (A + B),4)
            R = round(float(A) / (A + C),4)
            F = round(2*P*R/(P+R),4)
            submit_count = A + B
            logging.info("{0} {1} {2}".format(P,R,F))
            self.record_submit(P,R,F,submit_count,result_scale)
            return P,R,F
        except Exception,e:
            logging.info(e)
            return 0,0,0

    def evaluate_file(self,file_name):
        fdata = self.file_to_fdata(file_name)
        mdata = self.fdata_to_mdata(fdata)
        return self.evaluate_mdata(mdata)

    # 得到数据的分类结果
    #
    # def predict(self,features):
    #     return self.model.predict_value(features)

    @run_time
    def predict_mdata(self,mdata):
        model = self.model
        uid_predict = mdata.map(lambda p:(p[0],p[1].label,model.predict_value(p[1].features)))
        # uid_predict = mdata.map(lambda p:(p[0],p[1].label,model.predict_value(p[1].features)))

        result = uid_predict.collect()
        result = sorted(result,lambda x,y:cmp(x[2],y[2]),reverse=True)
        logging.info(result[:100])
        logging.info(result[-100:])  
        return result

    def predict_fdata(self,fdata):
        mdata = self.fdata_to_mdata(fdata)
        return self.predict_mdata(mdata)

    @run_time
    def submit_file(self,input_file_name,save_file_name):
        fdata = self.feature_to_fdata(input_file_name)
        self.submit_fdata(fdata,save_file_name)

    @run_time
    def record_submit(self,P,R,F1,submit_count,result_scale=0):
        try:
            log_list = [F1,P,R,submit_count,result_scale,self.model_name,self.train_args['scale'],",".join(self.feature_names),self.train_args['min_click']]
            log_list.append(self.train_args['regType'])
            log_list.append(self.train_args['regParam'])
            log_list.append(self.train_args['iterations'])
            log_list.append(self.train_args['step'])
            log_list.append(self.train_args['miniBatchFraction'])
            log_list = [str(x).strip() for x in log_list]
            log_str = "{0}".format("\t".join(log_list))
            train_log(log_str)
            logging.info(log_str)
        except Exception:
            log_list = [F1,P,R,submit_count,self.model_name,",".join(self.feature_names)]
            log_list = [str(x).strip() for x in log_list]
            log_str = "{0}".format("\t".join(log_list))
            train_log(log_str)
            logging.info(log_str)
        return P,R,F1

    @run_time
    def submit_fdata(self,fdata,save_file_name,result_scale=1000):
        result_list = self.predict_fdata(fdata)
        submit_count = self.submit_data(result_list,save_file_name,result_scale)
        P,R,F1 = self.submit_board(save_file_name)
        self.record_submit(P,R,F1,submit_count,result_scale)
