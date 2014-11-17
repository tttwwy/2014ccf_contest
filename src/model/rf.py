__author__ = 'WangZhe'
# coding=utf-8
import spark
from mylog import *
import os

class RF(spark.SparkModel):
    def __init__(self,model_name = ""):
        spark.SparkModel.__init__(self,model_name)
        self.model_name = 'RF'
        self.feature_name = "rf_train.txt"
        self.model_name = "forest"
        self.feature_local_dir = "/home/wangzhe/ccf/data/temp/rf.txt"
        self.feature_hdfs_dir = "/ccf/rf/feature/"
        self.model_hdfs_dir = "/ccf/rf/model/"

    def init_model(self):
        self.data_format_type = 1

    def train_model(self,mdata,tree_num=100,feature_num=100):

        self.mdata_to_file(mdata,os.path.join(self.feature_local_dir,self.feature_name))
        os.popen("hrm {0}*".format(self.feature_hdfs_dir))
        os.popen("hput {0} {1}".format(os.path.join(self.feature_local_dir,self.feature_name),self.feature_hdfs_dir))

        os.popen("hrm {0}".format(os.path.join(self.model_hdfs_dir,self.model_name)))
        os.popen("hadoop jar /opt/mahout-distribution-0.9/mahout-core-0.9-job.jar org.apache.mahout.classifier.df.tools.Describe -p {0} -f {1} -d I L {3} N".format(
            os.path.join(self.feature_hdfs_dir,self.feature_name),
            os.path.join(self.feature_hdfs_dir,self.feature_name+".info"),len(self.map)))

        os.popen("hadoop jar /opt/mahout-distribution-0.9/mahout-examples-0.9-job.jar org.apache.mahout.classifier.df.mapreduce.BuildForest -Dmapred.max.split.size=1874231 -d {0} -ds {1} -sl {2} -p -t {3} -o {4}".format(
            os.path.join(self.feature_hdfs_dir,self.feature_name),
            os.path.join(self.feature_hdfs_dir,self.feature_name+".info"),feature_num,tree_num,
            os.path.join(self.model_hdfs_dir,self.model_name)))

    def predict_mdata(self,mdata):
        predict_feature_name = "rf_predict.txt"
        test_hdfs_dir = "/ccf/rf/test/"
        self.mdata_to_file(mdata,os.path.join(self.feature_local_dir,predict_feature_name))
        os.popen("hrm {0}".format(os.path.join(self.feature_local_dir,predict_feature_name)))
        os.popen("hrm {0}".format(os.path.join(test_hdfs_dir,"*")))

        os.popen("hput {0} {1}".format(os.path.join(self.feature_local_dir,"rf_predict.txt"),self.feature_hdfs_dir))
        os.popen("hadoop jar /opt/mahout-distribution-0.9/mahout-examples-0.9-job.jar org.apache.mahout.classifier.df.mapreduce.TestForest -i {0} -ds {1} -m {2} -a -mr -o {3}".format(
            os.path.join(self.feature_hdfs_dir,predict_feature_name),
            os.path.join(self.feature_hdfs_dir,self.feature_name+".info"),
            os.path.join(self.model_hdfs_dir,self.model_name),
            os.path.join(test_hdfs_dir,"result.txt") ))

        os.popen("rm {0}".format(os.path.join(self.feature_local_dir,"result.txt")))

        os.popen("hput {0} {1}".format(os.path.join(test_hdfs_dir,"result.txt"),os.path.join(self.feature_local_dir,"result.txt")))

        result_list = []
        with open(os.path.join(self.feature_local_dir,predict_feature_name),'r') as f1:
            with open(self.feature_local_dir,"result.txt",'r') as f2:
                for label in f2:
                    label = label.strip()
                    uid = f1.readline().strip().split(",")[0]
                    result_list.append((uid,label))

        return result_list




