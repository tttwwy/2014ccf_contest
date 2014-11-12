# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import feature
from model.mylog import *



if __name__ == "__main__":
    logging.info("train model begin:")
    model = lr.LR()
    # model = svm.SVM()

    fdata = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/train/wz/u*.txt")
    # fdata = model.fdata_filter(fdata,lambda x:x[2].get('u16',0) > 0)
    mdata = model.fdata_to_mdata(fdata)
    # model.mdata_to_file(mdata,"feature.txt")
    # logging.info('''/opt/hadoop-2.2.0/bin/hadoop jar
    # /opt/mahout-distribution-0.9/mahout-core-0.9-job.jar
    #  org.apache.mahout.classifier.df.tools.Describe -p
    #  /data/feature/feature.txt -f /data/feature/result.txt -d l L {0} N'''.format(len(model.map)))
    model.train_mdata(balance_scale=0.05,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,mdata=mdata)
    model.save_model("/home/wangzhe/ccf/data/feature/train/lr")


    model.load_model("/home/wangzhe/ccf/data/feature/train/lr")
    ftest_data = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/validation/wz/u*.txt")
    model.submit_fdata(ftest_data,"submit.txt")
    logging.info("train model end:")


