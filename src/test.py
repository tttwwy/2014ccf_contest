# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import feature
from model.mylog import *



if __name__ == "__main__":
    mylog.info("begin:")
    model = lr.LR()
    # model = svm.SVM()
    # model.train_file("/home/wangzhe/ccf/data/feature/train/wz/result.txt",iterations=100,step = 1.0,miniBatchFraction=1.0,regParam=1.0,regType='l2',intercept=True)
    # logging.info(model.evaluate_file("/home/wangzhe/ccf/data/feature/validation/wz/result.txt"))
    # model.submit_file("/home/wangzhe/ccf/data/feature/validation/wz/result.txt","/home/wangzhe/ccf/data/feature/validation/wz/submit.txt")

    fdata = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/train/wz/us01*")
    mdata = model.fdata_to_mdata(fdata)
    model.train_mdata(mdata,iterations=100,step = 0.5,miniBatchFraction=1.0,regParam=1.0,regType='l2',intercept=False)

    model.evaluate_mdata(mdata)


    # fdata_test = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/validation/wz/us01*")
    # mdata_test = model.fdata_to_mdata(fdata_test)
    # model.submit_data(model.predict_mdata(mdata_test),'submit.txt')



    # fdata = model.file_to_fdata("/home/wangzhe/ccf/data/feature/train/wz/1.txt")
    # mdata = model.fdata_to_mdata(fdata)
    #
    # mtrain_data,mtest_data = model.divide_data(mdata,0.9)
    #
    # mdata_buy = model.fdata_filter(mtrain_data,lambda x:x[1].label == '1')
    # mdata_nobuy = model.fdata_filter(mtrain_data,lambda x:x[1].label == '0')
    #
    # mtrain_data = balance(mdata_buy,mdata_nobuy)
    #
    # model.train_mdata(mtrain_data,iterations=100,step = 1.0,miniBatchFraction=1.0,regParam=1.0,regType='l2',intercept=True)
    # logging.info(model.evaluate_mdata(mtest_data))

