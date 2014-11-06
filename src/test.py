# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import logging


def balance(data1,data2):
    data1_count = data1.count()
    data2_count = data2.count()
    logging.info("{0} {1}".format(data1_count,data2_count))
    max_data,min_data = (data1,data2) if data1_count > data2_count else (data2,data1)
    scale = int(max(data1_count,data2_count)/min(data1_count,data2_count)) - 1
    min_data = min_data.flatMap(lambda x:[x] * scale)
    return max_data + min_data

if __name__ == "__main__":
    logging.info("begin:")
    # model = lr.LR()
    model = svm.SVM()
    model.train_file("/home/wangzhe/ccf/data/feature/train/wz/result.txt",iterations=100,step = 1.0,miniBatchFraction=1.0,regParam=1.0,regType='l2',intercept=True)
    logging.info(model.evaluate_file("/home/wangzhe/ccf/data/feature/validation/wz/result.txt"))
    model.submit_file("/home/wangzhe/ccf/data/feature/validation/wz/result.txt","/home/wangzhe/ccf/data/feature/validation/wz/submit.txt")

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

