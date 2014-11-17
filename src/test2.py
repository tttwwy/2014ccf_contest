# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import feature
from model.mylog import *

def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

if __name__ == "__main__":
    logging.info("train model begin:")
    model = lr.LR()
    fdata1 = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/","u16")
    fdata1 = fdata1.map(lambda x:x[0])
    uids = set(fdata1.collect())




    feature_lists = [
        ["us12","us01"]
        ]
    for feature_list in feature_lists:
        fdata = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",*feature_list)
        # ftest_data = model.features_to_fdata("/home/wangzhe/ccf/data/feature/validation/",*feature_list)
        # fdata = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",)
        fdata = fdata.filter(lambda x:x[0] in uids)
        ftrain_data,ftest_data = model.divide_data(fdata,0.75)
        try:
            balance_scales = [0.1,0.08,0.07,0.06,0.02,0.03,0.04]

            for balance_scale in balance_scales:
                # fdata = model.fdata_filter(fdata,lambda x:x[2].get('u16',0) > 0)
                model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data)
                # model.save_model("/home/wangzhe/ccf/data/model/lr")
                # for result_scale in [0,0.0027,0.003,0.0033]:
                for result_scale in [0,0.002,0.003,0.004,0.005,0.006,0.007,0.008,0.009,0.01]:
                    model.evaluate_fdata(ftest_data,result_scale)
                    # model.load_model("/home/wangzhe/ccf/data/model/lr")
                    # model.submit_fdata(ftest_data,"submit.txt",result_scale)
                    # time.sleep(50)
                    logging.info("train model end:")

        except Exception,e:
            logging.info(e)
