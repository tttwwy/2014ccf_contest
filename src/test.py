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
    # model.submit_board()
    # model = svm.SVM()

    # feature_list = ["us00","us01","uc00","uc01"]
    feature_lists = [
        # ["us00"],
        ["us12","us01"],
        # ["uc00"],
        # ["uc01"],
        # ["uc00","uc01"],
        # ["us00","us01"],
        # ["uc08"	,"uc09","uc10",	"uc11","uc12"],
        # ["us00","us01","us08","us09","us10","us11","us12"],
        # ["uc08","uc09","uc10","uc11","uc12"],
        # ["uc00","uc01","uc08","uc09","uc10","uc11","uc12"],
        #
        # ["u13","u14","u15","u16","u17","u18","u19","u20","us00"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","us01"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","uc00"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","uc01"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","uc00","uc01"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","us00","us01"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","uc08"	,"uc09","uc10",	"uc11","uc12"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","us00","us01","us08","us09","us10","us11","us12"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","uc08","uc09","uc10","uc11","uc12"],
        # ["u13","u14","u15","u16","u17","u18","u19","u20","uc00","uc01","uc08","uc09","uc10","uc11","uc12"],
        ]
    # fdata = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/","us01")

    ftrain_data = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/","us01")
    ftest_data = model.features_to_fdata("/home/wangzhe/ccf/data/feature/validation/","us01")
    # ftrain_data,ftest_data = model.divide_data(fdata,0.75)
    try:
        # balance_scales = [0.024,0.027,0.03,0.033,0.036]
        balance_scales = [0.08,0.06,0.07,0.05,]

        for balance_scale in balance_scales:
            # fdata = model.fdata_filter(fdata,lambda x:x[2].get('u16',0) > 0)
            model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data)
            # model.save_model("/home/wangzhe/ccf/data/model/lr")
            # for result_scale in [0,0.0027,0.003,0.0033]:
            for result_scale in [0,0.004,0.005,0.006,0.007,0.008,0.009,0.01]:
                # model.evaluate_fdata(ftest_data,result_scale)
                # model.load_model("/home/wangzhe/ccf/data/model/lr")
                model.submit_fdata(ftest_data,"submit.txt",result_scale)
                time.sleep(30)
                logging.info("train model end:")

    except Exception,e:
        logging.info(e)
