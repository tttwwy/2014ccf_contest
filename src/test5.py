# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import combine

from model import feature
from model.mylog import *

def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

if __name__ == "__main__":
    # 特征筛选实验
    logging.info("train model begin:")
    feature_lists = [
    (["uc01","us01","uc12","us12"],0.05),
    ]

    # model_list = [
    # svm.SVM("/home/wangzhe/ccf/data/model/SVM.uc01,us01,uc12,us12.0.05"),
    # svm.SVM("/home/wangzhe/ccf/data/model/SVM.uc01.0.05"),
    # # svm.SVM("/home/wangzhe/ccf/data/model/SVM.uc12.0.05"),
    # svm.SVM("/home/wangzhe/ccf/data/model/SVM.uc12.0.08"),
    # svm.SVM("/home/wangzhe/ccf/data/model/SVM.us01.0.03"),
    # svm.SVM("/home/wangzhe/ccf/data/model/SVM.us12.0.03")
    # ]

    # model_list = [
    # lr.LR("/home/wangzhe/ccf/data/model/LR.uc01,us01,uc12,us12.0.05"),
    # lr.LR("/home/wangzhe/ccf/data/model/LR.uc01.0.05"),
    # lr.LR("/home/wangzhe/ccf/data/model/LR.uc12.0.05"),
    # lr.LR("/home/wangzhe/ccf/data/model/LR.uc12.0.08"),
    # lr.LR("/home/wangzhe/ccf/data/model/LR.us01.0.03"),
    # lr.LR("/home/wangzhe/ccf/data/model/LR.us12.0.03")
    # ]

    model = lr.LR("/home/wangzhe/ccf/data/model/LR.us01.0.03")
    lr_model = lr.LR()
    svm_model = svm.SVM()
    revere_map = {value:key for key,value in model.map.iteritems()}
    logging.info("old feature count:{0}".format(len(revere_map)))

    new_feature = set([revere_map[index] for index,weight in enumerate(model.model.weights) if abs(weight) > 0.0000001])
    logging.info("new feature count:{0}".format(len(new_feature)))
    logging.info(new_feature)
    fdata = lr_model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",True,"us01")
    def feature_filter(line):
        uid,label,features = line
        global new_feature
        new_values = {}
        for key,value in features.iteritems():
            if key in new_feature:
                new_values[key] = value

        return uid,label,new_values

    ftrain_data,ftest_data = lr_model.divide_data(fdata,0.75)
    ftrain_data_new = ftrain_data.map(feature_filter)
    ftest_data_new = ftest_data.map(feature_filter)
    balance_scales = [0.05,0.03]
    for balance_scale in balance_scales:
        # model.evaluate_fdata(ftest_data)
        # lr_model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data_new)
        # lr_model.evaluate_fdata(ftest_data_new)
        svm_model.map = {}


        svm_model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data)
        svm_model.evaluate_fdata(ftest_data)
        logging.info(len(svm_model.map))
        svm_model.map = {}
        svm_model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data_new)
        logging.info(len(svm_model.map))

        svm_model.evaluate_fdata(ftest_data_new)



    # for model in models:
    #     model = svm.SVM()
    #     feature_lists = [
    #         (["uc01","us01","uc12","us12"],0.05),
    #         (["uc01"],0.05),
    #          (["us01"],0.03),
    #           (["uc12"],0.05),
    #           (["uc12"],0.08),
    #            (["us12"],0.03),
    #         ]
    #     for feature_list,balance_scale in feature_lists:
    #             model.map= {}
    #             fdata = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",*feature_list)
    #             mdata = model.fdata_to_mdata(fdata)
    #             mtrain_data,mtest_data = model.divide_data(mdata,0.75)
    #             model.train_mdata(balance_scale=balance_scale,regType='l2',regParam=4.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,mdata=mtrain_data)
    #             model.save_model("/home/wangzhe/ccf/data/model/{0}.{1}.{2}".format(model.model_name,",".join(feature_list),balance_scale))
    #             model.evaluate_mdata(mtest_data)
    #
    #             logging.info("train model end:")
    #

