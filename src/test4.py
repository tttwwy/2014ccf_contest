# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import combine

from model import feature
from model.mylog import *
from model import bagging
def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

if __name__ == "__main__":
    # 模型融合实验
    logging.info("train model begin:")
    feature_lists = [
            # (["uc01","us01","uc12","us12"],0.05),
            # (["uc01"],0.05),
            #  (["us01"],0.03),
            # (["uc01","us01","uc12","us12"],20),
            # (["uc01","us01","uc12","us12"],21),
            # (["uc01","us01","uc12","us12"],19),
            # (["uc01","us01","uc12","us12"],22),
            # (["uc01"],20),
            # (["uc01"],21),
            # (["uc01"],19),
            # (["uc01"],22),
             (["us01"],33),
            (["us01"],32),
             (["us01"],31),
             (["us01"],20),
             (["us01"],25),


            # (["us12"],0.01),
            # (["us12"],0.008),
            # (["us12"],0.02),
            # (["uc12"],0.01),
            # (["uc12"],0.008),
            #
            #   (["uc12"],0.05),
            #   (["uc12"],0.08)
              ]
    #
    # model = svm.SVM()
    bagging_model = bagging.Bagging()
    feature_list = ["uc01","us01","uc12","us12"]
    fdata = bagging_model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",True,*feature_list)
    ftrain_data,ftest_data = bagging_model.divide_data(fdata,0.75)
    # for feature_list,balance_scale in feature_lists:
    #     def handle(line):
    #         uid,label,values = line
    #         new_values = {}
    #         for key in values:
    #             if key[0:4] in feature_list:
    #                 new_values[key] = values[key]
    #         return uid,label,new_values
    #
    #     new_train_data = ftrain_data.map(handle)
    #     new_test_data = ftest_data.map(handle)
    #     model.feature_names = feature_list
    #     model.train_fdata(fdata=new_train_data,balance_scale=balance_scale,regType='l2',regParam=4.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False)
    #     model.save_model("/home/wangzhe/ccf/data/model/{0}.{1}.{2}".format(model.model_name,",".join(feature_list),round(balance_scale,2)))
    #     model.evaluate_fdata(new_test_data)
    #

    model_list = []
    for feature_list,balance_scale in feature_lists:
        new_model = svm.SVM("/home/wangzhe/ccf/data/model/{0}.{1}.{2}".format("SVM",",".join(feature_list),round(balance_scale,2)))
        new_model.feature_names = feature_list

        P,R,F = new_model.evaluate_fdata(ftest_data)
        new_model.F = F
        model_list.append(new_model)

    for bagging_scale in [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.95,0.99]:
        bagging_model.train_fdata(model_list=model_list,bagging_scale = bagging_scale,balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data)
        bagging_model.evaluate_fdata(ftest_data)

