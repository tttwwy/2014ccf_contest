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
    # 获取待融合模型
    models = [svm.SVM(),lr.LR()]
    for model in models:
        model = svm.SVM()
        feature_lists = [
            (["uc01","us01","uc12","us12"],0.05),
            (["uc01"],0.05),
             (["us01"],0.03),
              (["uc12"],0.05),
              (["uc12"],0.08),
            #    (["us12"],0.03),
            #  (["uc01","us01","uc12","us12"],20),
            # (["uc01"],20),
            #  (["us01"],1.0/0.03),
            #   (["uc12"],1.0/0.05),
            #   (["uc12"],1.0/0.08),
            #    (["us12"],1.0/0.03),

            ]
        for feature_list,balance_scale in feature_lists:
                fdata = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",True,*feature_list)
                mdata = model.fdata_to_mdata(fdata)
                mtrain_data,mtest_data = model.divide_data(mdata,0.75)
                model.train_mdata(balance_scale=balance_scale,regType='l2',regParam=4.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,mdata=mtrain_data)
                model.save_model("/home/wangzhe/ccf/data/model/{0}.{1}.{2}".format(model.model_name,",".join(feature_list),round(balance_scale,2)))
                model.evaluate_mdata(mtest_data)

                logging.info("train model end:")


