# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import combine

from model import feature
from model.mylog import *
from model import bagging
import time
if __name__ == "__main__":

    logging.info("train model begin:")
    # model = lr.LR()
    model = svm.SVM()

    ffilter_data = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/train/us01.txt")
    ffilter_data = ffilter_data.filter(lambda x:x[1] == '1')
    spids = set(ffilter_data.flatMap(lambda x:[item[4:] for item in x[2].keys()]).distinct().collect())
    broadcast_spids = model.get_sc().broadcast(spids)
    def clean(line):
        uid,label,values = line
        new_values = {}
        for key,value in values.iteritems():
            if key[4:] in broadcast_spids.value:
                new_values[key] = value
        return uid,label,new_values

    feature_lists = [
        ['us01']
        ]
    for feature_list in feature_lists:
        ftrain_data = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/",*feature_list)
        ftest_data = model.features_to_fdata("/home/wangzhe/ccf/data/feature/validation/",*feature_list)

        fdata_buy = ftrain_data.filter(lambda x:x[1] == '1')
        fdata_nobuy = ftrain_data.filter(lambda x:x[1] == '0')
        fdata_nobuy = fdata_nobuy.map(clean)
        logging.info("filter count:{0}".format(fdata_nobuy.count()))
        # fdata_nobuy = fdata_nobuy.filter(lambda x:len(x[2]) > 0)
        # logging.info("filter count:{0}".format(fdata_nobuy.count()))

        # ftest_data = ftest_data.map(clean)
        # logging.info("filter count:{0}".format(ftest_data.count()))
        # ftest_data = ftest_data.filter(lambda x:len(x[2]) > 0)
        # logging.info("filter count:{0}".format(ftest_data.count()))

        # fdata_nobuy = fdata_nobuy.filter(lambda x:x[2].get('u16',0) > min_click)
        # fdata_nobuy = fdata_nobuy.map(clear)
        # fdata_buy = fdata_buy.map(clear)

        # ftest_data = ftest_data.filter(lambda x:x[2].get('u16',0) > min_click)


        balance_scales = [0.08]
        for balance_scale in balance_scales:
            model.map = {}
            ftrain_data = model.balance(fdata_buy,fdata_nobuy,balance_scale)
            mtrain_data = model.fdata_to_mdata(ftrain_data)
            model.train_args['scale'] = balance_scale
            # model.train_args['min_click'] = min_click


            # model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data)

            model.train_mdata(mdata=mtrain_data,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False)
            # model.save_model("/home/wangzhe/ccf/data/model/lr")
            # for result_scale in [0,0.0027,0.003,0.0033]:
            # cur_time = time.time()-60
            for result_scale in [0.0057]:
                model.evaluate_fdata(ftest_data,result_scale)
                # model.load_model("/home/wangzhe/ccf/data/model/lr")
                # dis_time = time.time() - cur_time
                # logging.info(dis_time)
                # if dis_time < 40:
                #     time.sleep(40 - dis_time)
                # model.submit_fdata(ftest_data,"submit.txt",result_scale)
                # cur_time = time.time()

                logging.info("train model end:")
