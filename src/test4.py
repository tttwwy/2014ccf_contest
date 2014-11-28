# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import combine

from model import feature
from model.mylog import *
from model import bagging
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector
def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

if __name__ == "__main__":


    logging.info("train model begin:")
    model = lr.LR()

    # ffilter_data = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/train/us01.txt")
    # ffilter_data = ffilter_data.filter(lambda x:x[1] == '1')
    # spids = set(ffilter_data.flatMap(lambda x:[item[4:] for item in x[2].keys()]).distinct().collect())
    # broadcast_spids = model.get_sc().broadcast(spids)
    # def clean(line):
    #     uid,label,values = line
    #     new_values = {}
    #     for key,value in values.iteritems():
    #         if key[4:] in broadcast_spids.value:
    #             new_values[key] = value
    #     return uid,label,new_values

    feature_lists = [
        ['us01' ]
        ]
    for feature_list in feature_lists:
        # fdata = model.features_to_fdata("/home/wangzhe/ccf/data/feature/train/","us01")
        # data = model.get_sc().textFile("/home/wangzhe/ccf/data/feature/train/spid.txt")
        data = model.file_to_data("/home/wangzhe/ccf/data/feature/train/spid.txt")
        def handle(line):
            uid = '1'
            label,values = line.strip().split("\t")
            values = values.split(" ")
            value_map = {}
            for item in values:
                key,value = item.split(":")
                value_map[int(key)] = float(value)
                return (uid,LabeledPoint(label,SparseVector(30000,value_map)))
        mdata = data.map(handle)

        logging.info(mdata.take(10))

        ftrain_data,ftest_data = model.divide_data(fdata,0.75)
        # for uid,label,values in ftrain_data.collect():
        #     logging.info("{0} {1} {2}".format(uid,label,values))

        # logging.info(ftrain_data.collect())
        # logging.info(ftest_data.collect())
        # fdata_buy = ftrain_data.filter(lambda x:x[1] == '1')
        # logging.info(fdata_buy.collect())
        # fdata_nobuy = ftrain_data.filter(lambda x:x[1] == '0')
        # logging.info(fdata_nobuy.collect())
        # count_buy = fdata_buy.count()
        # count_nobuy = fdata_nobuy.count()
        # fdata_nobuy = fdata_nobuy.sample(False,count_buy/count_nobuy*500,234)
        # # fdata_nobuy = fdata_nobuy.map(clean)
        # logging.info("filter count:{0}".format(fdata_nobuy.count()))
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


        balance_scales = [0.08,400,500]

        for balance_scale in balance_scales:
            # model.map = {}
            # ftrain_data = model.balance(fdata_buy,fdata_nobuy,balance_scale)
            # logging.info(ftrain_data.collect())
            mtrain_data = model.fdata_to_mdata(ftrain_data)
            # logging.info(model.map)
            # logging.info(mtrain_data)
            model.train_args['scale'] = balance_scale
            # model.train_args['min_click'] = min_click


            # model.train_fdata(balance_scale=balance_scale,regType='l2',regParam=1.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False,fdata=ftrain_data)

            model.train_mdata(mdata=mtrain_data,regType='l1',regParam=10.0,iterations=100,step = 0.5,miniBatchFraction=1.0,intercept=False)

            # model.save_model("/home/wangzhe/ccf/data/model/lr")
            # for result_scale in [0,0.0027,0.003,0.0033]:
            for result_scale in [0,0.0005,0.006,0.007,0.003,0.004,0.005,0.006,0.007,0.008,0.009,0.01]:
                model.evaluate_fdata(ftest_data,result_scale)
                # model.load_model("/home/wangzhe/ccf/data/model/lr")
                # model.submit_fdata(ftest_data,"submit.txt",result_scale)
                # time.sleep(50)
                logging.info("train model end:")
