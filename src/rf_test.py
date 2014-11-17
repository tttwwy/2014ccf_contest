# coding=utf-8
# created by WangZhe on 2014/11/1

from model import svm
from model import lr
from model import rf
from model import feature
from model.mylog import *

def drange(start, stop, step):
    r = start
    while r < stop:
        yield r
        r += step

def feature_filter(x):
    features = x[2]
if __name__ == "__main__":
    logging.info("train model begin:")
    model = lr.LR()
    model2 = svm.SVM()
    fdata = model.feature_to_fdata("/home/wangzhe/ccf/data/feature/train/u16.txt")
    fdata = model2.feature_to_fdata("/home/wangzhe/ccf/data/feature/train/u16.txt")
