__author__ = 'WangZhe'
# coding=utf-8
import time
import os
from mylog import *
import subprocess
class BaseModel():



    def __init__(self,model_name = ""):

        self.init_model()
        if model_name != "":
            self.load_model(model_name)

    def submit_board(self,submit_file='submit.txt'):
        result = os.popen("simple_submit").readlines()[-1]
        result_list = result.split(" ")
        R = result_list[-1]
        P = result_list[-3]
        F1 = result_list[-5]
        return P,R,F1



    @run_time
    def submit_data(self,predict_list,save_file_name,result_scale):
        index = 0
        result_num = int(len(predict_list) * result_scale)

        logging.info(result_num)
        with open(save_file_name,'w') as f:
            for index2,(uid,label,predict) in enumerate(predict_list):
                if index2 < result_num or (result_num == 0 and predict >= 0.5):
                    index += 1
                    f.write(uid + "\n")
                    # logging.info(predict)

        logging.info(index)
        return index