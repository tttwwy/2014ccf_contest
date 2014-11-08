__author__ = 'WangZhe'
# coding=utf-8
import time
from model.mylog import *

class BaseModel():



    def __init__(self,model_name = ""):

        self.init_model()
        if model_name != "":
            self.load_model(model_name)

    @run_time
    def sparse_file(self,input_name,output_name):
        cur_index = 0
        with open(input_name,'r') as f_read:
            with open(output_name,'w') as f_write:
                value_int = {}

                for line in f_read:
                    line_list = line.strip().split("\t")
                    uid = line_list[0]
                    label = line_list[1]
                    for index,item in enumerate(line_list[2:]):
                        key,value = item.split(":")
                        if key in value_int:
                            key = value_int[key]
                        else:
                            value_int[key] = str(cur_index)
                            key = str(cur_index)
                            cur_index += 1
                        line_list[index+2] = ":".join((key,value))
                    f_write.write("\t".join(line_list) + "\n")

        return output_name




    @run_time
    def submit_data(self,predict_list,save_file_name):
        index = 0
        with open(save_file_name,'w') as f:
            for uid,predict in predict_list:
                # logging.info("{0}:{1}".format(uid,predict))
                if str(predict) == '1':
                    f.write(uid + "\n")
                    index += 1

        mylog.info(index,'blue')