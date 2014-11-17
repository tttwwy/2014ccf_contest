# coding=utf-8
# created by WangZhe on 2014/11/2
import time
import sys
from timeit import timeit as timeit

colors = {
"black": 0,
"red": 1,
"green": 2,
"yellow": 3,
"blue": 4,
"purple": 5,
"cyan": 6,
"gray": 7
}
def mylog():
    import logging.config
    import os
    gLogger = logging.getLogger()
    # logdir = "/home/wangzhe/ccf/src"
    log_file = "/home/wangzhe/ccf/src/ccf.log"
    # os.system("mkdir -p " + logdir)
    # log_file = "./%s/%s"%(logdir,logfile)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] file:%(filename)s line:%(lineno)d func:%(funcName)s %(message)s','%Y-%m-%d %H:%M:%S')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s] file:%(filename)s line:%(lineno)d func:%(funcName)s %(message)s','%Y-%m-%d %H:%M:%S')
    handler = logging.handlers.RotatingFileHandler(log_file)
    handler.setFormatter(formatter)
    gLogger.addHandler(handler)
    gLogger.setLevel(logging.INFO)
    return gLogger

def train_log(str):
    log_file = "/home/wangzhe/ccf/src/train.log"
    with open(log_file,'a') as f:
        f.write("{0}\n".format(str))

logging = mylog()
#     logging.basicConfig(level=mylog.DEBUG,
#                     format=' %(asctime)s %(filename)s line:%(lineno)d %(funcName)s :%(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     filename='/home/wangzhe/ccf/src/log',
#                     filemode='a')
#
# # file = open("/home/wangzhe/ccf/src/log",'a')
# def info(str,color="red"):
#     # cur_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
#     # file.write("\033[0;3{0}m{1} {2}\033[0m".format(colors[color],cur_time,str))
#     mylog.info(str)



def run_time(func):

    def new_func(*args, **args2):
        start = time.time()
        logging.info("{0}:start".format(func.__name__))
        back = func(*args, **args2)
        end = time.time()
        logging.info("{0}:end".format(func.__name__))
        logging.info("{0}: {1} s".format(func.__name__,end - start))
        return back

    return new_func

if __name__ == "__main__":
    pass
