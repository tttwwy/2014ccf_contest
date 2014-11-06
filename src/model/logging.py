# coding=utf-8
# created by WangZhe on 2014/11/2
import time
# logging.basicConfig(level=logging.DEBUG,
#                     format='"ccf:"%(asctime)s %(message)s',
#                     datefmt='%m-%d %H:%M:%S', )
#
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

def info(str,color="red"):
    cur_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print "\033[0;3{0}m{1} {2}\033[0m".format(colors[color],cur_time,str)


def run_time(func):
    def new_func(*args, **args2):
        start = time.clock()
        info("{0}:start".format(func.__name__))
        back = func(*args, **args2)
        end = time.clock()
        info("{0}:end".format(func.__name__))
        info("running time {0}".format(end - start))
        return back

    return new_func

if __name__ == "__main__":
    pass
