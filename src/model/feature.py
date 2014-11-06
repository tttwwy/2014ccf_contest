# coding=utf-8
# created by WangZhe on 2014/11/2
import os
import collections
from operator import add

hive_path = '/opt/apache-hive-0.13.1-bin/bin/hive'
work_dir = '/home/wangzhe/ccf/data/feature/train/wz/'
database = 'ccf_train'
def read_hive(sql):
    execute = '''{0} -e "{1}" --database {2}'''.format(hive_path,sql,database)
    print execute
    for line in os.popen(execute):
        line_list = line.strip().split("\t")
        yield line_list

def handle(feature_name,sql):
    print "{0} start".format(feature_name)
    type = len(feature_name)
    with open(work_dir + feature_name + ".txt",'w') as f:
        uid_dict = collections.defaultdict(list)
        if type == 3:
            for uid,value in read_hive(sql):
                uid_dict[uid].append((feature_name,value))
        else:
            for uid,key,value in read_hive(sql):
                uid_dict[uid].append((feature_name+key,value))

        for uid,key_value_list in uid_dict.iteritems():
            result_list = ["{0}:{1}".format(key,value) for key,value in key_value_list]
            f.write("{0}\t{1}\n".format(uid,"\t".join(result_list)))
    print "{0} end".format(feature_name)

def handle_transform(output):
    sql = "select distinct uid from transformdata"
    with open(os.path.join(work_dir,output),'w') as f:
        for uid in read_hive(sql):
            f.write("{0}\n".format(uid[0]))

def read_transform(input):
    is_transform = set()
    with open(input,'r') as f:
        for line in f:
            uid = line.strip()
            is_transform.add(uid)
    return is_transform

def feature_to_fdata():
    pass
def spark_combine(input,output):
    from pyspark import SparkContext
    print "combine start"
    def handle(x):
        line = x.split("\t")
        return line[0],line[1:]
    sc = SparkContext(appName="combine")
    data = sc.textFile(os.path.join(work_dir,input))
    result = data.map(handle).reduceByKey(lambda x,y:list(x)+list(y)).collect()
    transform_set = read_transform("/home/wangzhe/ccf/data/feature/train/wz/transform.txt")

    with open(os.path.join(work_dir,output),'w') as f:
        for uid,result_list in result:
            is_transform = '1' if uid in transform_set else '0'
            f.write("{0}\t{1}\t{2}\n".format(uid,is_transform,"\t".join(result_list)))
    print "combine end"

def combine(feature_name,output):
    transform_set = read_transform("transform.txt")
    print len(transform_set)
    uid_dict = collections.defaultdict(list)
    for name in feature_name:
        print "{0} start".format(name)
        with open(os.path.join(work_dir,name+".txt"),'r') as f:
            for line in f:
                line_list = line.strip().split("\t")
                uid = line_list[0]
                result_list = line_list[1:]
                uid_dict[uid] += result_list
        print "{0} end".format(name)


    with open(os.path.join(work_dir,output),'w') as f:
        for uid,key_value_list in uid_dict.iteritems():
            is_transform = "1" if uid in transform_set else "0"
            f.write("{0}\t{1}\t{2}\n".format(uid,is_transform,"\t".join(key_value_list)))




if __name__ == "__main__":
    # handle('uc00','''select uid,spid,1 from monitor where act = 'IMP' group by uid,spid''')
    # handle('uc01','''select uid,spid,1 from monitor where act = 'CLK' group by uid,spid''')
    # handle('us00','''select uid,caid,1 from monitor where act = 'IMP' group by uid,caid''')
    # handle('us01','''select uid,caid,1 from monitor where act = 'CLK' group by uid,caid''')
    handle_transform("transform.txt")
    combine(["uc00","uc01","us00","us01"],"result.txt")





