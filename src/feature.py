# coding=utf-8
# created by WangZhe on 2014/11/5

from model import logging,feature
if __name__ == "__main__":
    # feature.database = 'ccf_validation'
    # feature.work_dir = '/home/wangzhe/ccf/data/feature/validation/wz/'

    feature.database = 'ccf_train'
    feature.work_dir = '/home/wangzhe/ccf/data/feature/train/wz/'

    feature.handle('us00','''select uid,spid,1 from monitor where act = 'IMP' group by uid,spid''')
    feature.handle('us01','''select uid,spid,1 from monitor where act = 'CLK' group by uid,spid''')
    feature.handle('uc00','''select uid,caid,1 from monitor where act = 'IMP' group by uid,caid''')
    feature.handle('uc01','''select uid,caid,1 from monitor where act = 'CLK' group by uid,caid''')
    feature.handle('u15','''select uid,count(*) from monitor where act='IMP' group by uid;	''')
    feature.handle('u16','''select uid,count(*) from monitor where act='CLK' group by uid;''')

    logging.info("start")
    feature.spark_combine('u*','result.txt')
    logging.info("end")

