# coding=utf-8
# created by WangZhe on 2014/11/5

from model import feature
from model.mylog import *
import sys

if __name__ == "__main__":
    # feature.database = 'ccf_validation'
    # feature.work_dir = '/home/wangzhe/ccf/data/feature/validation/wz/'

    # feature.database = 'ccf_train'
    # feature.work_dir = '/home/wangzhe/ccf/data/feature/train/wz/'

    lists = [ ('ccf_train','/home/wangzhe/ccf/data/feature/train/wz/'),('ccf_validation','/home/wangzhe/ccf/data/feature/validation/wz/')]
    for database,work_dir in lists:
        feature.database = database
        feature.work_dir = work_dir
        logging.info("{0} {1}".format(database,work_dir))
        feature.handle('ub02','''select uid,browser,1 from monitor group by uid,browser''')
        feature.handle('uo03','''select uid,os,1 from monitor group by uid,os''')
        feature.handle('ul04','''select uid,language,1 from monitor group by uid,language''')


        continue
        # feature.handle('u06','''select distinct a.uid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP') order by a.uid''')
        feature.handle('ub02','''select uid,browser,1 from monitor group by uid,browser''')
        feature.handle('uo03','''select uid,os,1 from monitor group by uid,os''')
        feature.handle('ul04','''select uid,language,1 from monitor group by uid,language''')

        # feature.handle('u00','''select distinct uid,is_stable from monitor order by uid,is_stable''')
        # feature.handle('u02','''select distinct uid,browser from monitor order by uid,browser''')
        # feature.handle('u05','''select distinct uid,ip from monitor order by uid,ip''')
        # feature.handle('u06','''select distinct a.uid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP') order by a.uid''')
        # feature.handle('u07','''select distinct a.uid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='CLK') order by a.uid''')
        # feature.handle('u08','''select distinct a.uid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='IMP') order by a.uid''')
        # feature.handle('u09','''select distinct a.uid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='CLK') order by a.uid''')
        # feature.handle('u10','''select distinct a.uid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct max(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=a.uid and act='CLK') and a.uid=b.uid order by a.uid''')
        # feature.handle('u11','''select distinct a.uid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='CLK') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='CLK') and a.uid=b.uid order by a.uid''')
        # feature.handle('u12','''select distinct a.uid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='IMP') and a.uid=b.uid order by a.uid''')
        feature.handle('u13','''select uid,count(uid) from monitor where act = 'IMP' group by uid,substring(time,7,2)''')
        feature.handle('u14','''select uid,count(distinct substring(time,7,2)) from monitor where act = 'CLK' group by uid''')
        feature.handle('u15','''select uid,count(distinct substring(time,7,2)) from monitor where act = 'IMP' group by uid''')
        feature.handle('u16','''select uid,count(uid) from monitor where act='CLK' group by uid''')
        feature.handle('u17','''select t1.uid,buid*1.0/auid from (select uid,count(*) as auid from monitor where act='IMP' group by uid) t1,(select uid,count(*) as buid from monitor where act='CLK' group by uid) t2 where t1.uid=t2.uid''')
        feature.handle('u18','''select uid,count(caid) from monitor where act='IMP' group by uid order by uid asc''')
        feature.handle('u19','''select uid,count(caid) from monitor where act='CLK' group by uid order by uid asc''')
        feature.handle('u20','''select uid,count(spid) from monitor where act='IMP' group by uid order by uid asc''')


        feature.handle('uc00','''select uid,caid,1 from monitor where act = 'IMP' group by uid,caid''')
        feature.handle('uc01','''select uid,caid,1 from monitor where act = 'CLK' group by uid,caid''')
        # feature.handle('uc02','''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP' and caid=a.caid) order by a.uid,a.caid''')
        # feature.handle('uc03','''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='CLK' and caid=a.caid) order by a.uid,a.caid''')
        # feature.handle('uc04','''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='IMP' and caid=a.caid) order by a.uid,a.caid''')
        # feature.handle('uc05','''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='CLK' and caid=a.caid) order by a.uid,a.caid''')
        # feature.handle('uc06','''select distinct a.uid,a.caid,day(b.time)-day(a.time) from monitor a,monitor b where b.time= (select distinct max(time) from monitor where uid=b.uid and act='CLK') and a.time = (select distinct max(time) from monitor where uid=a.uid and act='IMP' and time < (select distinct max(time) from monitor where uid=b.uid and act='CLK')) and a.uid=b.uid and a.caid=b.caid order by a.uid,a.caid''')
        # feature.handle('uc07','''select distinct a.uid,a.caid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='IMP') and a.uid=b.uid and a.caid=b.caid order by a.uid,a.caid''')
        feature.handle('uc08','''select uid,caid,count(distinct substring(time,7,2)) from monitor where act = 'IMP' group by uid,caid''')
        feature.handle('uc09','''select uid,caid,count(distinct substring(time,7,2)) from monitor where act = 'CLK' group by uid,caid''')
        feature.handle('uc10','''select uid,caid,count(caid) from monitor where act = 'IMP' group by uid,caid''')
        feature.handle('uc11','''select uid,caid,count(caid) from monitor where act = 'CLK' group by uid,caid''')
        feature.handle('uc12','''select t1.uid,t1.caid,buid*1.0/auid from (select uid,caid,count(*) as auid from monitor where act = 'IMP' group by uid,caid order by uid) t1,(select uid,caid,count(*) as buid from monitor where act = 'CLK' group by uid,caid order by uid) t2 where t1.uid=t2.uid and t1.caid=t2.caid''')


        feature.handle('us00','''select uid,spid,1 from monitor where act = 'IMP' group by uid,spid''')
        feature.handle('us01','''select uid,spid,1 from monitor where act = 'CLK' group by uid,spid''')
        # feature.handle('us02','''select distinct a.uid,a.spid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP' and spid=a.spid) order by a.uid,a.spid''')
        # feature.handle('us03','''select distinct a.uid,a.spid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='CLK' and spid=a.spid) order by a.uid,a.spid''')
        # feature.handle('us04','''select distinct a.uid,a.spid,a.time from monitor as a where time =(select distinct max(time) from monitor where uid=a.uid and act='IMP' and spid=a.spid) order by a.uid,a.spid''')
        # feature.handle('us05','''select distinct a.uid,a.spid,a.time from monitor as a where time =(select distinct max(time) from monitor where uid=a.uid and act='CLK' and spid=a.spid) order by a.uid,a.spid''')
        # feature.handle('us06','''select distinct a.uid,a.spid,day(b.time)-day(a.time) from monitor a,monitor b where b.time= (select distinct max(time) from monitor where uid=b.uid and act='CLK') and a.time = (select distinct max(time) from monitor where uid=a.uid and act='IMP' and time < (select distinct max(time) from monitor where uid=b.uid and act='CLK')) and a.uid=b.uid and a.spid=b.spid order by a.uid,a.spid''')
        # feature.handle('us07','''select distinct a.uid,a.spid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='IMP') and a.uid=b.uid and a.spid=b.spid order by a.uid,a.spid''')
        feature.handle('us08','''select uid,spid,count(distinct substring(time,7,2)) from monitor where act = 'IMP' group by uid,spid''')
        feature.handle('us09','''select uid,spid,count(distinct substring(time,7,2)) from monitor where act = 'CLK' group by uid,spid''')
        feature.handle('us10','''select uid,spid,count(spid) from monitor where act = 'IMP' group by uid,spid''')
        feature.handle('us11','''select uid,spid,count(spid) from monitor where act = 'CLK' group by uid,spid''')
        feature.handle('us12','''select t1.uid,t1.spid,buid*1.0/auid from (select uid,spid,count(*) as auid from monitor where act = 'IMP' group by uid,spid order by uid) t1,(select uid,spid,count(*) as buid from monitor where act = 'CLK' group by uid,spid order by uid) t2 where t1.uid=t2.uid and t1.spid=t2.spid;''')
