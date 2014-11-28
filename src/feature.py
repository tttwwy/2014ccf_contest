 v# coding=utf-8
# created by WangZhe on 2014/11/5

from model import feature
from model.mylog import *
import sys

if __name__ == "__main__":
    # feature.database = 'ccf_validation'
    # feature.work_dir = '/home/wangzhe/ccf/data/feature/validation/wz/'

    # feature.database = 'ccf_train'
    # feature.work_dir = '/home/wangzhe/ccf/data/feature/train/wz/'
    lists = [ ('ccf_test','/home/wangzhe/ccf/data/feature/test/')]

    # lists = [ ('ccf_train','/home/wangzhe/ccf/data/feature/train/wz'),('ccf_validation','/home/wangzhe/ccf/data/feature/validation/wz')]
    times = ["",
             "and cast(substring(time,9,2) as int) >= 0 and cast(substring(time,9,2) as int) < 6",
             "and cast(substring(time,9,2) as int) >= 6 and cast(substring(time,9,2) as int) < 11",
             "and cast(substring(time,9,2) as int) >= 11 and cast(substring(time,9,2) as int) < 15",
             "and cast(substring(time,9,2) as int) >= 15 and cast(substring(time,9,2) as int) < 18",
             "and cast(substring(time,9,2) as int) >= 18 and cast(substring(time,9,2) as int) < 24",
             ]
    for database,work_dir in lists:
        feature.database = database
        feature.work_dir = work_dir
        logging.info("{0} {1}".format(database,work_dir))
        feature.handle('ub02','''select uid,browser,1 from monitor group by uid,browser''')
        feature.handle('uo03','''select uid,os,1 from monitor group by uid,os''')
        feature.handle('ul04','''select uid,language,1 from monitor group by uid,language''')

        for index,time in enumerate(times):
            if index == 0:
                flag = ""
            else:
                flag = str(index)
            # continue
            # feature.handle('{0}u06'.format(flag),'''select distinct a.uid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP') order by a.uid'''.format(time))
            # feature.handle('{0}ub02'.format(flag),'''select uid,browser,1 from monitor group by uid,browser'''.format(time))
            # feature.handle('{0}uo03'.format(flag),'''select uid,os,1 from monitor group by uid,os'''.format(time))
            # feature.handle('{0}ul04'.format(flag),'''select uid,language,1 from monitor group by uid,language'''.format(time))
    
            # feature.handle('{0}u00'.format(flag),'''select distinct uid,is_stable from monitor order by uid,is_stable'''.format(time))
            # feature.handle('{0}u02'.format(flag),'''select distinct uid,browser from monitor order by uid,browser'''.format(time))
            # feature.handle('{0}u05'.format(flag),'''select distinct uid,ip from monitor order by uid,ip'''.format(time))
            # feature.handle('{0}u06'.format(flag),'''select distinct a.uid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP') order by a.uid'''.format(time))
            # feature.handle('{0}u07'.format(flag),'''select distinct a.uid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='CLK') order by a.uid'''.format(time))
            # feature.handle('{0}u08'.format(flag),'''select distinct a.uid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='IMP') order by a.uid'''.format(time))
            # feature.handle('{0}u09'.format(flag),'''select distinct a.uid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='CLK') order by a.uid'''.format(time))
            # feature.handle('{0}u10'.format(flag),'''select distinct a.uid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct max(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=a.uid and act='CLK') and a.uid=b.uid order by a.uid'''.format(time))
            # feature.handle('{0}u11'.format(flag),'''select distinct a.uid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='CLK') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='CLK') and a.uid=b.uid order by a.uid'''.format(time))
            # feature.handle('{0}u12'.format(flag),'''select distinct a.uid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='IMP') and a.uid=b.uid order by a.uid'''.format(time))
            feature.handle('{0}u13'.format(flag),'''select uid,count(uid) from monitor where act = 'IMP' {0} group by uid,substring(time,7,2)'''.format(time))
            feature.handle('{0}u14'.format(flag),'''select uid,count(distinct substring(time,7,2)) from monitor where act = 'CLK' {0} group by uid'''.format(time))
            feature.handle('{0}u15'.format(flag),'''select uid,count(distinct substring(time,7,2)) from monitor where act = 'IMP' {0} group by uid'''.format(time))
            feature.handle('{0}u16'.format(flag),'''select uid,count(uid) from monitor where act='CLK' {0} group by uid'''.format(time))
            feature.handle('{0}u17'.format(flag),'''select t1.uid,buid*1.0/auid from (select uid,count(*) as auid from monitor where act='IMP' {0} group by uid) t1,(select uid,count(*) as buid from monitor where act='CLK' group by uid) t2 where t1.uid=t2.uid '''.format(time))
            feature.handle('{0}u18'.format(flag),'''select uid,count(caid) from monitor where act='IMP' {0} group by uid order by uid asc'''.format(time))
            feature.handle('{0}u19'.format(flag),'''select uid,count(caid) from monitor where act='CLK' {0} group by uid order by uid asc'''.format(time))
            feature.handle('{0}u20'.format(flag),'''select uid,count(spid) from monitor where act='IMP' {0} group by uid order by uid asc'''.format(time))
    
    
            feature.handle('{0}uc00'.format(flag),'''select uid,caid,1 from monitor where act = 'IMP' {0} group by uid,caid'''.format(time))
            feature.handle('{0}uc01'.format(flag),'''select uid,caid,1 from monitor where act = 'CLK' {0} group by uid,caid'''.format(time))
            # feature.handle('{0}uc02'.format(flag),'''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP' and caid=a.caid) order by a.uid,a.caid'''.format(time))
            # feature.handle('{0}uc03'.format(flag),'''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='CLK' and caid=a.caid) order by a.uid,a.caid'''.format(time))
            # feature.handle('{0}uc04'.format(flag),'''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='IMP' and caid=a.caid) order by a.uid,a.caid'''.format(time))
            # feature.handle('{0}uc05'.format(flag),'''select distinct a.uid,a.caid,a.time from monitor as a where time = (select distinct max(time) from monitor where uid=a.uid and act='CLK' and caid=a.caid) order by a.uid,a.caid'''.format(time))
            # feature.handle('{0}uc06'.format(flag),'''select distinct a.uid,a.caid,day(b.time)-day(a.time) from monitor a,monitor b where b.time= (select distinct max(time) from monitor where uid=b.uid and act='CLK') and a.time = (select distinct max(time) from monitor where uid=a.uid and act='IMP' and time < (select distinct max(time) from monitor where uid=b.uid and act='CLK')) and a.uid=b.uid and a.caid=b.caid order by a.uid,a.caid'''.format(time))
            # feature.handle('{0}uc07'.format(flag),'''select distinct a.uid,a.caid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='IMP') and a.uid=b.uid and a.caid=b.caid order by a.uid,a.caid'''.format(time))
            feature.handle('{0}uc08'.format(flag),'''select uid,caid,count(distinct substring(time,7,2)) from monitor where act = 'IMP' {0} group by uid,caid'''.format(time))
            feature.handle('{0}uc09'.format(flag),'''select uid,caid,count(distinct substring(time,7,2)) from monitor where act = 'CLK' {0} group by uid,caid'''.format(time))
            feature.handle('{0}uc10'.format(flag),'''select uid,caid,count(caid) from monitor where act = 'IMP' {0} group by uid,caid'''.format(time))
            feature.handle('{0}uc11'.format(flag),'''select uid,caid,count(caid) from monitor where act = 'CLK' {0} group by uid,caid'''.format(time))
            feature.handle('{0}uc12'.format(flag),'''select t1.uid,t1.caid,buid*1.0/auid from (select uid,caid,count(*) as auid from monitor where act = 'IMP' {0} group by uid,caid order by uid) t1,(select uid,caid,count(*) as buid from monitor where act = 'CLK' group by uid,caid order by uid) t2 where t1.uid=t2.uid and t1.caid=t2.caid '''.format(time))
    
    
            feature.handle('{0}us00'.format(flag),'''select uid,spid,1 from monitor where act = 'IMP' {0} group by uid,spid'''.format(time))
            feature.handle('{0}us01'.format(flag),'''select uid,spid,1 from monitor where act = 'CLK' {0} group by uid,spid'''.format(time))
            # feature.handle('{0}us02'.format(flag),'''select distinct a.uid,a.spid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='IMP' and spid=a.spid) order by a.uid,a.spid'''.format(time))
            # feature.handle('{0}us03'.format(flag),'''select distinct a.uid,a.spid,a.time from monitor as a where time = (select distinct min(time) from monitor where uid=a.uid and act='CLK' and spid=a.spid) order by a.uid,a.spid'''.format(time))
            # feature.handle('{0}us04'.format(flag),'''select distinct a.uid,a.spid,a.time from monitor as a where time =(select distinct max(time) from monitor where uid=a.uid and act='IMP' and spid=a.spid) order by a.uid,a.spid'''.format(time))
            # feature.handle('{0}us05'.format(flag),'''select distinct a.uid,a.spid,a.time from monitor as a where time =(select distinct max(time) from monitor where uid=a.uid and act='CLK' and spid=a.spid) order by a.uid,a.spid'''.format(time))
            # feature.handle('{0}us06'.format(flag),'''select distinct a.uid,a.spid,day(b.time)-day(a.time) from monitor a,monitor b where b.time= (select distinct max(time) from monitor where uid=b.uid and act='CLK') and a.time = (select distinct max(time) from monitor where uid=a.uid and act='IMP' and time < (select distinct max(time) from monitor where uid=b.uid and act='CLK')) and a.uid=b.uid and a.spid=b.spid order by a.uid,a.spid'''.format(time))
            # feature.handle('{0}us07'.format(flag),'''select distinct a.uid,a.spid,day(b.time)-day(a.time) from monitor a,monitor b where a.time =(select distinct min(time) from monitor where uid=a.uid and act='IMP') and b.time= (select distinct max(time) from monitor where uid=b.uid and act='IMP') and a.uid=b.uid and a.spid=b.spid order by a.uid,a.spid'''.format(time))
            feature.handle('{0}us08'.format(flag),'''select uid,spid,count(distinct substring(time,7,2)) from monitor where act = 'IMP' {0} group by uid,spid'''.format(time))
            feature.handle('{0}us09'.format(flag),'''select uid,spid,count(distinct substring(time,7,2)) from monitor where act = 'CLK' {0} group by uid,spid'''.format(time))
            feature.handle('{0}us10'.format(flag),'''select uid,spid,count(spid) from monitor where act = 'IMP' {0} group by uid,spid'''.format(time))
            feature.handle('{0}us11'.format(flag),'''select uid,spid,count(spid) from monitor where act = 'CLK' {0} group by uid,spid'''.format(time))
            feature.handle('{0}us12'.format(flag),'''select t1.uid,t1.spid,buid*1.0/auid from (select uid,spid,count(*) as auid from monitor where act = 'IMP' {0} group by uid,spid order by uid) t1,(select uid,spid,count(*) as buid from monitor where act = 'CLK' group by uid,spid order by uid) t2 where t1.uid=t2.uid and t1.spid=t2.spid;'''.format(time))
