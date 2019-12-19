#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File: http_put.py

#python inc_build_new_v4.py  iqj_user_inc_v2 None
#调用程序
#参数1 cubename
#参数2 inc/full
#参数3 日期变量 yyyt-mm-dd 默认昨天

#1.1增加增量对最大分区判断 如果有则refresh 否则 build
#1.2增加读取配置文件
import os
from datetime import datetime, timedelta
import sys
import time
import urllib
import json
import requests
import configparser
import kylin_api

print(sys.getdefaultencoding())
cf= configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值



cubename = sys.argv[1]
buildtype = sys.argv[2]
runTime = sys.argv[3] + " 08:00:00"
# cubename = 'datawindow_cube_wide_inc2s'
runTime = [10000]
def date_get(datetime=datetime):
    now = datetime.now()
    data_date = now.strftime('%Y-%m-%d')
    return data_date



if sys.argv[3] == 'None':
    # runTime = '2019-03-15'
    v_date = date_get()
    runTime = date_get() + " 08:00:00"
else:
    v_date = sys.argv[3]
    runTime = sys.argv[3] + " 08:00:00"

print('runTime:'+str(runTime))

delta1 = (datetime.strptime(runTime, "%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
print('delta1:' + delta1 + '\n')

starttime = int(time.mktime(datetime.strptime(delta1, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
print('starttime:' + str(starttime) + '\n')

endtime = int(time.mktime(datetime.strptime(runTime, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
print('endtime:' + str(endtime) + '\n')





##增量、全量构建
if buildtype == 'full':
   print('全量构建cube:')
   resp, resp_code = kylin_api.http_put_full(v_url,cubename)
else:

   print('增量构建cube:')
   ##run job
   seg_list=[]
   req, ret_code = kylin_api.http_get_cube(v_url,cubename)
   #req=(str(req.text))
   load_dict = json.loads(req)   
   for segment in load_dict['segments']:
       print (segment['name'])
       seg_list.append(segment['name'])

   v_refreshtype = 'BUILD'
   ##judge wether exists segment
   if len(seg_list):
       max_index = seg_list.index((max(seg_list)))
       print(max_index)
       max_dt = seg_list[max_index]
       maxdt = max_dt.split('_')
       maxdt = (','.join(maxdt[1:])[:8])
       print('判断参数:')
       print(time.strftime('%Y-%m-%d', time.strptime(maxdt, '%Y%m%d')))
       ##judge wether the max dt have exists
       if time.strftime('%Y-%m-%d', time.strptime(maxdt, '%Y%m%d')) == v_date:
           print('refresh cube ')
           v_refreshtype = 'REFRESH'

   print(v_refreshtype + '  CUBE')
   resp, resp_code = kylin_api.http_put_inc(v_url, cubename, starttime, endtime, v_refreshtype)

resp=str(resp)
print (resp)


if resp_code == 200:
    print('cube' + cubename + '正在生成...')
else:
    print('异常代码:' + str(resp_code))
    exit

print ('获取jobid及job状态:')
print('~~~~~~~~~~~~')
load_dict = json.loads(resp)
print(load_dict.keys())
##get  jobid
job_id = load_dict['uuid']
job_status = load_dict['job_status']
print('job_id :' + str(job_id))

print ('等待job结果:')
##若没有结束一直循环 10秒监测一次

berror = True
error_cnt = 0
while (berror == True and error_cnt <= 3):
    try:
        ret =''
        ret_code =''
        ret, ret_code = kylin_api.http_get(v_url,job_id)
        print('~~~~~~~~~~~~')
        print ('ret_code:' + str(ret_code))

        load_dict = json.loads(ret)

        #print(load_dict.keys())
        progress=load_dict['progress']
        print('progress :' + str(progress))

        job_status = load_dict['job_status']
        if job_status == 'RUNNING':
            time.sleep(10)
            berror = True
            continue
        elif job_status == 'FINISHED':
            print('the job is SUCCESS')
            berror = False
            break
        elif job_status == 'ERROR':
            print('the job is ERROR')
            # raise RuntimeError('testError')
            berror = False
            sys.exit(1)
            break
        time.sleep(10)  ##推迟执行、休眠
    except Exception as e:
        print ('e.message:', e.message)
        time.sleep(15)
        berror = True
        error_cnt = error_cnt + 1
        pass

print ('结束')
