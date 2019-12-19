#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File: http_put.py

#python inc_build_new_v4.py  iqj_user_inc_v2 None
#调用程序
#参数1 cubename
#参数2 inc/full
#参数3 日期变量 yyyt-mm-dd 默认昨天

#1.1增加增量对最大分区判断 如果有则refresh 否则 build
import os
from datetime import datetime, timedelta
import sys
import time
import urllib
import json
import requests
import configparser

print(sys.getdefaultencoding())
cf= configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)

option = cf.options("kylin-pwd")

v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值

print(v_pwd)
print(v_url)
exit()

cubename = sys.argv[1]
buildtype = sys.argv[2]
runTime = sys.argv[3] + " 08:00:00"
# cubename = 'datawindow_cube_wide_inc2s'
runTime = [10000]



v_url = 'http://192.168.176.67:7070'


def date_get(datetime=datetime):
    now = datetime.now()
    data_date = now.strftime('%Y-%m-%d')
    return data_date


# def http_put(v_cube, v_startTime, v_endTime):
#     # url='http://analysi4olap.finupgroup.com/kylin/api/cubes/'+v_cube+'/rebuild'
#     url = 'http://192.168.176.67:7070'
#     url = url + '/kylin/api/cubes/' + v_cube + '/rebuild'
#     values = {"startTime": v_startTime, "endTime": v_endTime, "buildType": "BUILD"}
#     jdata = json.dumps(values)  # 对数据进行JSON格式化编码
#     #req = urllibrequest.Request(url, jdata=urllib.parse.urlencode(jdata).encode(encoding='utf-8'))
#     #req = urllibrequest.Request(url,jdata=urllib.parse.quote(jdata).encode(encoding='utf-8'))
#     req = urllibrequest.Request(url,jdata=urllib.parse.quote(jdata,encoding='utf-8'))

#     #request = urllib2.Request(url, data, headers)  
#     # request = urllib.request.Request(url, data=urllib.parse.urlencode(data).encode(encoding='UTF8'), headers=headers) 


#     req.add_header('Content-Type', 'application/json;charset=utf-8')
#     req.add_header('Authorization', 'Basic YWRtaW46S1lMSU4=')
#     req.get_method = lambda: 'PUT'  # 设置HTTP的访问方式
#     req = urllib.request(req)
#     return req.read(), req.getcode()



def http_put_inc(v_url,v_cube, v_startTime, v_endTime,v_refreshtype):
    #url = 'http://192.168.176.67:7070'
    url=v_url+'/kylin/api/cubes/'+v_cube+'/rebuild'
    values = {"startTime": v_startTime, "endTime": v_endTime, "buildType": v_refreshtype}
    jdata = json.dumps(values)                  # 对数据进行JSON格式化编码
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.put(url,data=jdata,headers=header  )
    return req.text, req.status_code

def http_put_full(v_url,v_cube):
    #url = 'http://192.168.176.67:7070'
    url=v_url+'/kylin/api/cubes/'+v_cube+'/rebuild'
    values = {"buildType": "BUILD"}
    jdata = json.dumps(values)                  # 对数据进行JSON格式化编码

    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.put(url,data=jdata,headers=header  )
    return req.text, req.status_code

def http_get_cube(v_url,v_cube):
    #url = 'http://192.168.176.67:7070'
    url = v_url +'/kylin/api/cubes/'+v_cube
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.get(url, headers=header  )
    return req.text, req.status_code


def http_get(v_url,v_job_id):
    #url = 'http://192.168.176.67:7070'
    url=v_url+'/kylin/api/jobs/'+v_job_id
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.get(url, headers=header  )
    return req.text, req.status_code



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
   resp, resp_code = http_put_full(v_url,cubename)
else:
   print('增量构建cube:')
   ##run job
   seg_list=[]
   req, ret_code = http_get_cube(v_url,cubename)
   #req=(str(req.text))
   load_dict = json.loads(req)   
   for segment in load_dict['segments']:
       print (segment['name'])
       seg_list.append(segment['name'])
   max_index= seg_list.index((max(seg_list)))
   max_dt=seg_list[max_index]
   maxdt= max_dt.split('_')
   maxdt=(','.join(maxdt[1:])[:8])   
   print('判断参数:')
   print(time.strftime('%Y-%m-%d', time.strptime(maxdt, '%Y%m%d')))
   ##judge wether the max dt have exists
   if time.strftime('%Y-%m-%d', time.strptime(maxdt, '%Y%m%d'))==v_date:
      print('refresh cube ')
      v_refreshtype='REFRESH'
   else:
      print('build  cube ')
      v_refreshtype='BUILD'
   resp, resp_code = http_put_inc(v_url,cubename, starttime, endtime,v_refreshtype)


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
        ret, ret_code = http_get(v_url,job_id)
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
            break
        time.sleep(10)  ##推迟执行、休眠
    except Exception as e:
        print ('e.message:', e.message)
        time.sleep(15)
        berror = True
        error_cnt = error_cnt + 1
        pass

print ('结束')
