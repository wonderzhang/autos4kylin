# #!/usr/bin/env python
# # -*- coding:utf-8 -*-
# # File: http_put.py
# import os
# from datetime import datetime,timedelta
# import sys
# import time
# import urllib2
# import json
#
#
# cubename =  sys.argv[1]
# runTime = sys.argv[2]+ " 08:00:00"
# #cubename = 'datawindow_cube_wide_inc2s'
# runTime=[10000]
#
#
# def date_get(datetime=datetime):
#     now = datetime.now()
#     data_date = now.strftime('%Y-%m-%d')
#     return data_date
#
#
# def http_put(v_cube,v_startTime,v_endTime):
#     #url='http://analysi4olap.finupgroup.com/kylin/api/cubes/'+v_cube+'/rebuild'
#
#     url='http://192.168.176.67:7070/kylin/api/cubes/'+v_cube+'/rebuild'
#     values={"startTime":v_startTime, "endTime":v_endTime, "buildType":"BUILD"}
#     jdata = json.dumps(values)                  # 对数据进行JSON格式化编码
#     request = urllib2.Request(url, jdata)
#     request.add_header('Content-Type', 'application/json;charset=utf-8')
#     request.add_header('Authorization', 'Basic YWRtaW46S1lMSU4=')
#     request.get_method = lambda:'PUT'           # 设置HTTP的访问方式
#     request = urllib2.urlopen(request)
#     return request.read(),request.getcode()
#
# def http_get(v_job_id):
#     #url='http://analysi4olap.finupgroup.com/kylin/api/jobs/'+v_job_id
#     url='http://192.168.176.67:7070/kylin/api/jobs/'+v_job_id
#     request = urllib2.Request(url,'')
#     request.add_header('Content-Type', 'application/json;charset=utf-8')
#     request.add_header('Authorization', 'Basic YWRtaW46S1lMSU4=')
#     request.get_method = lambda:'GET'           # 设置HTTP的访问方式
#     request = urllib2.urlopen(request)
#     return request.read(),request.getcode()
#
#
# if sys.argv[2] == 'None':
#    #runTime = '2019-03-15'
#    runTime = date_get()+ " 08:00:00"
# else:
#    runTime=sys.argv[2]+ " 08:00:00"
#
# delta1 = (datetime.strptime(runTime, "%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
# print('delta1:'+delta1+'\n' )
#
#
#
# starttime = int(time.mktime(datetime.strptime(delta1, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
# print('starttime:'+str(starttime)+'\n' )
#
# endtime = int(time.mktime(datetime.strptime(runTime, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
# print('endtime:'+str(endtime)+'\n' )
#
# ##run job
# resp,resp_code = http_put(cubename,starttime,endtime)
# if resp_code==200:
#    print('cube'+cubename+'正在生成...')
# else:
#    print('异常代码:'+string(resp_code))
#    exit
#
# print ('获取jobid及job状态:')
# load_dict = json.loads(resp)
# print(load_dict.keys())
# ##get  jobid
# job_id = load_dict['uuid']
# job_status = load_dict['job_status']
# print('job_id :'+str(job_id))
#
#
# print ('等待job结果:')
# ##若没有结束一直循环 10秒监测一次
#
# berror = True
# error_cnt =0
# while (berror == True and error_cnt<=3 ):
#     try:
#         ret,ret_code = http_get(job_id)
#         print ret
#         print 'ret_code:' +str(ret_code)
#
#         load_dict = json.loads(ret)
#
#         print(load_dict.keys())
#         job_status = load_dict['job_status']
#         if   job_status =='RUNNING':
#                   time.sleep(10)
#                   berror = True
#                   continue
#         elif job_status =='FINISHED':
#                   print('the job is SUCCESS')
#                   berror = False
#                   break
#         elif job_status =='ERROR':
#                   print('the job is ERROR')
#                   #raise RuntimeError('testError')
#                   berror = False
#                   break
#         time.sleep(10)##推迟执行、休眠
#     except Exception ,e:
#         print ('e.message:', e.message)
#         time.sleep(15)
#         berror = True
#         error_cnt =error_cnt+1
#         pass
#
#
#
#
#
#
#
#
#
#
# print ('结束')
