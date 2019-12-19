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
import pandas as pd
from   sqlalchemy import create_engine
import kylin_api
import py_sqlparse


uae_name='admin'
uae_passwd='KYLIN'
import base64
print(sys.getdefaultencoding())
cf= configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')

#cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值


resp, resp_code,cook=kylin_api.http_get_cube_sql(v_url,'iqj_invest_inc_v6')
load_dict = json.loads(resp)

sql=(load_dict['sql'])
print(sql)


source_table=','.join(py_sqlparse.extract_tables(sql))

print(source_table)
exit()

print(sys.getdefaultencoding())
cf= configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值

print(v_url)
# cubename = sys.argv[1]
# buildtype = sys.argv[2]
# runTime = sys.argv[3] + " 08:00:00"
cubename = 'all'
runTime = [10000]
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
    jdata = json.dumps(values)                 # 对数据进行JSON格式化编码

    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.put(url,data=jdata,headers=header  )
    return req.text, req.status_code

def http_get_cube(v_url,v_cube):
    if v_cube=='all':
        url = v_url + '/kylin/api/cubes'
    else:
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




req, ret_code = http_get_cube(v_url,cubename)
print('~' * 100+'\n')
#print(req)

load_dict = json.loads(req)
cube_dict={}
segment_dict = {}
all_segment_list = []
all_cube_list=[]
count=0
print(type(req))
print(type(load_dict))

with open('/Users/wonder/Documents/data_dict/kylin/script/test2.txt', 'w') as f:
    f.write(load_dict)
exit()

for cube in load_dict:
    #print('cube:'+str(cube))

    #print(cube['name'])
    #if cube['project'] in  ('iqj_olap','','qianzhan_olap')  :

    #if cube['status'] !='DISABLED' and cube['project'] == 'iqj_olap'  :
    project_list =['iqj_olap' ,'qianzhan_olap']
    if  cube['project'] in project_list  :
        print(cube['project'])
        count += 1
        cube_list=[]
        # print(cube['project'])
        # print(cube['model'])
        # print(cube['name'])
        # print(cube['status'])
        # print(cube['last_modified'])
        # print(cube['segments'])

        # cube_dict['project'] = cube['project']
        # cube_dict['model'] = cube['model']
        # cube_dict['cube_name'] = cube['name']
        # cube_dict['status'] = cube['status']
        # cube_dict['segments'] = cube['segments']

        cube_list.append(cube['project'])
        cube_list.append(cube['model'])
        cube_list.append(cube['name'])
        cube_list.append(cube['status'])
        all_cube_list.append(cube_list)






        req, ret_code = http_get_cube(v_url, cube['name'])
        load_dict = json.loads(req)
        print('循环次数:'+str(count))
        for segment in load_dict['segments']:
                segment_list=[]
                #if segment['project'] == 'iqj_olap':
                    #print(segment['project'])
                    # print(segment['name'])
                    # print(segment['storage_location_identifier'].split(':')[0])
                    # print(segment['storage_location_identifier'].split(':')[1])
                    # print(segment['size_kb'])
                    # print(segment['input_records'])
                    # print(segment['input_records_size'])
                    # print(segment['last_build_time'])

                segment_list.append(cube['name'])
                segment_list.append(segment['name'])
                segment_list.append(segment['storage_location_identifier'].split(':')[0])
                segment_list.append(segment['storage_location_identifier'].split(':')[1])
                segment_list.append(segment['size_kb'])
                segment_list.append(segment['input_records'])
                segment_list.append(segment['input_records_size'])
                segment_list.append(segment['last_build_time'])




                # segment_dict['cube_name'] = cube['name']
                # segment_dict['segmentsegment_name']=segment['name']
                # segment_dict['hbase_namespace'] = segment['storage_location_identifier'].split(':')[0]
                # segment_dict['habse_table'] = segment['storage_location_identifier'].split(':')[1]
                # segment_dict['size_kb'] = segment['size_kb']
                # segment_dict['input_records'] = segment['input_records']
                # segment_dict['input_records_size'] = segment['input_records_size']
                # segment_dict['last_build_time'] = segment['last_build_time']
                all_segment_list.append(segment_list)


                #print(segment_dict)
                #cube_df=pd.DataFrame.from_dict(segment_dict, orient='index').T

                # print(segment_dict.info())
                # print('-' * 2)
                # print('~~' * 20)
                #
                #
                #
                # DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.155.69:3306/data_window?charset=utf8'
                # conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)
                # # pd.io.sql.to_sql(df,'test_python',con=conn)
                # cube_df.to_sql(name='cube_test', con=conn, if_exists='replace', index=False)

                print('segment_list:'+str(segment_list))
        segment_df = pd.DataFrame(all_segment_list, columns=['cube_name', 'segmentsegment_name', 'hbase_namespace','habse_table','size_kb','input_records','input_records_size','last_build_time'])
        print(segment_df)

        DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.155.69:3306/data_window?charset=utf8'
        conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)
        segment_df.to_sql(name='cube_test4', con=conn, if_exists='replace', index=False)

cube_df = pd.DataFrame(all_cube_list, columns=['project', 'model', 'cube_name','status'])
cube_df.to_sql(name='cube_model_mapping', con=conn, if_exists='replace', index=False)

exit()

with open('/Users/wonder/Documents/data_dict/kylin/script/test.txt', 'w') as f:
     f.write(req)



print ('结束')
