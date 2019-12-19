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
import sys
import encodeself
# sys.setrecursionlimit(100000) #例如这里设置为十万
import encodeself
import configparser

print(sys.getdefaultencoding())

cf = configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs = cf.sections()
# print(secs)
option = cf.options("kylin-pwd")
v_user_name = cf.get("kylin-pwd", "user_name")  # 获取[Mysql-Database]中host对应的值
v_user_passwd = cf.get("kylin-pwd", "user_passwd")  # 获取[Mysql-Database]中host对应的值
v_user_pwd=v_user_name+':'+v_user_passwd

basekey='Basic ' + encodeself.pwdkey(v_user_pwd)

# cubename = sys.argv[1]
# buildtype = sys.argv[2]
# runTime = sys.argv[3] + " 08:00:00"
# cubename = 'datawindow_cube_wide_inc2s'
runTime = [10000]
v_url = 'http://analysi4olap.finupgroup.com'
#
# # url = v_url + '/kylin/api/jobs/' + '2541c505-d937-f68f-1ad8-c18d7fb5acf2' + '/' + 'cancel'
# # print(url)
# # header = {'Authorization': 'Basic YWRtaW46S1lMSU4=', 'Content-Type': 'application/json'}
# # req = requests.put(url, headers=header)
#
#
#
# exit()
def http_put_inc(v_url,v_cube, v_startTime, v_endTime,v_refreshtype):
    #url = 'http://192.168.176.67:7070'
    url=v_url+'/kylin/api/cubes/'+v_cube+'/rebuild'
    values = {"startTime": v_startTime, "endTime": v_endTime, "buildType": v_refreshtype}
    jdata = json.dumps(values)                  # 对数据进行JSON格式化编码
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


def http_get_model(v_url,v_project,v_model=''):
    #''可变参数
    if v_project=='all':
        url = v_url + '/kylin/api/model'
    else:
        url = v_url + '/kylin/api/models?projectName='+v_project

    if v_model:
        url = v_url +  '/kylin/api/models?projectName='+v_project+'/'+v_model
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.get(url, headers=header  )
    return req.text, req.status_code


def http_get_optimize(v_url,v_cube):
    url=v_url+'/kylin/api/cubes/'+v_cube+'/cuboids/recommend'
    header={'Authorization': basekey ,'Content-Type':'application/json','Accept': 'application/json'}
    req = requests.get(url, headers=header)
    return req.text, req.status_code


def http_get_cube_sql(v_url,v_cube):
    url=v_url+'/kylin/api/cubes/'+v_cube+'/sql'
    header={'Authorization': basekey ,'Content-Type':'application/json','Accept': 'application/json'}
    req = requests.get(url, headers=header)
    return req.text, req.status_code,req.cookies



def http_get(v_url,v_job_id):
    url=v_url+'/kylin/api/jobs/'+v_job_id
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.get(url, headers=header  )
    return req.text, req.status_code


def http_get_all_job(v_url,v_project,v_status):
    # cubeName - optional string Cube name.
    # projectName - required string Project name.
    # status - optional int Job status, e.g. (NEW: 0, PENDING: 1, RUNNING: 2, STOPPED: 32, FINISHED: 4, ERROR: 8, DISCARDED: 16)
    # offset - required int Offset used by pagination.
    # limit - required int Jobs per page.
    # timeFilter - required int, e.g. (LAST ONE DAY: 0, LAST ONE WEEK: 1, LAST ONE MONTH: 2, LAST ONE YEAR: 3, ALL: 4)
    url = v_url + '/kylin/api/jobs'+'?jobSearchMode=ALL&offset=0&projectName='+v_project+'&status='+str(v_status)+'&timeFilter=4'
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    req = requests.get(url, headers=header  )
    return req.text, req.status_code



def http_oper_job(v_url,v_job,v_oper):
    url=v_url+'/kylin/api/jobs/'+v_job+'/'+v_oper
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    if v_oper!='drop':
        req = requests.put(url, headers=header)
    else:
        req =requests.delete(url, headers=header)
    return req.text, req.status_code


def http_oper_cube(v_url,v_cube,v_oper):
    """http_oper_cube(v_url,v_cube,drop)  删除cube  """


    url=v_url+'/kylin/api/cubes/'+v_cube+'/'+v_oper
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}


    if v_oper=='sql':
        req = requests.get(url, headers=header)
    elif v_oper!='drop':
        req = requests.put(url, headers=header)
    else:
        url=v_url+'/kylin/api/cubes/'+v_cube
        req =requests.delete(url, headers=header)
    return req.text, req.status_code




def http_oper_model(v_url,v_model,v_oper):
    url=v_url+'/kylin/api/models/'+v_model+'/'+v_oper
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    if v_oper!='drop':
        req = requests.put(url, headers=header)
    else:
        url=v_url+'/kylin/api/models/'+v_model
        req =requests.delete(url, headers=header)
    return req.text, req.status_code




def http_oper_projcet(v_url,v_projcet,v_oper):

    url=v_url+'/kylin/api/projects/'+v_projcet
    header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    if v_oper!='drop':
        req = requests.put(url, headers=header)
    else:
        req =requests.delete(url, headers=header)
    return req.text, req.status_code



def http_oper_optimize(v_url,v_cube):

    resp, resp_code = http_get_optimize(v_url, v_cube)
    load_dict = json.loads(resp)
    for i in load_dict.values():
        base_recommand_cuboid_id = (i['cuboid_id'])
        base_recommand_list.append(base_recommand_cuboid_id)
        recommand_list = (unlimit_loop(i['children']))

    # basecuboid+其他所有cuboid
    base_recommand_list.extend(recommand_list)
    url=v_url+'/kylin/api/cubes/'+v_cube+'/optimize'
    header={'Authorization': basekey ,'Content-Type':'application/json','Accept': 'application/json'}
    values = { "cuboidsRecommend":base_recommand_list}
    jdata = json.dumps(values)  # 对数据进行JSON格式化编码
    req = requests.put(url, data=jdata,headers=header)
    return req.text, req.status_code






# ##只打印第一层
# def unlimit_loop_lev1(x):
#     for i in x:
#         if not i['children']:
#             print(i['cuboid_id'], type(i['cuboid_id']))
#
# ##只打印最后一层 children没有值的
# def unlimit_loop_levx(x):
#     for i in x:
#         if not i['children']:
#             print(i['cuboid_id'], type(i['cuboid_id']))
#         else:
#             unlimit_loop(i['children'])
# ##全部打印
# def unlimit_loop_all(x):
#     for i in x:
#         if  i['children']:
#             unlimit_loop(i['children'])
#         print(i['cuboid_id'], type(i['cuboid_id']))


def unlimit_loop(x):
    for i in x:
        if  i['children']:
          unlimit_loop(i['children'])
        print(i['cuboid_id'])
        recommand_list.append(i['cuboid_id'])##recommand_list 不需要声明 在被调用方声明过了
    return recommand_list

if __name__ == '__main__':
    print(basekey)
    recommand_list = []
    base_recommand_list = []
    resp,resp_code= http_oper_optimize(v_url,'iqj_user_common_v3')
    print(resp)
