#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File: http_put.py

#python inc_build_new_v4.py  iqj_user_inc_v2 None
#批量删除作业 或者 cube model
import os
from datetime import datetime, timedelta
import sys
import time
import  datetime
import urllib
import json
import requests
import configparser
import pandas as pd
from   sqlalchemy import create_engine
import kylin_api
from tqdm import tqdm
import  pandas as pd

test='iqj_invest_full_v4,iqj_invest_full_v8,2019-09-02 14:25:55,18'
#test='iqj_invest_full_v4,2,3,4'


# test_list=[i for i in test.split(',')]
# print(test_list)


# exit()
tm_now = time.localtime(time.time())
tm_now_str = time.strftime("%Y-%m-%d %H:%M:%S", tm_now)

print(tm_now_str)

print(sys.getdefaultencoding())
cf= configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值

print(v_url)
#project = sys.argv[1]
project = 'iqj_olap'

# cubename = sys.argv[1]
# b#uildtype = sys.argv[2]
# runTime = sys.argv[3] + " 08:00:00"

# cubename = sys.argv[1]
# buildtype = sys.argv[2]
# runTime = sys.argv[3] + " 08:00:00"
runTime = [10000]
def date_get(datetime=datetime):
    now = datetime.now()
    data_date = now.strftime('%Y-%m-%d')
    return data_date


##日期差
def days_diff(str1,str2):
    date1=datetime.datetime.strptime(str1[0:10],"%Y-%m-%d")
    date2=datetime.datetime.strptime(str2[0:10],"%Y-%m-%d")
    num=(date1-date2).days
    return num


##月份差
def months_diff(str1,str2):
    year1=datetime.datetime.strptime(str1[0:10],"%Y-%m-%d").year
    year2=datetime.datetime.strptime(str2[0:10],"%Y-%m-%d").year
    month1=datetime.datetime.strptime(str1[0:10],"%Y-%m-%d").month
    month2=datetime.datetime.strptime(str2[0:10],"%Y-%m-%d").month
    num=(year1-year2)*12+(month1-month2)
    return num

#req, ret_code =kylin_api.http_oper_cube(v_url,'iqj_user_inc_v5','sql')
# Request URL: http://analysi4olap.finupgroup.com/kylin/api/cubes/iqj_user_inc_v5/sql


model_list=[]
req, ret_code = kylin_api.http_get_cube(v_url, 'all')

##删除cube
cubes_list=[]
load_dict = json.loads(req)
for cube in tqdm(load_dict):
    if (cube['project']==project):
        if(cube['status']!='READY'):
           #print(cube)

           tmObject = time.localtime((cube['last_modified'] / 1000))
           tm_lastbuild_str= time.strftime("%Y-%m-%d %H:%M:%S", tmObject)


           days_diff_cnt=days_diff(tm_now_str,tm_lastbuild_str)
           cube_str=(cube['model']+','+cube['name']+','+str(tm_lastbuild_str) +','+str(days_diff_cnt))

           cube_list = [i for i in cube_str.split(',')]
           cubes_list.append(cube_list)
           #print(list(eval(cube_str)))







#print(cubes_list)


DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.155.69:3306/data_window?charset=utf8'
conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)

cube_df = pd.DataFrame(cubes_list, columns=['model', 'cube_name', 'lastbuildtime','days_diff_cnt'])


print(cube_df)
#cube_df.to_sql(name='cube_model_mapping', con=conn, if_exists='replace', index=False)


    # iqj_invest_full_v4,iqj_invest_full_v8,2019-09-02 14:25:55,18



