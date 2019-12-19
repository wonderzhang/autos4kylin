#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File: http_put.py

#python inc_build_new_v4.py  iqj_user_inc_v2 None
#批量删除作业 或者 cube model
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
from tqdm import tqdm

print(sys.getdefaultencoding())
cf= configparser.ConfigParser()

cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值

print(v_url)
project = sys.argv[1]
# cubename = sys.argv[1]
# buildtype = sys.argv[2]
# runTime = sys.argv[3] + " 08:00:00"

# cubename = sys.argv[1]
# buildtype = sys.argv[2]
# runTime = sys.argv[3] + " 08:00:00"
runTime = [10000]
def date_get(datetime=datetime):
    now = datetime.now()
    data_date = now.strftime('%Y-%m-%d')
    return data_date

#req, ret_code =kylin_api.http_oper_cube(v_url,'iqj_user_inc_v5','sql')
# Request URL: http://analysi4olap.finupgroup.com/kylin/api/cubes/iqj_user_inc_v5/sql


model_list=[]
req, ret_code = kylin_api.http_get_cube(v_url, 'all')

##删除cube
load_dict = json.loads(req)
for cube in tqdm(load_dict):
    if (cube['project']==project):
        print(cube['model']+':'+cube['name'])
        if cube['status']!='DISABLED':
           kylin_api.http_oper_cube(v_url,cube['name'],'disable')

        kylin_api.http_oper_cube(v_url,cube['name'],'drop')




##删除model
req, ret_code = kylin_api.http_get_model(v_url,project)
load_dict = json.loads(req)
for model in tqdm(load_dict):
    print(model['name'])
    req, ret_code = kylin_api.http_oper_model(v_url, model['name'], 'drop')


##删除项目
kylin_api.http_oper_projcet(v_url,project,'drop')






