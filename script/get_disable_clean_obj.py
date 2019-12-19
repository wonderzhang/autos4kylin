
#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File: http_put.py

#python inc_build_new_v4.py  iqj_user_inc_v2 None
#调用程序
#调用kylin_api 接口清理30天以上未使用的model和cube
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
import get_cube_hbase_relation_v2
print(sys.getdefaultencoding())
cf= configparser.ConfigParser()
cf.read('/Users/wonder/Documents/data_dict/kylin/script/conf/kylin_conf.ini')
secs =cf.sections()
print(secs)
option = cf.options("kylin-pwd")
v_pwd = cf.get("kylin-pwd", "pwd")  # 获取[Mysql-Database]中host对应的值
v_url = cf.get("kylin-pwd", "url")  # 获取[Mysql-Database]中host对应的值
v_project='iqj_olap'


option_db = cf.options("mysql")
v_model_cube_mapping = cf.get("mysql", "model_cube_mapping")  # 获取[Mysql-Database]中host对应的值
v_hbase_cube_mapping = cf.get("mysql", "hbase_cube_mapping")  # 获取[Mysql-Database]中host对应的值



print(v_url)
tm_now = time.localtime(time.time())
tm_now_str = time.strftime("%Y-%m-%d %H:%M:%S", tm_now)
cubename = 'all'
runTime = [10000]


engine = create_engine('mysql+pymysql://root:20131130@localhost:3306/finup_db4test')


sql_get_cube= '''
SELECT cube_name  FROM `finup_db4test`.`model_cube_mapping`  
where DATE_FORMAT(lastmodifytime,"%%Y-%%m-%%d") <= DATE_SUB(DATE_FORMAT(now(),'%%Y-%%m-%%d'), INTERVAL 30 DAY) 
and `status`='DISABLED'
ORDER BY `size_kb`;'''
df_cube = pd.read_sql_query(sql_get_cube, engine)
print(df_cube)
list_cube=df_cube.values.tolist()

for cube in list_cube:
    cube=(','.join(cube))
    if cube:
        print('drop '+ cube + '...')
        req, ret_code =  kylin_api.http_oper_cube(v_url=v_url,v_cube=cube,v_oper='drop')
        print(req)
        print(ret_code)
        print('drop '+cube+' done')



sql_get_model ='''
SELECT model  FROM finup_db4test.model_cube_mapping  where DATE_FORMAT(model_last_build_time,'%%Y-%%m-%%d') <=  DATE_SUB(DATE_FORMAT(now(),'%%Y-%%m-%%d'), INTERVAL 30 DAY)
and  cube_name is null
ORDER BY size_kb ;
 ;'''
df_model = pd.read_sql_query(sql_get_model, engine)

list_model=df_model.values.tolist()

for model in list_model:
    model=(','.join(model))
    if model:
        print('drop '+ model + '...')
        req, ret_code =  kylin_api.http_oper_model(v_url=v_url,v_model=model,v_oper='drop')
        print(req)
        print(ret_code)
        print('drop '+model+' done')

##清理后回下数据库
get_cube_hbase_relation_v2.main()
