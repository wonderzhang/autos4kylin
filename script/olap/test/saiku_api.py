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




def http_create_private_folder(v_url):
    #url = 'http://192.168.176.67:7070'
    url=v_url+'/saiku/rest/saiku/api/repository/resource'
    jdata = {"file": 'iqj_user4', "name":'iqj_user4'}
    #jdata = json.dumps(values)                  # 对数据进行JSON格式化编码
    header={'Cookie': 'JSESSIONID=A50968C4F542C014990803519CF3597E; _ga=GA1.2.240813558.1524470143; route=374f267ddb01715c178b5b0ab135599a' ,'Content-Type':'application/x-www-form-urlencoded'}


    req = requests.post(url,data=jdata,headers=header  )
    return req.text, req.status_code

if __name__ == '__main__':
    resp, resp_code = http_create_private_folder('http://analysi4olap.finupgroup.com')
    print(resp_code)


