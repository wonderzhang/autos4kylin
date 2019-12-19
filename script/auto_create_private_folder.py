#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File: http_put.py



import json
import requests

from selenium import webdriver
from selenium.webdriver import ActionChains
import  os
import time
from selenium.webdriver.common.keys import Keys

#d=webdriver.Chrome()
##url='https://www.qianpen.com/front/index.htm'

url='http://analysi4olap.finupgroup.com/saiku/rest/casLogin#'
#url='https://passport.jd.com/new/login.aspx?ReturnUrl=https%3A%2F%2Fwww.jd.com%2F%3Fcu%3Dtrue%26utm_source%3Dbaidu-pinzhuan%26utm_medium%3Dcpc%26utm_campaign%3Dt_288551095_baidupinzhuan%26utm_term%3D0f3d30c8dba7459bb52f2eb5eba8ac7d_0_fafa8d8e26f445d7a29be654da62e6b2'
#chrome_driver = "/Users/wonder/PycharmProjects/result_table_data_check/data/chromedriver"
chrome_driver='/Users/wonder/Downloads/chromedriver_72'
browser = webdriver.Chrome(executable_path=chrome_driver)
browser.get(url=url)
username1 = browser.find_element_by_xpath('//*[@id="username"]')
username1.send_keys('zhangwei09')
password1 = browser.find_element_by_xpath('//*[@id="password"]')
password1.send_keys('Thankyou1981')
browser.find_element_by_xpath('//*[@id="fm1"]/input[4]').click()
##需要停留 否则获取不到对象
time.sleep(5)
browser.maximize_window()
##发生重定向 ，需要重新定位新的url
url2='http://analysi4olap.finupgroup.com/saiku/rest/saiku/session/'
browser.get(url=url2)
# # print(browser.session_id)
# open = browser.find_element_by_id("open_query")
# open.click()
print((browser.get_cookies()))
print(browser.session_id)
cook=browser.get_cookies()
##获得相应的JSESSIONID 和 route
route='route=' + cook[0]['value']+';'
JSESSIONID='JSESSIONID=' + cook[1]['value']
cookie_str=route+JSESSIONID
print(cookie_str)
# cook2=browser.get_cookie(cook)
# for cookie in browser.get_cookies():
#     print("%s -> %s" % (cookie['name'], cookie['value']))

def http_get_users(v_url):
    url = v_url + '/saiku/rest/saiku/admin/users/'
    header={'Cookie': cookie_str ,'Content-Type':'application/x-www-form-urlencoded'}

    #header={'Authorization': 'Basic emhhbmd3ZWkwOTpUaGFua3lvdTE5ODE=' ,'Content-Type':'application/json'}
    req = requests.get(url,headers=header)
    return req.text, req.status_code

def http_create_private_folder(v_url,v_file_name,v_user_name):
    #url = 'http://192.168.176.67:7070'
    url=v_url+'/saiku/rest/saiku/api/repository/resource'
    #jdata = {"file": 'homes/iqj_user', "name":'iqj_user'}
    jdata = {"file": v_file_name, "name":v_user_name}
    #jdata = json.dumps(values)                  # 对数据进行JSON格式化编码
    header={'Cookie': cookie_str ,'Content-Type':'application/x-www-form-urlencoded'}
    #header={'Cookie': 'JSESSIONID=A50968C4F542C014990803519CF3597E; _ga=GA1.2.240813558.1524470143; route=374f267ddb01715c178b5b0ab135599a' ,'Content-Type':'application/x-www-form-urlencoded'}
    req = requests.post(url,data=jdata,headers=header  )
    return req.text, req.status_code



def http_get_metedata(v_url):
    url=v_url+'/saiku/rest/saiku/username/discover/iqj_olap/爱钱进olap/爱钱进olap/用户主题/metadata/'
    header = {'Cookie': cookie_str, 'Content-Type': 'application/x-www-form-urlencoded'}
    req = requests.post(url,headers=header)
    return req.text, req.status_code

resp, resp_code=http_get_users('http://analysi4olap.finupgroup.com')
print('~~~~~~~~~~~~~~~~~')
load_dict = json.loads(resp)


for i in load_dict:
     print (i['username'])
     user_name=i['username']
     file_name ='homes/'+user_name
     resp, resp_code=http_create_private_folder('http://analysi4olap.finupgroup.com',file_name,user_name)
     print(resp_code)
     print(resp)



