
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
from tqdm import tqdm


def main():
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
    # cubename = sys.argv[1]
    # buildtype = sys.argv[2]
    # runTime = sys.argv[3] + " 08:00:00"

    # cubename = sys.argv[1]
    # buildtype = sys.argv[2]
    # runTime = sys.argv[3] + " 08:00:00"
    cubename = 'all'
    runTime = [10000]
    def date_get(datetime=datetime):
        now = datetime.now()
        data_date = now.strftime('%Y-%m-%d')
        return data_date


    ##日期差
    def days_diff(str1,str2):
        date1=datetime.strptime(str1[0:10],"%Y-%m-%d")
        date2=datetime.strptime(str2[0:10],"%Y-%m-%d")
        num=(date1-date2).days
        return num


    ##月份差
    def months_diff(str1,str2):
        year1=datetime.strptime(str1[0:10],"%Y-%m-%d").year
        year2=datetime.strptime(str2[0:10],"%Y-%m-%d").year
        month1=datetime.strptime(str1[0:10],"%Y-%m-%d").month
        month2=datetime.strptime(str2[0:10],"%Y-%m-%d").month
        num=(year1-year2)*12+(month1-month2)
        return num


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
    #
    #
    #
    # def http_put_inc(v_url,v_cube, v_startTime, v_endTime,v_refreshtype):
    #     #url = 'http://192.168.176.67:7070'
    #     url=v_url+'/kylin/api/cubes/'+v_cube+'/rebuild'
    #     values = {"startTime": v_startTime, "endTime": v_endTime, "buildType": v_refreshtype}
    #     jdata = json.dumps(values)                  # 对数据进行JSON格式化编码
    #     header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    #     req = requests.put(url,data=jdata,headers=header  )
    #     return req.text, req.status_code
    #
    # def http_put_full(v_url,v_cube):
    #     #url = 'http://192.168.176.67:7070'
    #     url=v_url+'/kylin/api/cubes/'+v_cube+'/rebuild'
    #     values = {"buildType": "BUILD"}
    #     jdata = json.dumps(values)                 # 对数据进行JSON格式化编码
    #
    #     header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    #     req = requests.put(url,data=jdata,headers=header  )
    #     return req.text, req.status_code
    #
    # def http_get_cube(v_url,v_cube):
    #     if v_cube=='all':
    #         url = v_url + '/kylin/api/cubes'
    #     else:
    #     #url = 'http://192.168.176.67:7070'
    #         url = v_url +'/kylin/api/cubes/'+v_cube
    #     header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    #     req = requests.get(url, headers=header  )
    #     return req.text, req.status_code
    #
    #
    #
    # def http_get(v_url,v_job_id):
    #     #url = 'http://192.168.176.67:7070'
    #     url=v_url+'/kylin/api/jobs/'+v_job_id
    #     header={'Authorization': 'Basic YWRtaW46S1lMSU4=' ,'Content-Type':'application/json'}
    #     req = requests.get(url, headers=header  )
    #     return req.text, req.status_code

    req, ret_code = kylin_api.http_get_all_job(v_url,v_project,16)
    load_dict = json.loads(req)
    for cube in tqdm(load_dict):
        # if cube['related_cube']=='KYLIN_HIVE_METRICS_QUERY_PROD':
        if 'PROD' in  cube['related_cube']:
           print(cube['related_cube'])
           print(cube['uuid'])
           #req, ret_code= kylin_api.http_oper_job(v_url,cube['uuid'],'resume')
           req, ret_code= kylin_api.http_oper_job(v_url,cube['uuid'],'drop')


           print(req)
           print(ret_code)


    # PUT /kylin/api/jobs/{jobId}/cancel
    # with open('/Users/wonder/Documents/data_dict/kylin/script/data/output/test_job.txt', 'w') as f:
    #      f.write(req)
    all_model_list=[]
    req,ret_code = kylin_api.http_get_model(v_url,v_project)
    load_dict = json.loads(req)
    for model in tqdm(load_dict):
        #print(model['name'])

        tmObject = time.localtime((model['last_modified'] / 1000))
        tm_lastbuild_str = time.strftime("%Y-%m-%d %H:%M:%S", tmObject)
        #print(tm_lastbuild_str)

        model_str = (model['name'] + ',' + tm_lastbuild_str+','+v_project)

        model_list = [i for i in model_str.split(',')]
        all_model_list.append(model_list)

    model_df = pd.DataFrame(all_model_list,columns=['model', 'model_last_build_time','project'])
    # print(model_df)
    #
    # exit()



    req, ret_code = kylin_api.http_get_cube(v_url,cubename)
    print('~' * 100+'\n')
    #print(req)

    load_dict = json.loads(req)
    cube_dict={}
    segment_dict = {}
    all_segment_list = []
    all_cube_list=[]
    count=0
    print(type(req))


    for cube in tqdm(load_dict):
        #print('cube:'+str(cube))
        #print(cube['name'])
        if cube['project'] ==  'iqj_olap' :
        #if cube['status'] !='DISABLED' and cube['project'] == 'iqj_olap'  :
        # project_list =['iqj_olap' ,'qianzhan_olap']
        # if  cube['project'] in project_list  :
        #     print(cube['project'])
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

            print(cube)

            cube_list.append(cube['model'])
            cube_list.append(cube['name'])
            cube_list.append(cube['status'])
            cube_list.append(cube['size_kb'])

            tmObject = time.localtime((cube['last_modified'] / 1000))
            tm_lastbuild_str = time.strftime("%Y-%m-%d %H:%M:%S", tmObject)

            tmObject2 = time.localtime((cube['create_time_utc'] / 1000))
            tm_create_str = time.strftime("%Y-%m-%d %H:%M:%S", tmObject2)





            days_diff_cnt = days_diff(tm_now_str, tm_lastbuild_str)
            cube_list.append(tmObject)
            cube_list.append(tmObject2)

            cube_list.append(days_diff_cnt)

            all_cube_list.append(cube_list)

            req, ret_code = kylin_api.http_get_cube(v_url, cube['name'])
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

                    all_segment_list.append(segment_list)


                    #print(segment_dict)
                    #cube_df=pd.DataFrame.from_dict(segment_dict, orient='index').T

                    # print(segment_dict.info())
                    # print('-' * 2)
                    # print('~~' * 20)

                    # DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.155.69:3306/data_window?charset=utf8'
                    # conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)
                    # # pd.io.sql.to_sql(df,'test_python',con=conn)
                    # cube_df.to_sql(name='cube_test', con=conn, if_exists='replace', index=False)

                    print('segment_list:'+str(segment_list))
            segment_df = pd.DataFrame(all_segment_list, columns=['cube_name', 'segmentsegment_name', 'hbase_namespace','habse_table','size_kb','input_records','input_records_size','last_build_time'])
            print(segment_df)

            DB_CONNECT = 'mysql+pymysql://root:20131130@localhost:3306/finup_db4test?charset=utf8'

            #DB_CONNECT = 'mysql+pymysql://data_window:zPcQl%R2xWJ!ByXe@192.168.198.30:3306/data_window?charset=utf8'
            conn = create_engine(DB_CONNECT, echo=True, pool_size=10, max_overflow=20)
            segment_df.to_sql(name=v_hbase_cube_mapping, con=conn, if_exists='replace', index=False)


    cube_df = pd.DataFrame(all_cube_list, columns=[ 'model', 'cube_name','status','size_kb','lastmodifytime','createtime','days_diff_cnt'])

    ## model_df left join  cube_df
    model_cube_df=pd.merge(model_df,cube_df,on='model',how='outer')
    print(model_cube_df)
    model_cube_df.to_sql(name=v_model_cube_mapping, con=conn, if_exists='replace', index=False)

    print ('结束')

if __name__ == '__main__':
                main()


