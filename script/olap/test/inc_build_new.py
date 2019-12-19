

import os
from datetime import datetime,timedelta
import sys
import time

def date_get(datetime=datetime):
    now = datetime.datetime.now() + datetime.timedelta(days=-1)
    data_date = now.strftime('%Y-%m-%d')
    return data_date

if argv[1] == 'None':
  v_datadate = date_get()
else:
  v_datadate=argv[2]

cubename =  sys.argv[1]
nowdate = sys.argv[2] + "00:00:00"
#cubename = 'datawindow_cube_wide_inc2s'
delta1 = (datetime.strptime(nowdate, "%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
endtime = int(time.mktime(datetime.strptime(nowdate, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
starttime = int(time.mktime(datetime.strptime(delta1, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
cs = """curl -X PUT -H "Authorization: Basic YWRtaW46S1lMSU4="   -H "Content-Type: application/json;charset=utf-8" -d '{"startTime":'%s', "endTime":'%s', "buildType":"BUILD"}' http://10.10.117.105:7070/kylin/api/cubes/%s/rebuild"""%(starttime,endtime,cubename)
print('\n'+cs)
os.system(cs)##run

