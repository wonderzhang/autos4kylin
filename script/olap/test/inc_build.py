
import os
from datetime import datetime,timedelta
import sys
import time
cubename =  sys.argv[1]
#cubename = 'datawindow_cube_wide_inc2s'



def date_get(datetime=datetime):
    now = datetime.datetime.now()
    data_date = now.strftime('%Y-%m-%d')
    return data_date


cubename = sys.argv[1]

if sys.argv[2] == 'None':
  runTime = date_get()+ " 00:00:00"
else:
  runTime=sys.argv[2]+ " 00:00:00"

delta1 = (datetime.strptime(runTime, "%Y-%m-%d %H:%M:%S") + timedelta(days=-1)).strftime("%Y-%m-%d %H:%M:%S")
print('delta1:'+delta1+'\n' )



starttime = int(time.mktime(datetime.strptime(delta1, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
print('starttime:'+str(starttime)+'\n' )

endtime = int(time.mktime(datetime.strptime(runTime, "%Y-%m-%d %H:%M:%S").timetuple())) * 1000
print('endtime:'+str(endtime)+'\n' )


cs = """curl -X PUT -H "Authorization: Basic YWRtaW46S1lMSU4="   -H "Content-Type: application/json;charset=utf-8" -d '{"startTime":'%s', "endTime":'%s', "buildType":"BUILD"}' http://10.10.117.105:7070/kylin/api/cubes/%s/rebuild"""%(starttime,endtime,cubename)
print('\n'+cs)
os.system(cs)##run

