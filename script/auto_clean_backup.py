import os
import datetime
import shutil

def date_format(x):
        dd = datetime.datetime.now()
        E_DATE = dd.strftime('%Y-%m-%d')
        _31DayAgo = (dd - datetime.timedelta(days=x))
        V_DATE = _31DayAgo.strftime('%Y-%m-%d')
        return (V_DATE, E_DATE)





daya,dayb=date_format(15)
daya_fromat= daya.replace('-','_')
daya_fromat##'2019_11_05'
list_meta = os.listdir(os.getcwd())
for meta in list_meta:
    meta_format= meta[5:15]
    if (daya_fromat > meta_format):
        print (meta)
        shutil.rmtree(meta) 
