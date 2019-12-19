import os
import time
for ip in range(67,72):
    print(ip)
    os.system(f'curl -c ./cookie.txt -d "language=zh&password=admin&username=admin" http://192.168.176.{ip}:8080/saiku/rest/saiku/session')
    time.sleep(5)
    os.system(f'curl -b ./cookie.txt "http://192.168.176.6{ip}:8080/saiku/rest/saiku/admin/datasources/iqj_olap/refresh"')