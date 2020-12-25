#范围查询test
from suds.client import Client
import json
from time import *

##烦我查询测试
# start_t=input("start:")
# lat_start=input("lat_start:")
# lon_start=input("lon_start:")
#
# stop_t=input("stop:")
# lat_stop=input("lat_stop:")
# lon_stop=input("lon_stop:")
tableName = 'WL'
start_t = 2006060000
lat_start = 0
lon_start = 0
stop_t = 2006060200
lat_stop = 0
lon_stop = 0


start_time = time()
client = Client('http://221.239.0.181:8888/?wsdl')
# client.set_options(timeout=3600)
res = client.service.Range_Query(tableName,start_t,stop_t,lat_start,lon_start,lat_stop,lon_stop)
dict = json.loads(res)
print(dict)
end_time = time()
print(len(dict))
print('该程序的运行时间是：' , end_time-start_time)