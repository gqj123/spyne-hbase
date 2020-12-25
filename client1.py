#改进点查询测试
from suds.client import Client
import json

# tableName = input("tableName:")
# rowkey = input("rowkey:")

client = Client('http://221.239.0.181:8888/?wsdl')
res = client.service.Point_Query('WL','1861071407_138.38_175.50')
dict = json.loads(res)
print(dict["time"])