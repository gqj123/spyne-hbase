#!/usr/bin/env python
# -*- coding: utf-8 -*-

from spyne import Application,rpc,ServiceBase,Iterable,Integer,Unicode
from spyne.protocol.soap import Soap11,Soap12
from spyne.server.wsgi import WsgiApplication

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import happybase
import re
import sys  
import time
import os
import json


class RangeQuery(ServiceBase):    
    @rpc(Unicode,Unicode, Unicode, Unicode, Unicode, Unicode, Unicode, _returns=Unicode)
    def Range_Query(self,tableName,start_t,stop_t,lat_start,lon_start,lat_stop,lon_stop):
        transport = TSocket.TSocket('192.168.1.181', 9090)
        transport = TTransport.TBufferedTransport(transport)  
        protocol = TBinaryProtocol.TBinaryProtocol(transport)  
        client = Hbase.Client(protocol)
        transport.open()

        list_list = []

        if start_t=='':
            start_t='0'
        if stop_t=='':
            stop_t='2030'

        # startKey = start_t + '_' + lat_start + '_' + lon_start
        # stopKey = stop_t + '_' + lat_stop + '_' + lon_stop
        startKey = str(start_t).ljust(10, '0') + '_' + str("%3.2f" % float(lat_start)).rjust(6, '0') + '_' + str("%3.2f" % float(lon_start)).rjust(6, '0')
        stopKey = str(stop_t).ljust(10, '0') + '_' + str("%3.2f" % float(lat_stop)).rjust(6, '0') + '_' + str("%3.2f" % float(lon_stop)).rjust(6, '0')

        scanner = client.scannerOpenWithStop(tableName, startKey, stopKey, [])
        while True:
            result = client.scannerGet(scanner)   # 根据ScannerID来获取结果
            if not result:
                break
            for dir in result:
                list = []
                list.append(dir.columns.get('info:time').value)
                list.append(dir.columns.get('info:lat').value)
                list.append(dir.columns.get('info:lon').value)
                
                if(tableName=="WL"):
                    list.append(dir.columns.get('info:w_level').value)
                if(tableName=="AT"):
                    list.append(dir.columns.get('info:a_temperature').value)
                if(tableName=="SLP"):
                    list.append(dir.columns.get('info:pressure').value)
                if(tableName=="SST"):
                    list.append(dir.columns.get('info:w_temperature').value)
                if(tableName=="WIND"):
                    list.append(dir.columns.get('info:direction').value)
                    list.append(dir.columns.get('info:speed').value)
                if(tableName=="PSAL"):
                    list.append(dir.columns.get('info:deep').value)
                    list.append(dir.columns.get('info:deep_qc').value)
                    list.append(dir.columns.get('info:salinity').value)
                    list.append(dir.columns.get('info:salinity_qc').value)
                if(tableName=="TEMP"):
                    list.append(dir.columns.get('info:deep').value)
                    list.append(dir.columns.get('info:deep_qc').value)
                    list.append(dir.columns.get('info:temperature').value)
                    list.append(dir.columns.get('info:temperature_qc').value)
                if(tableName=="STATION_TEMP"):
                    list.append(dir.columns.get('info:deep').value)
                    list.append(dir.columns.get('info:temperature').value)
                if(tableName=="STATION_PSAL"):
                    list.append(dir.columns.get('info:deep').value)
                    list.append(dir.columns.get('info:salinity').value)
                if(tableName=="STATION_CUR"):
                    list.append(dir.columns.get('info:depth').value)
                    list.append(dir.columns.get('info:direction').value)
                    list.append(dir.columns.get('info:speed').value)

                list_list.append(list)

        responsejson = json.dumps(list_list)
        return responsejson


    @rpc(Unicode, Unicode, _returns=Unicode)
    def Point_Query(self, tableName, rowkey):
        connection = happybase.Connection('192.168.1.181')
        connection.open()
        table = connection.table(tableName)
        d = table.row(rowkey)
        dict = {}

        for k, v in d.items():
            if (k.decode("utf8") == 'info:time'):
                dict["time"] = v.decode("utf8")
            if (k.decode("utf8") == 'info:lat'):
                dict["lat"] = v.decode("utf8")
            if (k.decode("utf8") == 'info:lon'):
                dict["lon"] = v.decode("utf8")

        if (tableName == "WL"):
            for k, v in d.items():
                if (k.decode("utf8") == 'info:w_level'):
                    dict["w_level"] = v.decode("utf8")

        if (tableName == "AT"):
            for k, v in d.items():
                if (k.decode("utf8") == 'info:a_temperature'):
                    dict["a_temperature"] = v.decode("utf8")

        if (tableName == "SLP"):
            for k, v in d.items():
                if (k.decode("utf8") == 'info:pressure'):
                    dict["pressure"] = v.decode("utf8")

        if (tableName == "SST"):
            for k, v in d.items():
                if (k.decode("utf8") == 'info:w_temperature'):
                    dict["w_temperature"] = v.decode("utf8")

        if (tableName == "WIND"):
            for k, v in d.items():
                if (k.decode("utf8") == 'info:direction'):
                    dict["direction"] = v.decode("utf8")
            for k, v in d.items():
                if (k.decode("utf8") == 'info:speed'):
                    dict["speed"] = v.decode("utf8")

        if (tableName == "PSAL"):

            for k, v in d.items():
                if (k.decode("utf8") == 'info:deep'):
                    dict["deep"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:deep_qc'):
                    dict["deep_qc"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:salinity'):
                    dict["salinity"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:salinity_qc'):
                    dict["salinity_qc"] = v.decode("utf8")

        if (tableName == "TEMP"):

            for k, v in d.items():
                if (k.decode("utf8") == 'info:deep'):
                    dict["deep"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:deep_qc'):
                    dict["deep_qc"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:temperature'):
                    dict["temperature"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:temperature_qc'):
                    dict["temperature_qc"] = v.decode("utf8")

        if (tableName == "STATION_TEMP"):

            for k, v in d.items():
                if (k.decode("utf8") == 'info:deep'):
                    dict["deep"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:temperature'):
                    dict["temperature"] = v.decode("utf8")

        if (tableName == "STATION_PSAL"):

            for k, v in d.items():
                if (k.decode("utf8") == 'info:deep'):
                    dict["deep"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:salinity'):
                    dict["salinity"] = v.decode("utf8")

        if (tableName == "STATION_CUR"):

            for k, v in d.items():
                if (k.decode("utf8") == 'info:depth'):
                    dict["depth"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:direction'):
                    dict["direction"] = v.decode("utf8")

            for k, v in d.items():
                if (k.decode("utf8") == 'info:speed'):
                    dict["speed"] = v.decode("utf8")

        responsejson = json.dumps(dict)
        return responsejson


application = Application([RangeQuery],'http://schemas.xmlsoap.org/soap/envelope',in_protocol=Soap11(validator='lxml'),out_protocol=Soap11())
wsgi_application = WsgiApplication(application)

if __name__ == '__main__':    
    import logging    
    from wsgiref.simple_server import make_server    
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger('spyne.protocol.xml').setLevel(logging.ERROR)
    logging.info("listening to http://192.168.1.181:8888")
    logging.info("wsdl is at: http://192.168.1.181:8888/?wsdl")
    server = make_server('192.168.1.181',8888,wsgi_application)
    server.serve_forever()