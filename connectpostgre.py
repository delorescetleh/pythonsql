#!/usr/bin/python
from flask import Flask
from flask import request
from flask_cors import CORS
from datetime import datetime
from decimal import Decimal
import psycopg2
import json

conn = psycopg2.connect(database="chirpstack_as_events", user="chirpstack_as_events",
                        password="dbpassword", host="127.0.0.1", port="5432")
print("Open database successfully")

cursor = conn.cursor()

def convertDict(Object):
    result = {}
    for key, value in Object.items():
        if type(value) == dict:
            result[key] = convertDict(value)
        elif type(value) == datetime:
            result[key] = value.timestamp()
        elif type(value) == type([]):
            result[key] = convertList(value)
        elif type(value) == type(Decimal(1)):
            result[key] = float(value)
        else:
            result[key] = value
    return result


def convertList(Object):
    result = []
    for value in Object:
        
        if type(value) == dict:    
            result.append(convertDict(value))
        elif type(value) == datetime:
            result.append(value.timestamp())
        elif type(value) == type([]):
            result.append(convertList(value))
        elif type(value) == type(Decimal(1)):
            result.append(float(value))
        else:
            result.append(value)
    return result

def getUpData(deviceName,qty):
    sqlCommand="select received_at from device_join where device_name = '"+deviceName+"' order by received_at desc limit 1;"
    cursor.execute(sqlCommand)
    rows = cursor.fetchall()
    lastJoinAt = rows[0][0]
    sqlCommand="select received_at,frequency,dr,adr,f_cnt,f_port,tags,object from device_up where device_name = '"+deviceName+"' order by received_at desc limit "+qty+";"
    cursor.execute(sqlCommand)
    rows = cursor.fetchall()
    elementsData=[]
    keys = ['receivedAt','frequency','dr','adr','f_cnt','f_port','tags','object']
    for row in rows:
        myobj = {}
        if (row[0] < lastJoinAt) :
            continue
        for i, key in enumerate(keys):
                myobj[key]=row[i]
        elementsData.append(myobj)
    return(json.dumps(convertList(elementsData)))

def getStatusData(deviceName,qty):
    sqlCommand="select received_at from device_join where device_name = '"+deviceName+"' order by received_at desc limit 1;"
    cursor.execute(sqlCommand)
    rows = cursor.fetchall()
    lastJoinAt = rows[0][0]
    sqlCommand="select received_at,margin,battery_level from device_status where device_name = '"+deviceName+"' order by received_at desc limit "+qty+";"
    cursor.execute(sqlCommand)
    rows = cursor.fetchall()
    elementsData=[]
    keys = ['receivedAt', 'margin', 'batteryLevel']
    for row in rows:
        myobj = {}
        if (row[0] <= lastJoinAt) :
            continue
        for i, key in enumerate(keys):
            myobj[key]=row[i]
        elementsData.append(myobj) 
    return(json.dumps(convertList(elementsData)))

def getJoinData(qty):
    sqlCommand="select received_at,device_name,application_name from device_join where received_at is not null order by received_at desc;"
    cursor.execute(sqlCommand)
    rows = cursor.fetchall()
    sqlCommand="select distinct device_name,application_name from device_join;"
    cursor.execute(sqlCommand)
    devices = cursor.fetchall()

    elementsData=[]
    for device in devices:
        elementData={}
        join_at=[]
        for row in rows:
            if device[0] == row [1]:
                join_at.append(row[0])  
        if len(join_at) == 0:
            continue
        elementData['joinAt']=join_at
        elementData['deviceName']=device[0]
        elementData['applicationName']=device[1]
        elementsData.append(elementData)
    return(json.dumps(convertList(elementsData)))

def getStatusListData():
    sqlCommand="select received_at,device_name,application_name from device_join where received_at is not null order by received_at desc;"
    cursor.execute(sqlCommand)
    rows = cursor.fetchall()
    sqlCommand="select distinct device_name,application_name from device_join;"
    cursor.execute(sqlCommand)
    devices = cursor.fetchall()

    elementsData=[]
    for device in devices:
        elementData={}
        join_at=[]
        for row in rows:
            if device[0] == row [1]:
                join_at.append(row[0])
                
                continue
        if len(join_at) == 0:
            continue
        elementData['joinAt']=join_at[0]
        elementData['deviceName']=device[0]
        sqlCommand="select received_at,margin,battery_level from device_status where device_name = '"+device[0]+"' order by received_at desc limit 1;"
        cursor.execute(sqlCommand)
        status = cursor.fetchall()
        if len(status) == 0:
            continue
        sqlCommand="select received_at,object,dr,f_cnt from device_up where device_name = '"+device[0]+"' order by received_at desc limit 1;"
        cursor.execute(sqlCommand)
        up = cursor.fetchall()
        elementData['applicationName']=device[1]
        elementData['lastStatusReceviedAt']=status[0][0]
        elementData['lastMargin']=status[0][1]
        elementData['lastBatteryLevel']=status[0][2]
        elementData['lastUpReceviedAt']=up[0][0]
        elementData['lastInformation']=up[0][1]
        elementData['lastDr']=up[0][2]
        elementData['lastFcnt']=up[0][3]
        
        elementsData.append(elementData)
    return(json.dumps(convertList(elementsData)))    

app=Flask(__name__)
cors = CORS(app,resource={r"/api/*":{"origins":"*"}})
@app.route("/",methods=['GET'])
def home():
    table=request.args.get('table')
    qty=request.args.get('qty')
    if (table=='up'):
        deviceName=request.args.get('device')
        return getUpData(deviceName,qty)
    if (table=='status'):
        deviceName=request.args.get('device')
        return getStatusData(deviceName,qty)
    if (table=='join'):
        return getJoinData(qty)
    if (table=='statuslist'):
        return getStatusListData()
    return "error"

if __name__=="__main__":
    app.run(host='0.0.0.0',port=8082,debug=True)


