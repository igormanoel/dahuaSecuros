import requests
import json
import re
import ast
import time
import datetime
import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2


#Global variables
settings = {}
devices = {}
devices['devices'] = []
deviceIps = list()
#Log settings
logger = logging.getLogger("Rotating Log")
logname = "main.log"
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(logname, when="midnight", interval=1)
logger.addHandler(handler)


def connect(deviceIp):
    deviceIndex = int(deviceIps.index(deviceIp))
    sessionAuth = requests.Session()
    authB = requests.auth.HTTPBasicAuth(devices['devices'][deviceIndex]['user'], settings['camPassword'])
    authD = requests.auth.HTTPDigestAuth(devices['devices'][deviceIndex]['user'], settings['camPassword'])
    urlT = (f'http://{deviceIp}:80/cgi-bin/configManager.cgi?action=getConfig&name=MotionDetect')
    sB = sessionAuth.get(urlT, auth=authB)
    sD = sessionAuth.get(urlT, auth=authD)
    if sB.status_code == 200:
        devices['devices'][deviceIndex]['auth'] = 'basic'
    if sD.status_code == 200:
        devices['devices'][deviceIndex]['auth'] = 'digest'
    while True:
        retryTime = 0
        try:
            auth = str()
            now = datetime.datetime.now()
            logger.info(f'{now.strftime("%d")}/{now.strftime("%m")}/{now.strftime("%Y")} {now.strftime("%H")}:{now.strftime("%M")}:{now.strftime("%S")}-Trying to connect to: {deviceIp}')
            if devices['devices'][deviceIndex]['auth'] == 'basic':
                auth = authB
            elif devices['devices'][deviceIndex]['auth'] == 'digest':
                auth = authD
            url = (f'http://{deviceIp}:80/cgi-bin/eventManager.cgi?action=attach&codes=[All]&heartbeat=5')
            session = requests.Session()
            r = session.get(url, auth=auth, stream=True, timeout=30)
            retryTime = 1
            if not r.encoding:
                r.encoding = 'utf-8'
            #Event Filter
            pattern = '(^Code.*)'
            pattern2 = '^((?!Code=NewFile;action=Pulse.*).)*$'
            pattern3 = '^((?!Code=VideoMotionInfo;action=State;index=0).)*$'
            pattern4 = '^((?!Code=NTPAdjustTime;action=Pulse;index=0;data={).)*$'
            pattern5 = '^((?!Code=VideoMotion;action=Stop;index=.*).)*$'
            pattern6 = '^((?!Code=IntelliFrame;action=Pulse;index=0;data={).)*$'
            pattern7 = '^((?!Code=CrossLineDetection;action=Stop;.*).)*$'
            pattern8 = '^((?!Code=RtspSessionDisconnect;action=.*).)*$'
            pattern9 = '^((?!Code=SceneChange;action=Stop;index=.*).)*$'
            if r.status_code == 200:
                logger.info(f'{now.strftime("%d")}/{now.strftime("%m")}/{now.strftime("%Y")} {now.strftime("%H")}:{now.strftime("%M")}:{now.strftime("%S")}-Connected successfully to: {deviceIp}')
                    for line in r.iter_lines(decode_unicode=True, chunk_size=10):
                        if all(re.findall(pattern, line, flags=re.MULTILINE),
                           re.findall(pattern2, line, flags=re.MULTILINE),
                           re.findall(pattern3, line, flags=re.MULTILINE),
                           re.findall(pattern4, line, flags=re.MULTILINE),
                           re.findall(pattern5, line, flags=re.MULTILINE),
                           re.findall(pattern6, line, flags=re.MULTILINE),
                           re.findall(pattern7, line, flags=re.MULTILINE),
                           re.findall(pattern8, line, flags=re.MULTILINE),
                           re.findall(pattern9, line, flags=re.MULTILINE)):
                            logger.info(line)
                            line1 = line.replace("=", '":"')
                            line2 = line1.replace(";", '","')
                            line3 = '{"' + line2 + '"}'
                            event = ast.literal_eval(line3)
                            now = datetime.datetime.now()   
                            finalCode = event['Code']
                            channel = int(event['index'])
                            securosId = devices['devices'][deviceIndex]['objId'][channel]
                            #Writing to Spectator
                            securosSession = requests.Session()
                            s = securosSession.get(f'http://{settings["httpEventGateIp"]}:{settings["httpEventGatePort"]}/event?name={devices["devices"][deviceIndex]["name"]}&securosId={securosId}&code={event["Code"]}')
                            logger.info(f'{now.strftime("%d")}/{now.strftime("%m")}/{now.strftime("%Y")} {now.strftime("%H")}:{now.strftime("%M")}:{now.strftime("%S")}-Event:{event["Code"]} Name:{devices["devices"][deviceIndex]["name"]}')
        except Exception as err:
            now = datetime.datetime.now()
            logger.info(err)
            logger.info(f'{now.strftime("%d")}/{now.strftime("%m")}/{now.strftime("%Y")} {now.strftime("%H")}:{now.strftime("%M")}:{now.strftime("%S")}-Lost connection to: {devices["devices"][deviceIndex]["name"]}, retrying in {retryTime} seconds.')
            #Wrinting file for TxtToSpectator
            securosSession = requests.Session()
            s = securosSession.get(f'http://{settings["httpEventGateIp"]}:{settings["httpEventGatePort"]}/event?name={devices["devices"][deviceIndex]["name"]}&id={devices["devices"][deviceIndex]["id"]}&code=LostConnection') 
            time.sleep(retryTime)
            if retryTime > 29:
                retryTime = 30
            else:
                retryTime = retryTime + 1
            continue
        break

def getConfig():
    global settings
    with open('config.json', 'r') as config_json:
        settings = json.load(config_json)
        type(settings)
        #logger.info(settings)
        


def getSecurOs():
    try:
        connection = psycopg2.connect(user =  settings['userDB'],password = settings['passwordDB'],host = settings['ipDB'],port = settings['portDB'],database = settings['database'])
        cursor = connection.cursor()

        cursor.execute("SELECT id, name, ip, user_name FROM \"OBJ_GRABBER\" ORDER BY id")
        records = cursor.fetchall()
        #Build cam configuration
        for record in records:
            devices['devices'].append({'id': record[0], 'name': record[1],'ip': record[2],'user': record[3], 'auth' : 'null','password' : '', 'objId' : {}})
            deviceIps.append(record[2])
        cursor.execute("SELECT id, parent_id, mux FROM \"OBJ_CAM\" ORDER BY id")
        recordcams = cursor.fetchall()
        #Insert channel Ids   
        for record in recordcams:
            parentId = int(record[1]) - 1
            channelConfig = int(record[2])
            id = int(record[0])
            devices['devices'][parentId]['objId'][channelConfig] = id
        with open('devices.json', 'w') as outfile:
            json.dump(devices, outfile)
    except (Exception, psycopg2.Error) as error :
        logger.info ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                logger.info("PostgreSQL connection is closed")


if __name__ == "__main__":
    getConfig()
    getSecurOs()
    procs = len(deviceIps)
    logger.info(f'Processos:{procs} Processos:{deviceIps}')
    processes = []
    with ThreadPoolExecutor(max_workers=procs) as executor:
           for deviceIp in deviceIps:
               now = datetime.datetime.now()
               processes.append(executor.submit(connect, deviceIp))
    for task in as_completed(processes):
        logger.info(task.result())
    logger.info('{now.strftime("%d")}/{now.strftime("%m")}/{now.strftime("%Y")} {now.strftime("%H")}:{now.strftime("%M")}:{now.strftime("%S")}-Closing program')
