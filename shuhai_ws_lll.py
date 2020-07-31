# coding=UTF-8
import websocket
import json
import hashlib
import time
import zlib
import struct
import redis
import logging
import os
from threading import Timer
import threading


r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

fmt = '%(asctime)s , %(levelname)s , %(filename)s %(funcName)s line %(lineno)s , %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S %a'
logging.basicConfig(level=logging.INFO,
format=fmt,
datefmt=datefmt,
filename="log.txt")


userName = 'Zz1234567'
password = 'Zz123456789'

flag = True

class RepeatingTimer(Timer):
    def run(self):
        while not self.finished.is_set():
            self.function(*self.args, **self.kwargs)
            self.finished.wait(self.interval)

def md5sign(data):
    try:
        md5 = hashlib.md5()
        md5.update(data.encode("utf-8"))
        return md5.hexdigest().upper()
    except Exception as e:
        print("error=%s", repr(e))

def on_message(ws, message):  # 服务器有数据更新时，主动推送过来的数据
    # print(message)
    if type(message) == str:
        message = json.loads(message)
        try:
            if message['data'] == 'Login successfully':
                logging.info('登录成功')
            elif message['data'] == 'Subscription success':
                logging.info('订阅成功')
            elif message['data'] == 'Connection Timeout':
                logging.info('连接超时')
            elif message['data'] == 'Success':
                logging.info('心跳发送成功 '+str(int(time.time())))
            elif message['data'] == 'Market duplicate subscription':
                logging.info('重复订阅')

        except:
            if message['event'] == 'ping':
                logging.info('连接超时')


    elif type(message) == bytes:
        # print(message)

        try:

            message = zlib.decompress(message)

            data = {}
            timeStamp = struct.unpack('i', message[0:4])[0]
            lastClose = struct.unpack('f', message[48:52])[0]
            open = struct.unpack('f', message[52:56])[0]
            high = struct.unpack('f', message[56:60])[0]
            low = struct.unpack('f', message[60:64])[0]
            newPrice = struct.unpack('f', message[64:68])[0]
            vol = struct.unpack('f', message[68:72])[0]
            askPrice = struct.unpack('f', message[116:120])[0]
            bidPrice = struct.unpack('f', message[76:80])[0]

            if newPrice > 0:
                data['id'] = timeStamp
                data['open'] = open
                data['low'] = low
                data['high'] = high
                data['close'] = newPrice
                data['vol'] = vol
                data['askPrice'] = askPrice
                data['bidPrice'] = bidPrice
                data['lastClose'] = lastClose

                symbol = ''
                for messages in message[4:16]:
                    if messages == 0:
                        pass
                    else:
                        symbol = symbol + chr(messages)

                data['symbol'] = symbol.lower()

                r.set('sub:' + symbol.lower() + ':1min', json.dumps(data))
                # print(r.get('sub:' + symbol.lower() + ':1min'))
                # print(symbol.lower(), data)

                global flag
                if flag:
                    # t2 = Timer(1.0, sendPing, (ws,))
                    t2 = RepeatingTimer(60, sendPing, (ws,))

                    t2.start()
                    flag = False


                # global t
                # if int(time.time()) - t > 10:
                #     t = int(time.time())
                #     timeStamp = str(int(time.time()))
                #     stringA = f'u={userName}&p={password}&stamp={timeStamp}'
                #
                #     signMd5 = md5sign(stringA)
                #
                #     # 心跳
                #     pingDict = '{"event":"ping","data":{"u":"' + userName + '","sign":"' + signMd5 + '","stamp":' + timeStamp + '}}'
                #     ws.send(pingDict)
                #     # logging.info('发送心跳'+str(t))
        except Exception as e:
            time.sleep(0.5)
            logging.info(e)

def sendPing(ws):
    t = int(time.time())
    timeStamp = str(int(time.time()))
    stringA = f'u={userName}&p={password}&stamp={timeStamp}'

    signMd5 = md5sign(stringA)

    # 心跳
    pingDict = '{"event":"ping","data":{"u":"' + userName + '","sign":"' + signMd5 + '","stamp":' + timeStamp + '}}'
    ws.send(pingDict)

    print(threading.currentThread())


def on_error(ws, error):  # 程序报错时，就会触发on_error事件

    time.sleep(60)

    logging.info(error)

    logging.info('调用shell脚本重启',)
    os.system('/root/lll/data/shuhai_qihuo/restartShuhai.sh')

    #发生错误时，客户端重启
    # startWS()


def on_close(ws):
    logging.info("Connection closed ……")

    logging.info('调用shell脚本重启', )
    os.system('/root/lll/data/shuhai_qihuo/restartShuhai.sh')

    # 当2分钟未收到数据，服务端断开连接，客户端重启
    # startWS()


def on_open(ws):  # 连接到服务器之后就会触发on_open事件，这里用于send数据

    timeStamp = str(int(time.time()))
    stringA = f'u={userName}&p={password}&stamp={timeStamp}'

    signMd5 = md5sign(stringA)


    # 账号登录
    loginDict = '{"event":"login","data":{"u":"' + userName + '","sign":"' + signMd5 + '","stamp":' + timeStamp + '}}'
    # loginDict = '{"event":"login","data":{"u":"mengde","sign":"C1F853B6329546381C0485402749AE2E","stamp":1588595729}}'
    ws.send(loginDict)

    # 订阅行情
    marketDict = '{"event":"subscribe","data":{"market":"HI,WX,HB,WE,WA","wlist":"CEDAXA0,HIHSIF,HIMHIF,CMGCA0,NECLA0,' \
                 'CENQA0,NENGA0,CEYMA0,HBbtcusdt,HBltcusdt,HBeosusdt,HBethusdt"}}'
    ws.send(marketDict)

    # # 心跳
    # pingDict = '{"event":"ping","data":{"u":"' + userName + '","sign":"' + signMd5 + '","stamp":' + timeStamp + '}}'
    # ws.send(pingDict)

    # 开始连接时间戳
    global t
    t = int(time.time())




def startWS():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp('ws://real.cnshuhai.com:17381/stock',
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close, )
    ws.on_open = on_open
    ws.run_forever(ping_timeout=30)


startWS()
