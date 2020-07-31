# -*- coding = utf-8  -*-
# @Time: 2020/5/11 17:46
# @Author: 李雷雷
# @File: shuhai_history.py


# K线、历史数据查询接口（type=kline）

import redis
import hashlib
import time
import requests
import json
import threading
from multiprocessing import Pool
import os

import logging
fmt = '%(asctime)s , %(levelname)s , %(filename)s %(funcName)s line %(lineno)s , %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S %a'
logging.basicConfig(level=logging.INFO,
format=fmt,
datefmt=datefmt,
filename="history_shuhai.txt")
logging.disable(logging.CRITICAL)     # 屏蔽打印

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

userName = 'mengde666'
password = 'mengde666'


def md5sign(data):
    try:
        md5 = hashlib.md5()
        md5.update(data.encode("utf-8"))
        return md5.hexdigest().lower()
    except Exception as e:
        print("error=%s", repr(e))


def getname(time1,time2):
    #多线程，每个货币对儿一个线程
    B = ['HIHSI07',   # 恒指2007
         'HIHSI08',   # 恒指2008
         'NECLU0',    # 美原油2009
         'HIMHI07',   # 小恒指2007
         'HIMHI08',   # 小恒指2008
         'CMGCQ0',    # 美黄金2008
         'CMGCV0',    # 美黄金2010
         'CMGCZ0',    # 美黄金2012
         'CEYMU0',    # 小道指2009
         'CENQU0',    # 小纳指2009
         'WGCNN0',    # 富时A5007
         'WGCNQ0',    # 富时A5008
         'SCni2010',  # 沪镍2010
         'SCau2012',  # 沪金2012
         'SFIF2008',  # 沪深30008
         'ZCSR2009',  # 白糖009
         'DCjd2101',  # 鸡蛋2101
         'DCjm2009',  # 焦煤2009
         'SCcu2009',  # 沪铜2009
         'SCru2009',  # 沪胶2009
         'DCm2009',   # 豆粕2009
         'SCrb2010',  # 螺纹2010
        'CEDAXA0', 'CENQA0','HIHSIF', 'HIMHIF', 'CMGCA0', 'NECLA0', 'NENGA0', 'CEYMA0']
        # 德指主连, 小纳指连续, 恒指连续, 小恒连续, 美黄金主力,美原油连续,美天然气连续,小道琼连续
    for symbol in B:
        t1 = threading.Thread(target=getdata, args=(symbol,time1,time2))
        t1.start()
    t1.join()

def getdata(symbol,time1,time2):

    while 1:
        url = 'http://shapi.market.alicloudapi.com/stock'

        t = time.time()
        timeStamp = str(int(t))
        stringA = f'u={userName}&p={password}&stamp={timeStamp}'
        signMd5 = md5sign(stringA)

        headers = {
            'Authorization': 'APPCODE ' + '47fd3f25581e461fb559d5a0f7a4426b',
            'x-ca-nonce': str(round(t * 1000))
        }

        params = {
            'u': userName,
            'type': 'kline',
            'sign': signMd5,
            'symbol': symbol,
            'stamp': int(timeStamp),
            'num': 300,
            'line':time1,
        }
        try:

            html = requests.get(url, params=params, headers=headers, timeout=5)
            if html.status_code != 200:
                continue
            results = html.text
            results = json.loads(results)

            dataList = list()

            for result in results:
                data = {}
                data['id'] = result['Date']
                data['symbol'] = result['Symbol']
                data['open'] = result['Open']
                data['low'] = result['Low']
                data['high'] = result['High']
                data['close'] = result['Close']
                data['vol'] = result['Volume']
                dataList.append(data)

            # print('market:'+symbol.lower()+':'+time2)
            r.set('market:' + symbol.lower() + ':' + time2, json.dumps(dataList))
            print(r.get('market:' + symbol.lower() + ':' + time2))
            time.sleep(60)
        except Exception as e:
            logging.info(e)
            time.sleep(30)
            logging.info('restart shuhai history!')
            # os.system('/root/lll/data/shuhai_qihuo/restart_history.sh')

if __name__ == '__main__':
    #多进程，每个时间段的行情一个进程
    A = {'min,1':'1min', 'min,5':'5min', 'min,15':'15min', 'min,30':'30min', 'min,60':'60min', 'day':'1day',}
    p = Pool(len(A))
    for i in A:
        p.apply_async(getname, args=(i,A.get(i)))
        print('进程' + i + '启动成功！')
    p.close()
    p.join()