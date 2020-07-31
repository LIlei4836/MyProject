# -*- coding = utf-8  -*-
# @Time: 2020/5/11 15:25
# @Author: 李雷雷
# @File: shuhai_http.py

# 实时行情查询接口（type=stock）
import redis
import hashlib
import time
import requests
import json
import threading
import os

import logging
fmt = '%(asctime)s , %(levelname)s , %(filename)s %(funcName)s line %(lineno)s , %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S %a'
logging.basicConfig(level=logging.INFO,
format=fmt,
datefmt=datefmt,
filename="http_ws.txt")

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


url = 'http://shapi.market.alicloudapi.com/stock'

reqSession = requests.Session()

def getdata():
    while 1:
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
            'type': 'stock',
            'sign': signMd5,
            'symbol': 'ZCSR2009,'  # 白糖009
                      'DCjd2101,'  # 鸡蛋2101
                      'DCjm2009,'  # 焦煤2009
                      'SCcu2009,'  # 沪铜2009
                      'SCru2009,'  # 沪胶2009
                      'DCm2009,'   # 豆粕2009
                      'SCrb2010,'  # 螺纹2010
                      'SFIF2007,'  # 沪深30007
                      'HIHSI07,'   # 恒指2007
                      'HIHSI08,'   # 恒指2008
                      'HIMHI07,'   # 小恒指2007
                      'HIMHI08,'   # 小恒指2008
                      'WGCNN0,'    # 富时A5007
                      'WGCNQ0,'    # 富时A5008
                      'SFIF2008,'  # 沪深30008
                      'NECLU0,'    # 美原油2009
                      'CMGCQ0,'    # 美黄金2008
                      'CMGCV0,'    # 美黄金2010
                      'CMGCZ0,'    # 美黄金2012
                      'CEYMU0,'    # 小道指2009
                      'CENQU0,'    # 小纳指2009
                      'SCni2010,'  # 沪镍2010
                      'SCau2012',  # 沪金2012
            'stamp': int(timeStamp),
            'column': 'Date,Symbol,Name,Open,High,Low,NewPrice,Volume,BP1,SP1'
            # BP1 买价, SP1 卖价
        }

        try:
            with requests.Session() as reqSession:
                html = reqSession.request('GET',url, params=params, headers=headers, timeout=3)
                if html.status_code != 200:
                    continue
                results = html.text
                results = json.loads(results)

                for result in results:
                    try:
                        if result.get('NewPrice') != 0:
                            data = {}
                            data['id'] = result['Date']
                            data['symbol'] = result['Symbol']
                            data['open'] = result['Open']
                            data['low'] = result['Low']
                            data['high'] = result['High']
                            data['close'] = result['NewPrice']
                            data['vol'] = result['Volume']
                            data['askPrice'] = result['SP1']
                            data['bidPrice'] = result['BP1']

                            symbol = result['Symbol'].lower()
                            # print('sub:' + symbol + ':1min')
                            r.set('sub:' + symbol + ':1min', json.dumps(data))
                            # print(r.get('sub:' + symbol + ':1min'))
                    except Exception as e:
                        logging.info(e)
                        pass
                time.sleep(0.01)
        except Exception as e:
            logging.info(e)
            time.sleep(0.1)
            pass


if __name__ == '__main__':
    getdata()


