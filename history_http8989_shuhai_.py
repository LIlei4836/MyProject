#!/usr/bin/python3
#coding: utf-8
from flask import Flask
import json
import redis
import requests

from newSymbol import getData

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)


app = Flask(__name__)
@app.route('/symbol/<symbol>/period/<period>/size/<int:size>')
def get_hongbao(symbol,period,size):

    try:
        data = r.get('market:' + symbol + ':' + period)
        data = json.loads(data)
        data = data[0:int(size)]
        data = json.dumps(data)
    except Exception as e:

        # 内地服务器不存在，去香港服务器查询
        try:
            # 有别名时，查询原数据的历史数据
            if getData(symbol):
                symbol = getData(symbol)
            url = 'http://pythonhksj.wlnxx.top:8080/symbol/' + symbol + '/period/' + period + '/size/' + str(size)
            # print(url)
            html = requests.get(url, timeout=5)
            data = html.text
        except Exception as e:
            print(e)
            data = ''


        # 内地服务器查询英为才情期货
        if not data:
            try:
                url = 'http://pythonsj.wlnxx.top/python/symbol/' + symbol + '/period/' + period + '/size/'+ str(size)
                # print(url)#
                html = requests.get(url, timeout=5)
                data = html.text
            except Exception as e:
                print(e)
                data = ''
        print(1111111111111111111)

    return data

if __name__ == '__main__':
    # print('http://127.0.0.1:8080/symbol/btcusdt/period/15min/size/4')
    app.run(host='0.0.0.0',port=8989)
