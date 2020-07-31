#!/usr/bin/python3
#coding: utf-8
from flask import Flask,jsonify
import json
import redis
import pymysql

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

app = Flask(__name__)


# app.config['JSON_AS_ASCII'] = False


@app.route('/python/getHeyueLists')
def get_heyue():

    newHeyues = {
        'HIHSI07' : '恒指2007',
        'HIHSI08' : '恒指2008',
        'NECLU0'  : '美原油2009',
        'HIMHI07' : '小恒指2007',
        'HIMHI08' : '小恒指2008',
        'CMGCQ0'  : '美黄金2008',
        'CMGCV0'  : '美黄金2010',
        'CMGCZ0'  : '美黄金2012',
        'CEYMU0'  : '小道指2009',
        'CENQU0'  : '小纳指2009',
        'WGCNN0'  : '富时A5007',
        'WGCNQ0'  : '富时A5008',
        'SFIF2008': '沪深30008',
        'ZCSR2009': '白糖009',
        'DCjd2101': '鸡蛋2101',
        'DCjm2009': '焦煤2009',
        'SCcu2009': '沪铜2009',
        'SCru2009': '沪胶2009',
        'DCm2009' : '豆粕2009',
        'SCrb2010': '螺纹2010',
        'SCau2012': '沪金2012',
        'SCni2010': '沪镍2010',
    }



    resultLists = {}
    newtimeDicts = {'1min': [1, 'M'], '5min': [5, 'M'], '15min': [15, 'M'], '30min': [30, 'M'], '60min': [60, 'M'],
                 '1day': [1, 'D']}
    for newHeyue in newHeyues:
        resultDict = {}
        resultDict['socket_Key'] = 'sub:' + newHeyue.lower() + ':1min'
        timeLists = list()
        for timeDict in newtimeDicts:
            timeData = {}
            urlDict = ['http://39.100.233.117:8989/symbol/',newHeyue.lower(),'/period/',timeDict,'/size/300']
            url = ''.join(urlDict)
            # timeData[timeDict] = f'http://39.100.233.117:8989/symbol/{newHeyue.lower()}/period/{timeDict}/size/300'
            timeData[timeDict] = url
            timeLists.append(timeData)
            resultDict['history_key'] = timeLists

        resultLists[newHeyues[newHeyue]] = resultDict
    # print(resultLists)
    return jsonify(resultLists)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9150)
    get_heyue()



