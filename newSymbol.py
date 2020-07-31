# -*- coding=utf-8  -*-
# @Time: 2020/6/11 19:15
# @Author: LeiLei Li
# @File: newSymbol.py



newDict = {
    'hbbtcusdt':'btcusdt',
    'hbltcusdt':'ltcusdt',
    'hbeosusdt':'eosusdt',
    'hbethusdt':'ethusdt',
}


def getData(symbol):
    oldData = newDict.get(symbol)
    if not oldData:
        oldData = None

    return oldData

