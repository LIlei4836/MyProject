# 请使用3.6以上版本
import threading
from userAgents import get_html_bytes, get_html
import json
import time
import datetime
import pymysql
import contextlib
import requests
from multiprocessing import Pool

import logging


fmt = '%(asctime)s , %(levelname)s , %(filename)s %(funcName)s line %(lineno)s , %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S %a'
logging.basicConfig(level=logging.INFO,
format=fmt,
datefmt=datefmt,
filename="log.txt")


@contextlib.contextmanager
# def mysql(host='127.0.0.1', port=3306, user='root', passwd='', db='tkq1',charset='utf8'):
def mysql(host='103.80.18.238', port=3306, user='hhjc', passwd='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',
          db='hhjc', charset='utf8'):
# def mysql(host='localhost', port=3306, user='root', passwd='root',
#           db='huihuang', charset='utf8'):
    conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)  # 生成游标对象
    try:
        yield cursor
    finally:
        conn.commit()
        cursor.close()
        conn.close()


def create_database():
    with mysql() as cursor:
        sql = """CREATE TABLE IF NOT EXISTS qm_huihuangtest(
            id INT AUTO_INCREMENT PRIMARY KEY comment '主键',
            expect VARCHAR(20) comment '期望值',
            code VARCHAR(20) comment '彩种',
            openCode VARCHAR(20) comment '中奖号码',
            openTimeStamp int comment '中奖时间戳',
            nextExpect VARCHAR(20) comment '下一期期号',
            nextTimeStamp int comment '下期开奖时间戳')
            ENGINE = MyISAM DEFAULT CHARSET=utf8;"""
        cursor.execute(sql)
        cursor.execute("ALTER TABLE qm_huihuangtest ADD UNIQUE KEY(expect, code)")
        cursor.execute("ALTER TABLE qm_huihuangtest ADD UNIQUE KEY(nextExpect, nextTimeStamp, code)")
        print('数据库创建成功！')


def getData(variety):
    url = f'http://ho.apiplus.net/newly.do?token=tb986ae9a96e6052ak&code={variety}&format=json&rows=1'
    # url = f'http://ho.apiplus.net/daily.do?token=tb986ae9a96e6052ak&code={variety}&format=json&date=2020-05-09'
    while 1:
        # print(url)
        try:
            datas = get_html(url)
            if not datas:
                continue
            datas = json.loads(datas, encoding='utf-8')

            datas = datas['data']
            for data in reversed(datas):
                # print(code + '->', end='')
                expect = data['expect']      # 20200505002
                openCode = data['opencode']   # 3,5,6
                openTime = data['opentime']
                openTimeStamp = data['opentimestamp'] # 1588640124
                nextExpect = 0
                nextTimeStamp = 0

                periodDict = {'shk3': 41, 'jxk3': 42, 'hubk3': 39, 'fjk3': 42,
                              'sh11x5': 45, 'hub11x5': 40, 'zj11x5': 42, 'fj11x5': 45, 'jx11x5': 42}

                # 第一期倒计时开始时间
                openTimeDict = {'fjk3': '08:30:00', 'shk3': '08:38:00', 'jxk3': '08:15:00', 'hubk3': '09:00:00',
                                'sh11x5': '09:00:00',
                                'hub11x5': '08:35:00', 'zj11x5': '08:30:00', 'fj11x5': '08:10:00', 'jx11x5': '09:10:00'}
                if variety in periodDict and int(expect[-2:]) < periodDict[variety]:

                    nextExpect = int(expect) + 1

                    # 得到下一期开奖时间
                    tm_year, tm_mon, tm_mday, tm_hour, tmr_min = getDetailDate()

                    if int(tm_mon) < 10:
                        tm_mon = ('0' + str(tm_mon))[-2:]
                    if int(tm_mday) < 10:
                        tm_mday = ('0' + str(tm_mday))[-2:]

                    firstTime = str(tm_year) + '-' + str(tm_mon) + '-' + str(tm_mday) + ' ' + str(openTimeDict[variety])
                    timeArray = time.strptime(firstTime, "%Y-%m-%d %H:%M:%S")
                    now_time_stamp = time.mktime(timeArray)
                    nextTimeStamp = now_time_stamp + (int(expect[-2:])) * 1200+1200

                    # print(variety, int(time.time()), int(nextTimeStamp))
                    # print(int(nextTimeStamp) - int(time.time()))
                    # print(int(str(nextExpect)[-2:])-periodDict[variety])

                    # 延迟开奖
                    if int(int(nextTimeStamp)-int(time.time())) < 0:
                        print('下一期倒计时开始1:',variety,int(int(nextTimeStamp)-int(time.time())),nextExpect+1,nextTimeStamp+1200)
                        if int(str(nextExpect+1)[-2:]) <= periodDict[variety]:

                            # 不让上海十一选五最后一期延迟开奖后插入下一期数据，
                            if variety == 'sh11x5' and int((str(nextExpect+1))[-2:]) == 45:
                                currYear = expect[0:4]
                                currMonth = expect[4:6]
                                currDay = expect[6:8]

                                if int(currMonth) < 10:
                                    currMonth = ('0' + str(currMonth))[-2:]
                                if int(currDay) == 0:
                                    currDay = ('0' + str(currDay))[-2:]

                                currRiqi = str(currYear) + '-' + str(currMonth) + '-' + str(currDay)
                                date_time = datetime.datetime.strptime(currRiqi, '%Y-%m-%d')
                                date_time = date_time + datetime.timedelta(days=1)
                                nextTimeStamp = time.mktime(date_time.timetuple())-1200

                            insertData(variety, nextExpect+1, nextTimeStamp+1200)

                    # # 提前开奖
                    # elif int(int(nextTimeStamp)-int(time.time())) >1200:
                    #     print('下一期倒计时开始:', variety, int(int(nextTimeStamp)-int(time.time())), nextExpect ,
                    #           nextTimeStamp)
                    #     if int(str(nextExpect)[-2:]) <= periodDict[variety]:
                    #         insertData(variety, nextExpect, nextTimeStamp)


                elif variety in periodDict and int(expect[-2:]) == periodDict[variety]:

                    # 最后一期是凌晨12点之后开奖
                    if variety == 'sh11x5':

                        # 获取最后一期的开奖日期
                        openTimeArray = datetime.datetime.fromtimestamp(openTimeStamp)

                        year = openTimeArray.year
                        month = openTimeArray.month
                        day = openTimeArray.day

                        if int(month) < 10:
                            month = ('0' + str(month))[-2:]
                        if int(day) < 10:
                            day = ('0' + str(day))[-2:]

                        nextExpect = str(year)+str(month)+str(day)+'01'

                        # 12点之后开奖
                        if int(openTimeArray.hour) < 1:

                            currRiqi = str(year) + '-' + str(month) + '-' + str(day) + ' ' + str(openTimeDict[variety])

                            timeArray = time.strptime(currRiqi, "%Y-%m-%d %H:%M:%S")
                            nextTimeStamp = int(time.mktime(timeArray)) + 1200

                            insertData(variety, nextExpect, int(nextTimeStamp))
                        else:

                            # 最后一期在12点之前开奖
                            currYear = expect[0:4]
                            currMonth = expect[4:6]
                            currDay = expect[6:8]
                            currRiqi = str(currYear) + '-' + str(currMonth) + '-' + str(currDay)
                            date_time = datetime.datetime.strptime(currRiqi, '%Y-%m-%d')
                            date_time = date_time + datetime.timedelta(days=1)
                            year = date_time.year
                            month = date_time.month
                            day = date_time.day

                            if int(month) < 10:
                                month = ('0' + str(month))[-2:]
                            if int(day) < 10:
                                day = ('0' + str(day))[-2:]

                            nextqi = str(year) + str(month) + str(day)

                            nextRiqi = str(year) + '-' + str(month) + '-' + str(day)

                            # 获取每天第一期的期数
                            nextExpect = int(nextqi + '01')

                            # 获取明天第一期的时间戳
                            strTime = str(nextRiqi) + " " + str(openTimeDict[variety])
                            timeArray = time.strptime(strTime, "%Y-%m-%d %H:%M:%S")
                            nextTimeStamp = int(time.mktime(timeArray)) + 1200

                            insertData(variety, nextExpect, int(nextTimeStamp))

                    else:
                        # today = datetime.date.today()
                        # tomorrow = today + datetime.timedelta(days=1)

                        currYear = expect[0:4]
                        currMonth = expect[4:6]
                        currDay = expect[6:8]
                        currRiqi = str(currYear) + '-' + str(currMonth) + '-' + str(currDay)
                        date_time = datetime.datetime.strptime(currRiqi, '%Y-%m-%d')
                        date_time = date_time + datetime.timedelta(days=1)
                        year = date_time.year
                        month = date_time.month
                        day = date_time.day

                        if int(month) < 10:
                            month = ('0' + str(month))[-2:]
                        if int(day) < 10:
                            day = ('0' + str(day))[-2:]

                        nextqi = str(year) + str(month) + str(day)

                        nextRiqi = str(year)+'-'+str(month)+'-'+str(day)

                        # 获取每天第一期的期数
                        if 'k3' in variety:
                            nextExpect = int(nextqi + '001')
                        elif '11x5' in variety:
                            nextExpect = int(nextqi + '01')


                        # 获取明天第一期的时间戳
                        strTime = str(nextRiqi) + " " + str(openTimeDict[variety])
                        timeArray = time.strptime(strTime, "%Y-%m-%d %H:%M:%S")
                        nextTimeStamp = int(time.mktime(timeArray)) + 1200

                        insertData(variety, nextExpect, int(nextTimeStamp))

                        # 每天第一期提前倒计时
                        # if int(nextTimeStamp) - int(time.time()) < 1200:
                        #     insertData(variety, nextExpect,int(nextTimeStamp))
                        # elif (int(time.time()) - openTimeStamp) < 600:
                        #     nextExpect = 0
                        #     nextTimeStamp = 0
                        # else:
                        #     time.sleep(8)
                        #     continue

                    # 当第一期延迟开奖
                    # 获取当天日期
                    curr_year, curr_mon, curr_mday, curr_hour, curr_min = getDetailDate()


                    if int(curr_hour) < 10:
                        curr_hour = ('0' + str(curr_hour))[-2:]
                    if int(curr_min) == 0 or curr_min == '00':
                        curr_min = ('0' + str(curr_min))[-2:]

                    currentTime = str(curr_hour) + ':' + str(curr_min)

                    secondTimeDict = {'fjk3': '08:50:00', 'shk3': '08:58:00', 'jxk3': '08:35:00',
                                          'hubk3': '09:20:00',
                                          'sh11x5': '09:20:00',
                                          'hub11x5': '08:55:00', 'zj11x5': '08:50:00', 'fj11x5': '08:30:00',
                                          'jx11x5': '09:30:00'}

                    # 达到第一期开奖时时间
                    if currentTime == secondTimeDict[variety][0:5]:
                        # 得到第一期期号
                        if int(curr_mon) < 10:
                            curr_mon = ('0' + str(curr_mon))[-2:]
                        if int(curr_mday) < 10:
                            curr_mday = ('0' + str(curr_mday))[-2:]
                        if 'k3' in variety:
                            nextExpect = str(curr_year) + str(curr_mon) + str(curr_mday) + '002'
                        elif '11x5' in variety:
                            nextExpect = str(curr_year) + str(curr_mon) + str(curr_mday) + '02'


                        firstTime = str(curr_year) + '-' + str(curr_mon) + '-' + str(curr_mday) + ' ' + str(
                                secondTimeDict[variety])
                        currTimeArray = time.strptime(firstTime, "%Y-%m-%d %H:%M:%S")
                        nextTimeStamp = time.mktime(currTimeArray)

                        # 每天第一期提前倒计时
                        insertData(variety, nextExpect, int(nextTimeStamp) + 1200)


                areaDict = {'sh11x5': 'shanghai', 'hub11x5': 'hubei', 'zj11x5': 'zhejiang', 'fj11x5': 'fujian',
                            'jx11x5': 'jiangxi'}

                # print((expect, variety, openCode, openTimeStamp, nextExpect, nextTimeStamp))
                # continue
                with mysql() as cursor:

                    # num2 = 0
                    # if str(str(expect)[-2:]) == '01':
                    #
                    #     curr_year, curr_mon, curr_mday, curr_hour, curr_min = getDetailDate()
                    #
                    #     if int(curr_mon) < 10:
                    #         curr_mon = '0' + str(curr_mon)
                    #     if int(curr_mday) < 10:
                    #         curr_mday = '0' + str(curr_mday)
                    #
                    #     firstTime = str(curr_year) + '-' + str(curr_mon) + '-' + str(curr_mday) + ' ' + str(
                    #         openTimeDict[variety])
                    #     currTimeArray = time.strptime(firstTime, "%Y-%m-%d %H:%M:%S")
                    #     firstTimeStamp = int(time.mktime(currTimeArray))+1200
                    #
                    #     sql2 = "update qm_huihuangtest set expect='%s',openCode='%s',openTimeStamp='%d' where " \
                    #           "nextExpect='%s'AND nextTimeStamp='%s' AND code='%s'" % (
                    #           expect, openCode, openTimeStamp, expect, firstTimeStamp, variety)
                    #
                    #     try:
                    #         num2 = cursor.execute(sql2)
                    #     except Exception as e:
                    #         print(e)

                    sql = "update qm_huihuangtest set expect='%s',openCode='%s',openTimeStamp='%d' where " \
                          "nextExpect='%s'AND nextTimeStamp='%s' AND code='%s'"%(expect,openCode, openTimeStamp, nextExpect, nextTimeStamp, variety)

                    sql1 = "insert into qm_huihuangtest(expect,code,openCode,openTimeStamp,nextExpect,nextTimeStamp)" \
                          "values('%s','%s','%s','%d','%s','%s')" % (
                              expect, variety, openCode, openTimeStamp, nextExpect, nextTimeStamp)
                    try:
                        num = 0
                        num1 = 0


                        try:
                            num = cursor.execute(sql)
                        except Exception as e:
                            print(e)

                        if num == 0:
                            try:
                                num1 = cursor.execute(sql1)
                            except Exception as e:
                                print(e)

                        if num == 1:
                            print('更新成功！', variety, expect, num)
                        elif num1 == 1:
                            print('插入成功！', variety, expect, num1)
                        # elif num2 == 1:
                        #     print('第一期更新成功', variety, expect, num2)

                        if num == 1 or num1 == 1:
                            if '11x5' in variety:
                                area = areaDict[variety]
                                url_11x5 = ['http://whjc.h148777.com/api/index/epf/', 'expect/', str(expect), '/area/', area]
                                url_11x5 = ''.join(url_11x5)
                                try:
                                    html11x5 = requests.get(url_11x5)
                                    result = json.loads(html11x5.text)
                                    code = result['code']
                                    logging.info(variety+','+str(code)+','+url_11x5)
                                except Exception as e:
                                    logging.info(html11x5+','+url_11x5)
                                print(url_11x5)
                            elif 'k3' in variety:
                                url_k3 = ['http://whjc.h148777.com/api/ft/open/', 'code/', variety, '/outSn/', expect,'/nums/',openCode]
                                url_k3 = ''.join(url_k3)
                                try:
                                    htmlk3 = requests.get(url_k3)
                                    result = json.loads(htmlk3.text)
                                    code = result['code']
                                    logging.info(variety + ',' + str(code) + ',' + url_k3)
                                except Exception as e:
                                    logging.info(htmlk3+','+url_k3)
                                print(url_k3)
                    except Exception as e:
                        # print(e)
                        time.sleep(5)


        except Exception as e:
            print(e)
            time.sleep(4)

        time.sleep(4)

def getDetailDate():
    currDate = time.localtime()
    curr_year = currDate.tm_year
    curr_mon = currDate.tm_mon
    curr_mday = currDate.tm_mday
    curr_hour = currDate.tm_hour
    curr_min = currDate.tm_min
    return curr_year, curr_mon, curr_mday, curr_hour, curr_min

def insertData(variety, nextExpect,nextTimeStamp):
    with mysql() as cursor:
        sql = "insert into qm_huihuangtest(code, nextExpect,nextTimeStamp)" \
              "values('%s','%s','%d')" % (
                variety,nextExpect, nextTimeStamp)
        try:
            cursor.execute(sql)
        except:
            pass


if __name__ == '__main__':
    # create_database()
    varietys = [ 'jxk3', 'jx11x5', 'zj11x5','shk3','sh11x5','hubk3','hub11x5','fjk3','fj11x5']
    # varietys = [ 'sh11x5']
    for variety in varietys:
        t1 = threading.Thread(target=getData, args=(variety,))
        t1.start()
    t1.join()

