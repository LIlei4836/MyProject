#http://info.sporttery.cn/interface/interface_mixed.php?action=fb_list&pke=0.7051481764546048&_=1545299746926
#coding=utf-8
from userAgents import get_html
import re
import random
import time
import pymysql
import json
import warnings


#根据时间戳，返回月日 时分秒
def get_yerishijia(a):
    xingqi=time.strftime("%m-%d %H:%M:%S", time.localtime(a))
    return xingqi


#传入时间戳返回周几，小时，
def get_xingqi_hour(a):
    xingqi=time.strftime("%w", time.localtime(a))
    xiaoshi=time.strftime("%H", time.localtime(a))
    return int(xingqi),int(xiaoshi),int(a)
#传入时间戳，返回当天凌晨时间戳
def get_lingcheg(a):
    a=int(a)
    b=a-(a - 1546790400) % 86400
    return b

#传入开始时间戳 返回截止购彩时间戳。
def end_time(a):
    #判断是，周几，几点
    week_num1,hour1,a=get_xingqi_hour(a)
    #如果周一到周五
    if week_num1>=1 and week_num1<=5:
        #如果周一
        if week_num1 == 1:
            #凌晨1点前开始比赛，截止购彩时间提前5分钟
            if hour1 < 1:
                return (a - 600)
            #9点前开始比赛，截止购彩时间为0:55
            elif hour1<9:
                return(get_lingcheg(a) + 3300)
            # 其他时间在比赛开始前5分钟截止购彩
            else:
                return (a-600)
        #如果周二到周五
        else:
            #如果9点前开始比赛，在前一天23:55分截止购彩
            if hour1<9:
                return (get_lingcheg(a) - 600)
            #其他时间比赛开始前5分钟截止购彩
            else:
                return (a - 600)
    else:
    # 如果周六
        if week_num1 == 6:
            #9点之前开赛，前一天23:55截止购彩
            if hour1<9:
                return (get_lingcheg(a)-600)
            #其他时间开赛前5分钟，
            else:
                return (a-600)
        #如果是周日
        else:
            #凌晨1:00前提前5分钟
            if hour1 < 1:
                return (a - 600)
            #1点后  9点前，0:55分截止购彩
            elif hour1 < 9:
                return (get_lingcheg(a) + 3300)
            #其他时间开始比赛前5分钟截止购彩
            else:
                return (a - 600)

# 6月1日，到7月31日
def end_time2(a):
    # 判断是，周几，几点
    sql = "select bl from qm_configs where id=9"
    cursor.execute(sql)
    try:
        result = cursor.fetchone()
        b = int(result[0])
    except:
        b = 600
    week_num1, hour1, a = get_xingqi_hour(a)
    # 如果周一到周五
    if week_num1 >= 1 and week_num1 <= 5:
        # 如果周一
        if week_num1 == 1:
            if hour1 < 9:
                return (get_lingcheg(a) - 3600)
            elif hour1<22:
                return (a - b)#******************************************这个600
            else:
                return(get_lingcheg(a)+79200)
        # 如果周二到周五
        else:
            if hour1 < 9:
                return (get_lingcheg(a) - 7200)
            elif hour1 < 22:
                return (a - b)#******************************************这个600
            else:
                return (get_lingcheg(a) + 79200)
    elif week_num1==6:
        # 如果周六
        if hour1 < 9:
            return (get_lingcheg(a) - 7200)
        # 其他时间开赛前5分钟，
        elif hour1 < 23:
            return (a - b)#******************************************这个600
        else:
            return (get_lingcheg(a) + 82800)
    else:
    # ,周日
        if hour1 < 9:
            return (get_lingcheg(a) - 3600)
        # 其他时间开赛前5分钟，
        elif hour1 < 23:
            return (a - b)#******************************************这个600
        else:
            return (get_lingcheg(a) + 82800)






#代号，比赛开始时间戳，返回
def panduan(zhouji,a):
    week1 = {'0': '周一', '1': '周二', '2': '周三', '3': '周四', '4': '周五', '5': '周六', '6': '周日'}
    xing_qi_hao = time.strftime("%w", time.localtime(a))
    if week1[xing_qi_hao]==zhouji[0:2]:
        b=a+86400
        day = time.strftime('%m-%d', time.localtime(b))
        return day
    else:
        a=a-86400
        return panduan(zhouji, a)

warnings.filterwarnings("ignore")


conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()


def submit(sql):
    cursor.execute(sql)
    conn.commit()


def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()

#字符串转化时间戳
def str_time(a):
    a='20'+a
    a=time.mktime(time.strptime(a,'%Y-%m-%d %H:%M'))
    a=int(a)
    return a

def chuangjian_qm_fb_info():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_info(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   saishi_name VARCHAR(30) comment '赛事名字',
   time1 VARCHAR(30) comment '比赛时间',
   kaisai_daihao VARCHAR(30) comment '比赛代号，周一001',
   zhu_name VARCHAR(30) comment '主队名字',
   ke_name VARCHAR(30) comment '客队名字',
   time2 VARCHAR(30) comment '代号日期',
   time3 VARCHAR(30) comment '截止购彩时间戳'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！fb_info')

def chuangjian_qm_fb_spf():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_spf(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   state VARCHAR(30) comment '可否单关 0：否 1：是',
   time1 VARCHAR(30) comment '赔率更新时间',
   s VARCHAR(30) comment '主胜赔率',
   p VARCHAR(30) comment '平赔率',
   f VARCHAR(30) comment '主负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！fb_spf')

def chuangjian_qm_fb_rspf():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_rspf(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   state VARCHAR(30) comment '可否单关 0：否 1：是',
   r_num VARCHAR(30) comment '主让客球数',
   time1 VARCHAR(30) comment '赔率更新时间',
   s VARCHAR(30) comment '主胜赔率',
   p VARCHAR(30) comment '平赔率',
   f VARCHAR(30) comment '主负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！fb_rspf')

def chuangjian_qm_fb_zjq():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_zjq(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   state VARCHAR(30) comment '可否单关 0：否 1：是',
   time1 VARCHAR(30) comment '赔率更新时间',
   a0 VARCHAR(30) comment '0球赔率',
   a1 VARCHAR(30) comment '1球赔率',
   a2 VARCHAR(30) comment '2球赔率',
   a3 VARCHAR(30) comment '3球赔率',
   a4 VARCHAR(30) comment '4球赔率',
   a5 VARCHAR(30) comment '5球赔率',
   a6 VARCHAR(30) comment '6球赔率',
   a7 VARCHAR(30) comment '7+球赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！fb_zjq')


def chuangjian_qm_fb_bqc():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_bqc(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   state VARCHAR(30) comment '可否单关 0：否 1：是',
   time1 VARCHAR(30) comment '赔率更新时间',
   ss VARCHAR(30) comment '胜胜赔率',
   sp VARCHAR(30) comment '胜平赔率',
   sf VARCHAR(30) comment '胜负赔率',
   ps VARCHAR(30) comment '平胜赔率',
   pp VARCHAR(30) comment '平平赔率',
   pf VARCHAR(30) comment '平负赔率',
   fs VARCHAR(30) comment '负胜赔率',
   fp VARCHAR(30) comment '负平赔率',
   ff VARCHAR(30) comment '负负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！fb_bqc')


def chuangjian_qm_fb_bf():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_bf(
   id  VARCHAR(30) PRIMARY KEY  comment '球赛id',
   state VARCHAR(30) comment '可否单关 0：否 1：是',
   time1 VARCHAR(30) comment '赔率更新时间',
   a0100 VARCHAR(30) comment '0100赔率',
   a0200 VARCHAR(30) comment '0200赔率',
   a0201 VARCHAR(30) comment '0201赔率',
   a0300 VARCHAR(30) comment '0300赔率',
   a0301 VARCHAR(30) comment '0301赔率',
   a0302 VARCHAR(30) comment '0302赔率',
   a0400 VARCHAR(30) comment '0400赔率',
   a0401 VARCHAR(30) comment '0401赔率',
   a0402 VARCHAR(30) comment '0402赔率',
   a0500 VARCHAR(30) comment '0500赔率',
   a0501 VARCHAR(30) comment '0501赔率',
   a0502 VARCHAR(30) comment '0502赔率',
   as0 VARCHAR(30) comment '其他胜赔率',
   b0000 VARCHAR(30) comment '0000赔率',
   b0101 VARCHAR(30) comment '0101赔率',
   b0202 VARCHAR(30) comment '0202赔率',
   b0303 VARCHAR(30) comment '0303赔率',
   bp0 VARCHAR(30) comment '其他平赔率',
   c0001 VARCHAR(30) comment '0001赔率',
   c0002 VARCHAR(30) comment '0002赔率',
   c0102 VARCHAR(30) comment '0102赔率',
   c0003 VARCHAR(30) comment '0003赔率',
   c0103 VARCHAR(30) comment '0103赔率',
   c0203 VARCHAR(30) comment '0203赔率',
   c0004 VARCHAR(30) comment '0004赔率',
   c0104 VARCHAR(30) comment '0104赔率',
   c0204 VARCHAR(30) comment '0204赔率',
   c0005 VARCHAR(30) comment '0005赔率',
   c0105 VARCHAR(30) comment '0105赔率',
   c0205 VARCHAR(30) comment '0205赔率',
   cf0 VARCHAR(30) comment '其他负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！fb_bf')

def m_fb_log():
    a = random.uniform(0, 1)
    t = time.time()
    t = int(round(t * 1000))
    rand_a = round(a, 16)
    url = 'http://info.sporttery.cn/interface/interface_mixed.php?action=fb_list&pke=' + str(rand_a) + '&_=' + str(t)
    print(url)
    text = get_html(url)
    pat = 'var data=(.*);getData\(\);'
    results = re.compile(pat).findall(text)
    a = results[0]
    results = json.loads(a)

    #获取让球个数及单关信息
    url2 = 'http://i.sporttery.cn/odds_calculator/get_odds?i_format=json&i_callback=getData&poolcode[]=hhad&poolcode[]=had&_=' + str(t)
    text2 = get_html(url2)
    pat2 = 'getData\((.*)\);'
    results2 = re.compile(pat2).findall(text2)
    results2 = json.loads(results2[0])

    for i in results:
        try:
            time20 = panduan(i[0][0], int(str_time(i[0][3])))
            sql = "insert into qm_fb_info(id,saishi_name,time1,kaisai_daihao,zhu_name,ke_name,time2,time3)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (i[0][4], i[0][1], str_time(i[0][3]), i[0][0], i[0][7], i[0][8],time20,end_time2(str_time(i[0][3])))
            print(sql)
            submit(sql)
            print('插入成功！qm_fb_info', i[0][0])
        except pymysql.err.IntegrityError as e:
            sql = "update qm_fb_info set saishi_name='%s',time1=%s,kaisai_daihao='%s',zhu_name='%s',ke_name='%s' where id=%s" % (i[0][1], str_time(i[0][3]), i[0][0], i[0][7], i[0][8], i[0][4])
            submit(sql)
            print('更新成功！qm_fb_info', i[0][0])

        #赔率插入
        try:
            sql_spf = "insert into qm_fb_spf(id,state,time1,s,p,f)VALUES ('%s','%s','%s','%s','%s','%s')" \
                      % (i[0][4], results2['data']['_' + i[0][4]]['had']['single'], int(time.time()), i[5][0], i[5][1],
                         i[5][2])
            submit(sql_spf)
            print('插入成功！qm_fb_spf',i[0][0])
        #赔率更新
        except pymysql.err.IntegrityError as e:
            sql_spf = "update qm_fb_spf set state=%s,time1=%s,s=%s,p=%s,f=%s where id =%s" % (results2['data']['_' + i[0][4]]['had']['single'], int(time.time()), i[5][0], i[5][1],
                         i[5][2],i[0][4])

            submit(sql_spf)
            print('胜平负赔率更新完毕', i[0][0])
        # 停止售票报错KeyError，此时id,及当前时间戳更新。其他赔率信息为-2
        except KeyError as e:
            try:
                sql_spf = "insert into qm_fb_spf(id,state,time1,s,p,f)VALUES ('%s','%s','%s','%s','%s','%s')"  % (i[0][4], -2, int(time.time()), -2, -2, -2)
                submit(sql_spf)
                print('胜平负暂停售票',i[0][0])
            except pymysql.err.IntegrityError as e:
                pass


        try:
            sql_rspf = "insert into qm_fb_rspf(id,state,r_num,time1,s,p,f)VALUES ('%s','%s','%s','%s','%s','%s','%s')" \
                       % (i[0][4], results2['data']['_' + i[0][4]]['hhad']['single'],
                          results2['data']['_' + i[0][4]]['hhad']['fixedodds'], int(time.time()), i[1][0], i[1][1],
                          i[1][2])
            submit(sql_rspf)
            print('插入成功！qm_fb_rspf',i[0][0])
        except pymysql.err.IntegrityError as e:
            qm_fb_rspf = "update qm_fb_rspf set state=%s,r_num=%s,time1=%s,s=%s,p=%s,f=%s where id =%s" % (
                results2['data']['_' + i[0][4]]['hhad']['single'],
                results2['data']['_' + i[0][4]]['hhad']['fixedodds'], int(time.time()), i[1][0], i[1][1],
                i[1][2],i[0][4])
            submit(qm_fb_rspf)
            print('让球胜平负赔率更新完毕', i[0][0])

        # 停止售票报错KeyError，此时id,及当前时间戳更新。其他赔率信息为-2
        except KeyError as e:
            sql_rspf = "update qm_fb_rspf set state=%s,r_num=%s,time1=%s,s=%s,p=%s,f=%s where id =%s" % (
                -2,-2, int(time.time()), -2,-2,-2,i[0][4])
            submit(sql_rspf)
            print('让球胜平负暂停售票', i[0][0])

        try:
            sql_zjq = "insert into qm_fb_zjq(id,state,time1,a0,a1,a2,a3,a4,a5,a6,a7)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                  % (i[0][4], i[3][8], int(time.time()),i[3][0],i[3][1],i[3][2],i[3][3],i[3][4],i[3][5],i[3][6],i[3][7])
            # print(sql_zjq)
            submit(sql_zjq)
            print('插入成功！qm_sql_zjq',i[0][0])
        except pymysql.err.IntegrityError as e:
            qm_sql_zjq = "update qm_fb_zjq set state=%s,time1=%s,a0=%s,a1=%s,a2=%s,a3=%s,a4=%s,a5=%s,a6=%s,a7=%s where id =%s" % (
                i[3][8], int(time.time()), i[3][0], i[3][1], i[3][2], i[3][3], i[3][4], i[3][5], i[3][6], i[3][7],i[0][4])
            submit(qm_sql_zjq)
            # print('总进球胜平负更新完毕', i[0][0])

        # 停止售票报错KeyError，此时id,及当前时间戳更新。其他赔率信息为-2
        except KeyError as e:
            sql_zjq = "update qm_fb_zjq set state=%s,time1=%s,a0=%s,a1=%s,a2=%s,a3=%s,a4=%s,a5=%s,a6=%s,a7=%s where id =%s" % (
                -2, int(time.time()), -2, -2, -2, -2, -2, -2,-2, -2,-2)
            submit(sql_zjq)
            print('总进球暂停售票',i[0][0])

        try:
            sql_bqc = "insert into qm_fb_bqc(id,state,time1,ss,sp,sf,ps,pp,pf,fs,fp,ff)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                      % (
                      i[0][4], i[4][9], int(time.time()), i[4][0], i[4][1], i[4][2], i[4][3], i[4][4], i[4][5], i[4][6],
                      i[4][7], i[4][8])
            # print(sql_bqc)

            submit(sql_bqc)
            print('插入成功！qm_sql_bqc',i[0][0])
        except pymysql.err.IntegrityError as e:
            sql_bqc = "update qm_fb_bqc set state=%s,time1=%s,ss=%s,sp=%s,sf=%s,ps=%s,pp=%s,pf=%s,fs=%s,fp=%s,ff=%s where id=%s"%(
                i[4][9], int(time.time()), i[4][0], i[4][1], i[4][2], i[4][3], i[4][4], i[4][5], i[4][6],
                i[4][7], i[4][8],i[0][4])
            # print(sql_bqc)
            submit(sql_bqc)
            # print('更新成功！qm_sql_bqc', i[0][0])
        # 停止售票报错KeyError，此时id,及当前时间戳更新。其他赔率信息为-2
        except KeyError as e:
            sql_bqc = "update qm_fb_bqc set state=%s,time1=%s,ss=%s,sp=%s,sf=%s,ps=%s,pp=%s,pf=%s,fs=%s,fp=%s,ff=%s where id=%s" % (
                -2, int(time.time()), -2, -2, -2, -2, -2, -2, -2, -2, -2, -2)
            submit(sql_bqc)
            print('半全场暂停售票',i[0][0])

        try:
            sql_bf = "insert into qm_fb_bf(id,state,time1,a0100,a0200,a0201,a0300,a0301,a0302,a0400,a0401,a0402,a0500,a0501" \
                     ",a0502,as0,b0000,b0101,b0202,b0303,bp0,c0001,c0002,c0102,c0003,c0103,c0203,c0004,c0104,c0204,c0005," \
                     "c0105,c0205,cf0)VALUES " \
                     "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
                     "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                     % (i[0][4], i[2][31],
                        int(time.time()), i[2][0],
                        i[2][1],
                        i[2][2], i[2][3], i[2][4],
                        i[2][5], i[2][6], i[2][7], i[2][8],
                        i[2][9], i[2][10], i[2][11], i[2][12], i[2][13], i[2][14], i[2][15], i[2][16], i[2][17],
                        i[2][18], i[2][19], i[2][20], i[2][21],
                        i[2][22], i[2][23], i[2][24], i[2][25], i[2][26], i[2][27], i[2][28], i[2][29], i[2][30])
            # print(sql_bf)
            submit(sql_bf)
            print('插入成功！qm_fb_bf', i[0][0])
        except pymysql.err.IntegrityError as e:
            sql_bf = "update qm_fb_bf set state=%s,time1=%s,a0100=%s,a0200=%s,a0201=%s,a0300=%s,a0301=%s,a0302=%s,a0400=%s,a0401=%s,a0402=%s,a0500=%s,a0501=%s" \
                     ",a0502=%s,as0=%s,b0000=%s,b0101=%s,b0202=%s,b0303=%s,bp0=%s,c0001=%s,c0002=%s,c0102=%s,c0003=%s,c0103=%s,c0203=%s,c0004=%s,c0104=%s,c0204=%s,c0005=%s," \
                     "c0105=%s,c0205=%s,cf0=%s where id= %s" %\
                     (i[2][31],int(time.time()), i[2][0],i[2][1],i[2][2], i[2][3], i[2][4],i[2][5], i[2][6], i[2][7], i[2][8],
                        i[2][9], i[2][10], i[2][11], i[2][12], i[2][13], i[2][14], i[2][15], i[2][16], i[2][17],i[2][18],
                      i[2][19], i[2][20], i[2][21],i[2][22], i[2][23], i[2][24], i[2][25], i[2][26], i[2][27], i[2][28],
                      i[2][29], i[2][30],i[0][4])
            print(sql_bf)
            submit(sql_bf)
            print('更新成功！qm_fb_bf', i[0][0])
        # 停止售票报错KeyError，此时id,及当前时间戳更新。其他赔率信息为-2
        except KeyError as e:
            sql_bf = "update qm_fb_bf set state=%s,time1=%s,a0100=%s,a0200=%s,a0201=%s,a0300=%s,a0301=%s,a0302=%s,a0400=%s,a0401=%s,a0402=%s,a0500=%s,a0501=%s" \
                     ",a0502=%s,as0=%s,b0000=%s,b0101=%s,b0202=%s,b0303=%s,bp0=%s,c0001=%s,c0002=%s,c0102=%s,c0003=%s,c0103=%s,c0203=%s,c0004=%s,c0104=%s,c0204=%s,c0005=%s," \
                     "c0105=%s,c0205=%s,cf0=%s where id= %s" % \
                     (-2, int(time.time()),-2, -2, -2, -2, -2, -2, -2, -2,
                      -2,-2, -2, -2, -2, -2, -2, -2, -2,
                      -2, -2, -2, -2, -2, -2, -2, -2,
                      -2, -2, -2, -2, -2, -2,i[0][4])
            print(sql_bf)
            submit(sql_bf)
            print('比分暂停售票', e,i[0][0])
    close_conn()
if __name__ == '__main__':
    chuangjian_qm_fb_info()
    chuangjian_qm_fb_spf()
    chuangjian_qm_fb_rspf()
    chuangjian_qm_fb_zjq()
    chuangjian_qm_fb_bqc()
    chuangjian_qm_fb_bf()
    m_fb_log()

    print('ok')