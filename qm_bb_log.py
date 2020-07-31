#coding=utf-8
from userAgents import get_html
import re
import random
import time
import pymysql
import json
import warnings




warnings.filterwarnings("ignore")


a = random.uniform(0, 1)
t = time.time()
t = int(round(t * 1000))

rand_a=round(a, 16)
url='http://info.sporttery.cn/interface/interface_mixed.php?action=bk_list&'+str(rand_a)+'&_='+str(t)
text=get_html(url)
pat='var data=(.*);getData\(\);'
results = re.compile(pat).findall(text)
a=results[0]
results=json.loads(a)
results=list(results)
result=results

# conn = pymysql.connect(host='127.0.0.1', port=3306, user='mibao', password='MGRlODIwN2VhNmRjM2Y3N2ZlNmYzMTVk',database='mibao')
conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()

def submit(sql):
    cursor.execute(sql)
    conn.commit()

def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()

#篮球胜负：
url1 = 'http://i.sporttery.cn/odds_calculator/get_odds?i_format=json&i_callback=getData&poolcode[]=mnl&_=' + str(
    t)
text1 = get_html(url1)
pat1 = 'getData\((.*)\);'
results1 = re.compile(pat1).findall(text1)
results1 = json.loads(results1[0])
print(results1)

#篮球让分胜负：
url2 = 'http://i.sporttery.cn/odds_calculator/get_odds?i_format=json&i_callback=getData&poolcode[]=hdc&_=' + str(
    t)
text2 = get_html(url2)
pat2 = 'getData\((.*)\);'
results2 = re.compile(pat2).findall(text2)
results2 = json.loads(results2[0])


#篮球胜分差：
url3 = 'http://i.sporttery.cn/odds_calculator/get_odds?i_format=json&i_callback=getData&poolcode[]=wnm&_' + str(
    t)
text3 = get_html(url3)
pat3 = 'getData\((.*)\);'
results3 = re.compile(pat3).findall(text3)
results3 = json.loads(results3[0])


#篮球大小分：
url4 = 'http://i.sporttery.cn/odds_calculator/get_odds?i_format=json&i_callback=getData&poolcode[]=hilo&_' + str(
    t)
text4 = get_html(url4)
pat4 = 'getData\((.*)\);'
results4 = re.compile(pat4).findall(text4)
results4 = json.loads(results4[0])





#传入时间戳返回周几，小时，
def get_xingqi_hour(a):
    xingqi=time.strftime("%w", time.localtime(a))
    xiaoshi=time.strftime("%H", time.localtime(a))
    fenzhong = time.strftime("%M", time.localtime(a))
    return int(xingqi),int(xiaoshi),int(a),int(fenzhong)

def get_lingcheg(a):
    a=int(a)
    b=a-(a - 1546790400) % 86400
    return b

#传入开始时间戳 返回截止购彩时间戳。
def end_time(a):
    #判断是，周几，几点
    week_num1,hour1,a,min1=get_xingqi_hour(a)
    #如果周一到周五9点到24点的比赛 提前10分钟，
    if week_num1==6 or week_num1==2 or week_num1==5 :
        if hour1<9:
            return (get_lingcheg(a)-600)
        else:
            return (a-600)
    elif week_num1==3 or week_num1==4:
        if hour1<=7 and min1<=30:
            return (get_lingcheg(a)-600)
        else:
            return (a - 600)
    else:
        if hour1<1:
            return (a-600)
        elif hour1<9:
            return (get_lingcheg(a) + 3300)
        else:
            return (a-600)
# 6月1日，到7月31日
def end_time2(a):
    # 判断是，周几，几点
    sql = "select bl from qm_configs where id=10"
    cursor.execute(sql)
    try:
        result = cursor.fetchone()
        b = int(result[0])
    except:
        b = 600
    week_num1, hour1, a,min1 = get_xingqi_hour(a)
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



def chuangjian_qm_bb_info():
    sql2="""CREATE TABLE IF NOT EXISTS qm_bb_info(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   saishi_name VARCHAR(30) comment '赛事名字',
   time1 VARCHAR(30) comment '比赛时间',
   kaisai_daihao VARCHAR(30) comment '比赛代号，周一001',
   zhu_name VARCHAR(30) comment '主队名字',
   ke_name VARCHAR(30) comment '客队名字',
   time2 VARCHAR(30) comment '代号日期',
   time3 VARCHAR(30) comment '截止购彩时间'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！bb_info')


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


def chuangjian_qm_bb_sf():
    sql2="""CREATE TABLE IF NOT EXISTS qm_bb_sf(
   id  VARCHAR(30) PRIMARY key comment '球赛id',
   kaisai_daihao VARCHAR(30) comment '比赛代号，周一001',
   state VARCHAR(30) comment '是否可单关 1：是，0：否',
   time1 VARCHAR(30) comment '赔率更新时间',
   s VARCHAR(30) comment '主胜赔率',
   f VARCHAR(30) comment '主负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！bb_sf')


def chuangjian_qm_bb_rsf():
    sql2 = """CREATE TABLE IF NOT EXISTS qm_bb_rsf(
   id  VARCHAR(30) PRIMARY key comment '球赛id',
   kaisai_daihao VARCHAR(30) comment '比赛代号，周一001',
   state VARCHAR(30) comment '是否可单关 1：是，0：否',
   time1 VARCHAR(30) comment '赔率更新时间',
   r_num VARCHAR(30) comment '主让客球数',
   s VARCHAR(30) comment '主胜赔率',
   f VARCHAR(30) comment '主负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！bb_rsf')


def chuangjian_qm_bb_dxf():
    sql2 = """CREATE TABLE IF NOT EXISTS qm_bb_dxf(
   id  VARCHAR(30) PRIMARY key comment '球赛id',
   kaisai_daihao VARCHAR(30) comment '比赛代号，周一001',
   state VARCHAR(30) comment '是否可单关 1：是，0：否',
   time1 VARCHAR(30) comment '赔率更新时间',
   zf VARCHAR(30) comment '总分数',
   d VARCHAR(30) comment '大分赔率',
   x VARCHAR(30) comment '小分赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！bb_dxf')


def chuangjian_qm_bb_sfx():
    sql2 = """CREATE TABLE IF NOT EXISTS qm_bb_sfx(
   id  VARCHAR(30) PRIMARY key comment '球赛id',
   kaisai_daihao VARCHAR(30) comment '比赛代号，周一001',
   state VARCHAR(30) comment '是否可单关 1：是，0：否',
   time1 VARCHAR(30) comment '赔率更新时间',
   a1_5 VARCHAR(30) comment '主胜1-5赔率',
   a6_10 VARCHAR(30) comment '主胜6-10赔率',
   a11_15 VARCHAR(30) comment '主胜11-15球赔率',
   a16_20 VARCHAR(30) comment '主胜16_20赔率',
   a21_25 VARCHAR(30) comment '主胜21_25赔率',
   a26_ VARCHAR(30) comment '主胜26+赔率',
   b1_5 VARCHAR(30) comment '主负1-5赔率',
   b6_10 VARCHAR(30) comment '主负6-10赔率',
   b11_15 VARCHAR(30) comment '主负11-15球赔率',
   b16_20 VARCHAR(30) comment '主负16_20赔率',
   b21_25 VARCHAR(30) comment '主负21_25赔率',
   b26_ VARCHAR(30) comment '主负26+赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！bb_sfx')


def str_time(a):
    a='20'+a
    a=time.mktime(time.strptime(a,'%Y-%m-%d %H:%M'))
    a=int(a)
    return a

#根据时间戳，返回月日 时分秒
def get_yerishijia(a):
    xingqi=time.strftime("%m-%d %H:%M:%S", time.localtime(a))
    return xingqi

def m_bb_log():
    # print(result)

    for i in range(len(result)):
        try:
            time20 = panduan(result[i][0][0], int(str_time(result[i][0][4])))
            sql = "insert into qm_bb_info(id,saishi_name,time1,kaisai_daihao,zhu_name,ke_name,time2,time3)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" \
                  % (result[i][0][5], result[i][0][1], str_time(result[i][0][4]), result[i][0][0], result[i][0][3],
                     result[i][0][2],time20,end_time2(str_time(result[i][0][4])))
            submit(sql)
            print('插入成功！qm_bb_info',result[i][0][0])

        except pymysql.err.IntegrityError as e:
            sql = "update qm_bb_info set saishi_name='%s',time1=%s,kaisai_daihao='%s',zhu_name='%s',ke_name='%s' where id=%s"%(result[i][0][1],
                 str_time(result[i][0][4]),result[i][0][0],result[i][0][3],result[i][0][2],result[i][0][5])
            submit(sql)
            # print('更新成功！qm_bb_info', result[i][0][0])

        try:
            sql = "insert into qm_bb_sf(id,kaisai_daihao,state,time1,s,f)VALUES ('%s','%s','%s','%s','%s','%s')" \
          % (result[i][0][5],result[i][0][0],results1['data']['_' +result[i][0][5]]['mnl']['single'],str(int(time.time())),result[i][1][1],result[i][1][0])

            print(sql)
            submit(sql)
            print('插入成功！qm_bb_sf',result[i][0][0])
        #当为上来就是暂停售票
        except (KeyError,TypeError) as e:
            sql = "insert into qm_bb_sf(id,kaisai_daihao,state,time1,s,f)VALUES ('%s','%s','%s','%s','%s','%s')" \
                  % (result[i][0][5], result[i][0][0], -2,
                     str(int(time.time())), -2, -2)

            print(sql)
            try:
                submit(sql)
                print('暂停售票！qm_bb_sf', result[i][0][0])
            except pymysql.err.IntegrityError:
                pass
        except pymysql.err.IntegrityError as e:
            try:
                sql2 = "update qm_bb_sf set state=%s,time1=%s,s=%s,f=%s where id =%s" % (results1['data']['_' +result[i][0][5]]['mnl']['single'],
                        int(time.time()),result[i][1][1],result[i][1][0],result[i][0][5])
                # print(e,result[i][0][0])
                submit(sql2)
                # print('更新成功！qm_bb_sf', result[i][0][0])
            except pymysql.err.ProgrammingError as e:
                sql2 = "update qm_bb_sf set state='%s',time1='%s',s='%s',f='%s' where id ='%s'" % (-2,
                                                                                            int(time.time()),
                                                                                            -2,
                                                                                            -2,
                                                                                         result[i][0][5])
                # print(e,result[i][0][0])
                submit(sql2)
                print('胜平负暂停售票！qm_bb_sf', result[i][0][0])







        try:
            sql = "insert into qm_bb_rsf(id,kaisai_daihao,state,time1,r_num,s,f)VALUES ('%s','%s','%s','%s','%s','%s','%s')" \
          % (result[i][0][5],result[i][0][0],results2['data']['_' +result[i][0][5]]['hdc']['single'],str(int(time.time())),result[i][2][0],result[i][2][2],result[i][2][1])
            submit(sql)
            print('插入成功！qm_bb_rsf',result[i][0][0])
        except pymysql.err.IntegrityError as e:
            try:
                sql2 = "update qm_bb_rsf set state=%s,time1=%s,r_num=%s,s=%s,f=%s where id =%s" % (results2['data']['_' +result[i][0][5]]['hdc']['single'],
                        int(time.time()),result[i][2][0],result[i][2][2],result[i][2][1],result[i][0][5])
                # print(e,result[i][0][0])
                submit(sql2)
                # print('更新成功！qm_bb_rsf', result[i][0][0])
            except pymysql.err.ProgrammingError as e:
                sql2 = "update qm_bb_rsf set state='%s',time1='%s',r_num='%s',s='%s',f='%s' where id ='%s'" % (results2['data']['_' +result[i][0][5]]['hdc']['single'],
                                                                                                       int(time.time()),
                                                                                                       -2,
                                                                                                       -2,
                                                                                                       -2,
                                                                                                       result[i][0][5])
                # print(e,result[i][0][0])
                submit(sql2)
                print('暂停售票！qm_bb_rsf', result[i][0][0])



        try:
            sql = "insert into qm_bb_dxf(id,kaisai_daihao,state,time1,zf,d,x)VALUES ('%s','%s','%s','%s','%s','%s','%s')" \
                  % (result[i][0][5], result[i][0][0], results4['data']['_' +result[i][0][5]]['hilo']['single'], str(int(time.time())), result[i][3][0],
                     result[i][3][1], result[i][3][2])
            submit(sql)
            # print('插入成功！qm_bb_dxf',result[i][0][0])
        except KeyError as e:
            print(e)
        except pymysql.err.IntegrityError as e:
            try:
                sql2 = "update qm_bb_dxf set state=%s,time1=%s,zf=%s,d=%s,x=%s where id =%s" % (results4['data']['_' +result[i][0][5]]['hilo']['single'],
                        int(time.time()),result[i][3][0],result[i][3][1],result[i][3][2],result[i][0][5])
                # print(sql2)
                submit(sql2)
                # print('更新成功！qm_bb_dxf', result[i][0][0])
            except pymysql.err.ProgrammingError as e:
                sql2 = "update qm_bb_dxf set state='%s',time1=%s,zf=%s,d='%s',x='%s' where id =%s" % (-2,
                                                                                                    int(time.time()),
                                                                                                    result[i][3][0],
                                                                                                    -2,
                                                                                                    -2,
                                                                                                    result[i][0][5])
                # print(sql2)
                submit(sql2)
                print('大小分暂停售票！qm_bb_dxf', result[i][0][0])



        try:
            sql = "insert into qm_bb_sfx(id,kaisai_daihao,state,time1,b1_5,b6_10,b11_15,b16_20,b21_25,b26_,a1_5,a6_10," \
                  "a11_15,a16_20,a21_25,a26_)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                  % (result[i][0][5], result[i][0][0], results3['data']['_' +result[i][0][5]]['wnm']['single'], str(int(time.time())), result[i][4][0],
                     result[i][4][1],
                     result[i][4][2], result[i][4][3], result[i][4][4], result[i][4][5], result[i][4][6],
                     result[i][4][7],
                     result[i][4][8], result[i][4][9], result[i][4][10], result[i][4][11])
            submit(sql)
            # print('插入成功！qm_bb_sfx',result[i][0][0])
        except KeyError as e:
            print(e)
        except pymysql.err.IntegrityError as e:
            try:
                sql2 = "update qm_bb_sfx set state=%s,time1=%s,b1_5=%s,b6_10=%s,b11_15=%s,b16_20=%s,b21_25=%s,b26_=%s,a1_5=%s,a6_10=%s," \
                  "a11_15=%s,a16_20=%s,a21_25=%s,a26_=%s where id =%s" % (results3['data']['_' +result[i][0][5]]['wnm']['single'],
                int(time.time()),result[i][4][0],result[i][4][1],result[i][4][2],result[i][4][3],result[i][4][4],result[i][4][5]
                ,result[i][4][6],result[i][4][7],result[i][4][8],result[i][4][9],result[i][4][10],result[i][4][11],result[i][0][5])
                submit(sql2)
                # print('更新成功！qm_bb_sfx', result[i][0][0])
            except pymysql.err.ProgrammingError as e:
                sql2 = "update qm_bb_sfx set state='%s',time1=%s,b1_5='%s',b6_10='%s',b11_15='%s',b16_20='%s',b21_25='%s',b26_='%s',a1_5='%s',a6_10='%s'," \
                       "a11_15='%s',a16_20='%s',a21_25='%s',a26_='%s' where id ='%s'" % (-2,
                                                                               int(time.time()), -2,
                                                                               -2, -2,
                                                                               -2, -2,
                                                                               -2, -2, -2,
                                                                               -2, -2,
                                                                               -2, -2,
                                                                               -2)
                submit(sql2)
                print('暂停售票！qm_bb_sfx', result[i][0][0])
    close_conn()


if __name__ == '__main__':
    chuangjian_qm_bb_sfx()
    chuangjian_qm_bb_dxf()
    chuangjian_qm_bb_rsf()
    chuangjian_qm_bb_sf()
    chuangjian_qm_bb_info()
    m_bb_log()
    print('ok')