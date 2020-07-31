#coding=utf-8
import json
import re
import time
from bs4 import BeautifulSoup
import pymysql
from userAgents import get_html,get_html_bytes
import datetime
import warnings
import requests


warnings.filterwarnings("ignore")

sf_daihao={'主 负':'1','主 胜':'2','0':'-2'}
rsf_daihao={'让分主负':'3','让分主胜':'4','0':'-2'}
dxf_daihao={'大':'5','小':'6','0':'-2'}
sfx_daihao={'客胜1-5':'7','客胜6-10':'8','客胜11-15':'9','客胜16-20':'10','客胜21-25':'11','客胜26+':'12',
            '主胜1-5':'13','主胜6-10':'14','主胜11-15':'15','主胜16-20':'16','主胜21-25':'17','主胜26+':'18','0':'-2'}

#定义数据库连接方式，方便三个项目数据同步。
# connjzc = pymysql.connect(host='47.75.42.189', port=3306, user='juzicai', password='MDBlYzg1YmJiMTM2YmY2OTllZjE3ZDY4',database='juzicai')
# cursorjzc = connjzc.cursor()

conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()
#http://info.sporttery.cn/basketball/pool_result.php?id=114758  详情

def str_time(a):
    a=time.mktime(time.strptime(a,'%Y-%m-%d %H:%M:%S'))
    a=int(a)
    return a

def submit(sql):

    cursor.execute(sql)
    conn.commit()


def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()



def bb_lottery(id1):

    url = "http://whjc.h148777.com/api/index/laQqQq/id/" + str(id1)
    response = requests.get(url)
    print(response.text)

def chuangjian_qm_bb_result_info():
    sql2="""CREATE TABLE IF NOT EXISTS qm_bb_result_info(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   time1 VARCHAR(30) comment '结果出现日期',
   saishi_name VARCHAR(30) comment '联赛名字',
   daihao VARCHAR(30) comment '赛事编号',
   zhu_ke_name VARCHAR(30) comment '主客队名字',
   bf VARCHAR(30) comment '比分',
   rf VARCHAR(30) comment '让分',
   time2 VARCHAR(30) comment '编号对应时间',
   state_bb VARCHAR(30) comment '篮球状态'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！qm_bb_result_info')

def chuangjian_qm_bb_result():
    sql2="""CREATE TABLE IF NOT EXISTS qm_bb_result(
    id  VARCHAR(30) PRIMARY KEY comment '球赛id',
    daihao VARCHAR(30) comment '赛事代号',
    open_time VARCHAR(30) comment '开赛时间',
    state_sf VARCHAR(30) comment '胜负赛果',
    sf_pl VARCHAR(30) comment '胜负赔率',
    state_rsf VARCHAR(30) comment '让球胜负赛果',
    rsf_pl VARCHAR(30) comment '让球胜负赔率',
    rsf_num VARCHAR(30) comment '让球数量',
    state_dxf VARCHAR(30) comment '大小分赛果',
    dxf_pl VARCHAR(30) comment '大小分赔率',
    dxf_num VARCHAR(30) comment '大小分预设总分',
    state_sfx VARCHAR(30) comment '胜分差赛果',
    sfx_pl VARCHAR(30) comment '胜分差赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！qm_bb_result')


def panduan(zhouji,a):
    week1 = {'0': '周一', '1': '周二', '2': '周三', '3': '周四', '4': '周五', '5': '周六', '6': '周日'}
    xing_qi_hao = time.strftime("%w", time.localtime(a))
    if week1[xing_qi_hao]==zhouji[0:2]:
        b=a+86400
        return b
    else:
        a=a-86400
        return panduan(zhouji, a)

def m_bb_result():
    url_zhu='http://info.sporttery.cn/basketball/match_result.php'
    html = get_html_bytes(url_zhu)
    if html:
        soup = BeautifulSoup(html, "lxml")
        s=soup.find_all('tr')
        for i in s:
            a=i.find_all('td')
            if len(a)==14:
                url = a[3].find('a').get('href')
                pat = '\=(.*)'
                results = re.compile(pat).findall(url)
                id0 = results[0]#

                # 获取联赛名字saishi_name
                saishi_name0 = a[2].get_text()

                #获取开赛代号daihao
                daihao0=a[1].get_text()

                #获取主队名字zhu_name
                zhu_ke_name0 = a[3].get_text()

                #比分
                bf0=a[7].get_text()

                #让分数
                rf0 = a[9].get_text()

                state_bb_text=a[11].get_text()
                if state_bb_text=='已完成':
                    state_bb0=1
                else:
                    state_bb0 = 0
                if bf0:

                #存入数据库。操作

                    url1='http://info.sporttery.cn/basketball/pool_result.php?id='+str(id0)
                    html = get_html_bytes(url1)
                    if html:
                        soup = BeautifulSoup(html, "lxml")
                        opentime1 = soup.find_all('div', {'class': 'c-time'})[0].get_text()
                        pat = '比赛开始时间(.*?)比赛场地'
                        results = re.compile(pat).findall(opentime1)
                        a = results[0].strip()
                        pat='(.*) '
                        t0=re.compile(pat).findall(a)[0]
                        time_award=datetime.date.today()
                        time20 = time.time()
                        time20 = panduan(daihao0, time20)
                        time20 = time.strftime("%Y-%m-%d", time.localtime(time20))
                        sql = "insert into qm_bb_result_info(id,time1,saishi_name,daihao,zhu_ke_name,bf,rf,time2,state_bb)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                              % (id0,time_award, saishi_name0, daihao0, zhu_ke_name0, bf0, rf0,time20,state_bb0)
                        print(sql)
                        try:
                            submit(sql)
                            print('插入成功！qm_bb_result_info', id0, daihao0)
                        except pymysql.err.IntegrityError as e:
                            sql2 = "update qm_bb_result_info set state_bb='%s'where id='%s'" % (state_bb0,id0)
                            submit(sql2)
                            print('更新成功！qm_bb_result_info', id0, daihao0)
                        open_time = str_time(a)
                        ss = soup.find_all('table', {'class': 'kj-table'})
                        # print(ss)
                        id1=id0
                        daihao1=daihao0
                        # print(daihao1)
                        open_time1=open_time
                        try:
                            state_sf1=ss[0].find_all('span', {'class': 'win'})[-1].get_text()
                            sf_pl1 = ss[0].find_all('td', {'class': 'cbgb'})[-1].get_text()
                        except IndexError as e:
                            # print(e)
                            state_sf1='0'
                            sf_pl1 =-2
                        try:
                            state_rsf1=ss[1].find_all('span', {'class': 'win'})[-1].get_text()
                            rsf_pl1=ss[1].find_all('td', {'class': 'cbgb'})[-1].get_text()
                        except IndexError as e:
                            state_rsf1 = '0'
                            rsf_pl1 = -2
                        try:
                            rsf_num1=ss[1].find_all('td', {'class': 'win'})[-1].get_text()
                            state_dxf1=ss[2].find_all('span', {'class': 'win'})[-1].get_text()
                        except IndexError as e:
                            rsf_num1 = '0'
                            state_dxf1 = -2
                        try:
                            dxf_pl1=ss[2].find_all('td', {'class': 'cbgb'})[-1].get_text()
                            dxf_num1=ss[2].find_all('td', {'class': 'win'})[-1].get_text()
                        except IndexError as e:
                            dxf_pl1 = '0'
                            dxf_num1 = -2
                        try:
                            state_sfx1=ss[3].find_all('span', {'class': 'win'})[-1].get_text()
                            sfx_pl1=ss[3].find_all('td', {'class': 'cbgb'})[-1].get_text()
                        except IndexError as e:
                            state_sfx1 = '0'
                            sfx_pl1 = -2

                        sql = "insert into qm_bb_result(id,daihao,open_time,state_sf,sf_pl,state_rsf,rsf_pl,rsf_num," \
                              "state_dxf,dxf_pl,dxf_num,state_sfx,sfx_pl)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s'," \
                              "'%s','%s','%s','%s','%s')" \
                          % (id1,daihao1,open_time1,sf_daihao[state_sf1],sf_pl1,rsf_daihao[state_rsf1],rsf_pl1,rsf_num1,
                             dxf_daihao[state_dxf1],
                             dxf_pl1,dxf_num1,
                             sfx_daihao[state_sfx1],
                             sfx_pl1)
                        try:
                            submit(sql)
                            print('插入成功！qm_bb_result', id1,daihao1)
                            bb_lottery(id1)
                        except pymysql.err.IntegrityError as e:
                            print('插入失败！qm_bb_result', e, id1,daihao1)
                            bb_lottery(id1)
    close_conn()
if __name__ == '__main__':
    chuangjian_qm_bb_result_info()
    chuangjian_qm_bb_result()
    m_bb_result()

    print('ok')