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

conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()


def submit(sql):
    cursor.execute(sql)
    conn.commit()


def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()


def fb_lottery(id):

    url = "http://whjc.h148777.com/api/index/lotteryResults/id/" + str(id)
    response = requests.get(url)
    print(response.text)


spf_daihao = {'胜':'1', '平':'2', '负':'3','取消':'55'}
rspf_daihao = {'胜': '4', '平': '5', '负': '6','取消':'55'}
bf_daihao = {'1:0': '7','2:0':'8','2:1':'9', '3:0':'10', '3:1':'11', '3:2':'12', '4:0':'13', '4:1':'14', '4:2':'15',
      '5:0':'16','5:1':'17', '5:2':'18', '胜其他':'19', '0:0':'20', '1:1':'21', '2:2':'22', '3:3':'23',
      '平其他':'24', '0:1':'25','0:2':'26', '1:2':'27', '0:3':'28', '1:3':'29', '2:3':'30', '0:4':'31',
      '1:4':'32', '2:4':'33', '0:5':'34', '1:5':'35','2:5':'36', '负其他':'37','取消':'55'}
zjq_daihao= { '0':'38', '1':'39', '2':'40','3':'41', '4':'42', '5':'43', '6':'44', '7+':'45','取消':'55'}
bqc_daihao= {'胜胜':'46','胜平':'47', '胜负':'48', '平胜':'49','平平':'50', '平负':'51', '负胜':'52', '负平':'53',
      '负负':'54','取消':'55'}

def chuangjian_qm_fb_result_info():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_result_info(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   time1 VARCHAR(30) comment '结果出现时间',
   daihao VARCHAR(30) comment '赛事编号',
   ls_name VARCHAR(30) comment '联赛',
   zk_name VARCHAR(30) comment '主客队',
   bc_result VARCHAR(30) comment '半场比分',
   qc_result VARCHAR(30) comment '全场比分',
   time2 VARCHAR(30) comment '编号对应时间',
   state_fb VARCHAR(30) comment '足球状态'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！qm_fb_result_info')

def panduan(zhouji,a):
    week1 = {'0': '周一', '1': '周二', '2': '周三', '3': '周四', '4': '周五', '5': '周六', '6': '周日'}
    xing_qi_hao = time.strftime("%w", time.localtime(a))
    if week1[xing_qi_hao]==zhouji[0:2]:
        b=a+86400
        return b
    else:
        a=a-86400
        return panduan(zhouji, a)

def chuangjian_qm_fb_result():
    sql2="""CREATE TABLE IF NOT EXISTS qm_fb_result(
    id  VARCHAR(30) PRIMARY KEY comment '球赛id',
    daihao VARCHAR(30) comment '赛事编号',
    spf_state VARCHAR(30) comment '胜平负赛果',
    spf_pl VARCHAR(30) comment '胜平负赔率',
    rspf_state VARCHAR(30) comment '让球胜平负赛果',
    rspf_pl VARCHAR(30) comment '让球胜平负赔率',
    bf_state VARCHAR(30) comment '比分赛果',
    bf_pl VARCHAR(30) comment '比分赔率',
    zjq_state VARCHAR(30) comment '总进球赛果',
    zjq_pl VARCHAR(30) comment '总进球赔率',
    bspf_state VARCHAR(30) comment '半场胜平负赛果',
    bspf_pl VARCHAR(30) comment '半场胜平负赔率'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    submit(sql2)
    print('创建成功！qm_fb_result')



def m_fb_result():
    #主页面列表
    for i in range(1,5):
        url_zhu='http://info.sporttery.cn/football/match_result.php?page='+str(i)
        html = get_html_bytes(url_zhu)
        print(html)
        soup = BeautifulSoup(html, "lxml")
        s=soup.find_all('tr')
        print(s)
        for i in s:
            a=i.find_all('td')
            if len(a)==12:
                #解析主页面内容，获取_info信息，主页面内容，及id


                try:
                    url = a[10].find('a').get('href')
                    pat = 'id\=(.*)'
                    results = re.compile(pat).findall(url)
                    id = results[0]
                    time_award = datetime.date.today()
                    time20=time.time()
                    time20= panduan(a[1].text, time20)
                    time20 = time.strftime("%Y-%m-%d", time.localtime(time20))
                    if a[9].text=='已完成':
                        state_fb0=1
                    else:
                        state_fb0 = 0

                    sql = "insert into qm_fb_result_info(id,time1,daihao,ls_name,zk_name,bc_result,qc_result,time2,state_fb)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                          % (id,time_award,a[1].text,a[2].text,a[3].text,a[4].text,a[5].text,time20,state_fb0)
                    try:
                        submit(sql)
                        print('插入成功！qm_fb_result_info', id, a[1].text)
                    except Exception as e:
                        pass
                        sql2 = "update qm_fb_result_info set state_fb='%s'where id='%s'" % (state_fb0, id)
                        submit(sql2)
                        print('更新成功！qm_fb_result_info', id, a[1].text)

                    #获取id后进行网址拼接，继续访问。获取分页面内容
                    t = time.time()
                    t = int(round(t * 1000))
                    url1 = 'http://i.sporttery.cn/api/fb_match_info/get_pool_rs/?f_callback=pool_prcess&mid=' + id + '&_=' + str(t)
                    html = get_html(url1)
                    pat = 'pool_prcess\((.*)\)'
                    results = re.compile(pat).findall(html)
                    k = json.loads(results[0])
                    try:
                        spf = k['result']['pool_rs']['had']
                        spf_state0=spf_daihao[spf['prs_name']]
                        spf_pl0=spf['odds']
                    except TypeError as e:
                        spf_state0 = 55
                        spf_pl0 = 1
                    except KeyError as e:
                        spf_state0 = -2
                        spf_pl0 = -2

                    print(id,a[1].text,str(k['result']['pool_rs']))
                    if len(str(k['result']['pool_rs']))>10:
                        # sql_log='insert into qm_fb_result_log(id,daihao,content)VALUES ("%s","%s","%s")' \
                        #       % (id,a[1].text,str(k['result']['pool_rs']))
                        # try:
                        #     cursorxmc.execute(sql_log)
                        #     connxmc.commit()
                        #     print('sql_log 成功')
                        # except pymysql.err.IntegrityError as e:
                        #     pass
                        try:
                            rspf = k['result']['pool_rs']['hhad']
                        except TypeError as e:
                            rspf = {'prs_name': '取消', 'odds': '1'}
                        except KeyError as e:
                            rspf = {'prs_name': '取消', 'odds': '1'}

                        try:
                            bf = k['result']['pool_rs']['crs']
                        except TypeError as e:
                            bf = {'prs_name': '取消', 'odds': '1'}
                        except KeyError as e:
                            bf = {'prs_name': '取消', 'odds': '1'}

                        try:
                            zjq = k['result']['pool_rs']['ttg']
                        except TypeError as e:
                            zjq = {'prs_name': '取消', 'odds': '1'}
                        except KeyError as e:
                            zjq = {'prs_name': '取消', 'odds': '1'}

                        try:
                            bq_spf = k['result']['pool_rs']['hafu']

                        except KeyError as e:
                            bq_spf = {'prs_name': '取消', 'odds': '1'}
                        except TypeError as e:
                            bq_spf = {'prs_name': '取消', 'odds': '1'}







                        sql = "insert into qm_fb_result(id,daihao,spf_state,spf_pl,rspf_state,rspf_pl,bf_state,bf_pl,zjq_state,zjq_pl,bspf_state,bspf_pl)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                              % (id, a[1].text, spf_state0, spf_pl0, rspf_daihao[rspf['prs_name']],
                                 rspf['odds'], bf_daihao[bf['prs_name']], bf['odds'], zjq_daihao[zjq['prs_name']]
                                 , zjq['odds'], bqc_daihao[bq_spf['prs_name']], bq_spf['odds'])
                        try:
                            submit(sql)
                            print('插入成功！qm_fb_result', id, a[1].text)
                            fb_lottery(id)
                        except Exception as e:
                            fb_lottery(id)
                            # pass
                            # print('插入失败！qm_fb_result',e, id, a[1].text)
                except AttributeError as e:
                    print(e)
    close_conn()
def del_result():
    sql1 = 'delete from qm_fb_result'
    submit(sql1)
    print('数据删除成功！')



if __name__ == '__main__':
    chuangjian_qm_fb_result_info()
    chuangjian_qm_fb_result()
    m_fb_result()

    print('ok')






