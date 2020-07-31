import time
from userAgents import get_html
import json
import re
import pymysql

conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()
def chuangjian():
    sql1 = """CREATE TABLE IF NOT EXISTS qm_basketball_from(id int(10) not null AUTO_INCREMENT,m_id VARCHAR(30) comment '比赛id',l_cn_abbr  VARCHAR(30) comment '联赛',a_cn VARCHAR(30) comment '客场',h_cn VARCHAR(30) comment '主场',date_cn VARCHAR(30) comment '日期',final VARCHAR(30) comment '比分',bifen VARCHAR(30) comment '胜负',name VARCHAR(30),PRIMARY KEY (`id`),UNIQUE KEY `m_id_data_cn` (`m_id`,`final`,`name`)) ;"""
    cursor.execute(sql1)
    print('创建成功！score_fb')
    conn.commit()
def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()

def get_bs_score():
    t = int(time.time()*1000)
    m_url = "https://i.sporttery.cn/odds_calculator/get_odds?i_format=json&i_callback=getData&poolcode[]=mnl&_="+str(t)
    m_resp = get_html(m_url)
    m_resp = re.compile(r'getData\((.*)\)').findall(m_resp)
    m_resp = json.loads(m_resp[0])
    for m in m_resp['data']:
        print(m)
        m = m[1:]

        tid_url = "https://i.sporttery.cn/api/bk_match_info/get_match_info/?mid={}&f_callback=match_place&_=".format(str(m),str(t))
        tid_resp = get_html(tid_url)
        tid_resp = re.compile(r'match_place\((.*)\)').findall(tid_resp)
        tid_resp = json.loads(tid_resp[0])
        tid = tid_resp['result']['h_id_dc']

        data_url = "https://i.sporttery.cn/api/bk_match_info/get_team_rec_data/?tid={}&_={}".format(str(tid),str(t))
        data_resp = get_html(data_url)
        data_resp = json.loads(data_resp)
        data_list = data_resp['result']['data']
        for data in data_list:

            data['final'] = str(data['fs_A'])+"-"+str(data['fs_B'])
            if int(data['fs_A']) > int(data['fs_B']):
                data["bifen"] = "胜"
            elif int(data['fs_A']) == int(data['fs_B']):
                data["bifen"] = "平"
            else:
                data["bifen"] = "负"
            if data['is_home'] == 1:
                data['name'] = "home"
            elif data['is_home'] == 0:
                data['name'] = "away"
            else:
                data['name'] = "history"
# #
# #                 # print(data['name'])
# #                 # print(m)
# #                 # print(data['l_cn_abbr'])  # 联赛
# #                 # print(data['a_cn_abbr'])  # 客场
# #                 # print(data['h_cn_abbr'])  # 主场
# #                 # print(data['date_cn'])  # 时间
# #                 # print(data['final'])  # 比分
# #                 # print(data['bifen'])
# #
            try:
                sql2 = "insert into qm_basketball_from(m_id,l_cn_abbr,a_cn,h_cn,date_cn,final,bifen,name)VALUES('%s','%s','%s','%s','%s','%s','%s','%s')"%(m,data['l_cn_abbr'],data['a_cn_abbr'],data['h_cn_abbr'],data['date_cn'],data['final'],data['bifen'],data['name'])
                cursor.execute(sql2)
                conn.commit()
                # print('插入成功!1')
            except Exception as e:
                sql3 = "update qm_basketball_from set final='%s',bifen='%s' where m_id ='%s' and date_cn='%s' and name = '%s'" % (data['final'],data['bifen'],m,data['date_cn'],data['name'])
                cursor.execute(sql3)
                conn.commit()
                # print('更新成功1')
            try:
                sql4 = "insert into qm_basketball_from(m_id,l_cn_abbr,a_cn,h_cn,date_cn,final,bifen,name)VALUES('%s','%s','%s','%s','%s','%s','%s','%s')"%(m,data['l_cn_abbr'],data['a_cn_abbr'],data['h_cn_abbr'],data['date_cn'],data['final'],data['bifen'],'history')
                cursor.execute(sql4)
                conn.commit()
                # print('插入成功!2')
            except Exception as e:
                sql5 = "update qm_basketball_from set final='%s',bifen='%s' where m_id ='%s' and date_cn='%s' and name = '%s'" % (data['final'],data['bifen'],m,data['date_cn'],data['name'])
                cursor.execute(sql5)
                conn.commit()
                # print('更新成功2')


def get_fb_score():
    t = int(time.time() * 1000)
    m_url = "https://i.sporttery.cn/odds_calculator/get_proportion?i_format=json&pool[]=had&pool[]=hhad&i_callback=getReferData1&_=" + str(
        t)
    m_resp = get_html(m_url)
    m_resp = re.compile(r'getReferData1\((.*)\)').findall(m_resp)
    m_resp = json.loads(m_resp[0])
    for m in m_resp['data']:
        m = m[1:]
        tid_url = "https://i.sporttery.cn/api/fb_match_info/get_result_his?limit=10&is_ha=all&limit=10&c_id=0&mid={}&ptype[]=three_-1&f_callback=getResultHistoryInfo&_={}".format(
            str(m), str(t))
        tid_resp = get_html(tid_url)
        tid_resp = re.compile(r'getResultHistoryInfo\((.*)\)').findall(tid_resp)
        tid_resp = json.loads(tid_resp[0])
        tid = tid_resp['result']['data'][0]['h_id_dc']
        data_url = "https://i.sporttery.cn/api/fb_match_info/get_team_rec_data?tid={}&_={}".format(str(tid), str(t))
        data_resp = get_html(data_url)
        data_resp = json.loads(data_resp)
        data_list = data_resp['result']['data']
        for data in data_list:
            if data['final'] == '':
                print(data['final'])
            else:
                one = str(data["final"]).split(':')[0]
                two = str(data["final"]).split(':')[1]

                if int(one) > int(two):
                    data["bifen"] = "胜"
                elif int(one) == int(two):
                    data["bifen"] = "平"
                else:
                    data["bifen"] = "负"
                if data['is_home'] == 1:
                    data['name'] = "home"
                elif data['is_home'] == 0:
                    data['name'] = "away"
                else:
                    data['name'] = "history"

                # print(data['name'])
                # print(m)
                # print(data['l_cn_abbr'])  # 联赛
                # print(data['a_cn_abbr'])  # 客场
                # print(data['h_cn_abbr'])  # 主场
                # print(data['date_cn'])  # 时间
                # print(data['final'])  # 比分
                # print(data['bifen'])

                try:
                    sql2 = "insert into qm_football_from(m_id,l_cn_abbr,a_cn_abbr,h_cn_abbr,date_cn,final,bifen,name)VALUES('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    m, data['l_cn_abbr'], data['a_cn_abbr'], data['h_cn_abbr'], data['date_cn'], data['final'],
                    data['bifen'], data['name'])
                    cursor.execute(sql2)
                    conn.commit()
                    # print('插入成功!1')

                except Exception as e:
                    sql3 = "update qm_football_from set final='%s',bifen='%s' where m_id ='%s' and date_cn='%s' and name = '%s'" % (
                    data['final'], data['bifen'], m, data['date_cn'], data['name'])
                    cursor.execute(sql3)
                    conn.commit()
                    # print('更新成功1')

                try:
                    sql4 = "insert into qm_football_from(m_id,l_cn_abbr,a_cn_abbr,h_cn_abbr,date_cn,final,bifen,name)VALUES('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                    m, data['l_cn_abbr'], data['a_cn_abbr'], data['h_cn_abbr'], data['date_cn'], data['final'],
                    data['bifen'], 'history')
                    cursor.execute(sql4)
                    conn.commit()
                    # print('插入成功!2')

                except Exception as e:
                    sql5 = "update qm_football_from set final='%s',bifen='%s' where m_id ='%s' and date_cn='%s' and name = '%s'" % (
                    data['final'], data['bifen'], m, data['date_cn'], data['name'])
                    cursor.execute(sql5)
                    conn.commit()
                    # print('更新成功2')

if __name__ == '__main__':
    chuangjian()
    get_bs_score()
    get_fb_score()
    close_conn()
