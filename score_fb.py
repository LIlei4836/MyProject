#项目

import requests
import json
import re
import time
import urllib.request
import pymysql
import warnings
from userAgents import get_html



warnings.filterwarnings("ignore")
conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()

"'Fixture':'未开赛','Playing':'进行中','Postponed':'推迟','Suspended':'暂停','Played':'已结束','Cancelled':'取消'"
fb_status={'Fixture':'0','Playing':'2','Played':'-1','Postponed':'3','Suspended':'4','Cancelled':'5'}
#创建数据表score_fb存储足球比分数据。

def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()
def chuangjian_qm_score_fb():
    sql1 = """CREATE TABLE IF NOT EXISTS qm_score_fb(
       id  VARCHAR(30) PRIMARY KEY comment '球赛id',
       date1 VARCHAR(30) comment '球赛日期',
       time1 VARCHAR(30) comment '比赛时间',
       minute1 VARCHAR(30) comment '比赛进行时长',
       lcn_abbr VARCHAR(30) comment '联赛名字',
       num_cn VARCHAR(30) comment '赛事编号',
       status VARCHAR(30) comment '状态 进行中：2，未开始：0，已经结束-1',
       hcn_abbr VARCHAR(30) comment '主队名字',
       acn_abbr VARCHAR(30) comment '客队名字',
       hts_h VARCHAR(30) comment '主队半场分',
       hts_a VARCHAR(30) comment '客队半场分',
       hscore VARCHAR(30) comment '主队总比分',
       ascore VARCHAR(30) comment '客队总比分',
       score_detail TEXT comment '比分详情json串,G进球，YC黄牌,SI替换，PG点球，RC待定，a右侧，h左侧'
        )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    cursor.execute(sql1)
    print('创建成功！score_fb')
    conn.commit()

#时间戳 毫秒级
t=time.time()
t=int(t*10000)

def get_fb_score():
    #足球比分数据接口地址：
    url = "http://i.sporttery.cn/zfapp_v1_1/matches/fb_livescore?cb=get_list_fb&_="+str(t)
    s=get_html(url)
    # print(s)
    # response = requests.get(url)
    # s=response.text
    #将接口中获取到的数据转化成标准的json格式
    pat4 = 'get_list_fb\((.*)\);'
    results=re.compile(pat4).findall(s)
    #用json.loads方法将字符串转化成json格式。备用
    s=json.loads(results[0])
    #根据前面的id获取
    for i in s['data']:
        # print(s['data'][i])
        #拼接图片地址接口地址

        url_zhutu_jiekou='https://i.sporttery.cn/zfapp_v1_1/fb_match_info/get_team_data?f_callback=access_fb_logo&tids[]='+str(s['data'][i]['h_gid'])+'&_='+str(t)
        tu = get_html(url_zhutu_jiekou)
        # print(type(tu))
        pat_tu = 'access_fb_logo\((.*)\);'
        results = re.compile(pat_tu).findall(tu)
        url_tu= json.loads(results[0])
        #主队图片名字，及图片url
        zhutu_name=url_tu['data'][str(s['data'][i]['h_gid'])]['team_abbr']
        zhutu_url='http:'+url_tu['data'][str(s['data'][i]['h_gid'])]['team_pic']
        # print('(主)',zhutu_name,zhutu_url)
        #将图片下载到文件夹中并且以球队命名
        try:
            urllib.request.urlretrieve(zhutu_url, filename='/www/hhjc/public/pic_fb/'+zhutu_name+'.png')
            # urllib.request.urlretrieve(zhutu_url, filename='fb_pic/' + zhutu_name + '.png')
        except urllib.error.URLError as e:
            print('(主)', zhutu_name, e)

        url_ke_jiekou = 'https://i.sporttery.cn/zfapp_v1_1/fb_match_info/get_team_data?f_callback=access_fb_logo&tids[]=' + str(s['data'][i]['a_gid']) + '&_=' + str(t)
        # response = requests.get(url_ke_jiekou)
        # tu = response.text
        tu = get_html(url_ke_jiekou)
        # print(type(tu))
        pat_tu = 'access_fb_logo\((.*)\);'
        results = re.compile(pat_tu).findall(tu)
        url_tu= json.loads(results[0])
        # 客队图片名字，及图片url
        ketu_name = url_tu['data'][str(s['data'][i]['a_gid'])]['team_abbr']
        ketu_url = 'http:' + url_tu['data'][str(s['data'][i]['a_gid'])]['team_pic']
        # print('(客)',ketu_name, ketu_url)
        # 将图片下载到文件夹中并且以球队命名
        try:
            # urllib.request.urlretrieve(ketu_url, filename='fb_pic/' + ketu_name + '.png')
            urllib.request.urlretrieve(ketu_url, filename='/www/hhjc/public/pic_fb/' + ketu_name + '.png')
        except urllib.error.URLError as e:
            print('(客)', ketu_name, e)
        ss=s['data'][i]
        # print(ss)

        #足球比分详情
        fb_score_details_url='https://i.sporttery.cn/zfapp_v1_1/matches/fb_livescore_detail_float?cb=get_detail_float&matchid='+str(s['data'][i]['m_id'])+'&_'+str(t)
        # response = requests.get(fb_score_details_url)
        # fb_score_details= response.text
        fb_score_details = get_html(fb_score_details_url)
        pat_details='get_detail_float\((.*)\);'
        results = re.compile(pat_details).findall(fb_score_details)
        details = json.loads(results[0])
        detail0 = []
        for  i in range(len(details['data']['event'])):
            detail=details['data']['event'][i][0]
            #dic加在数组中
            detail2=[]
            detail2.append(detail['minute'])
            detail2.append(str(detail['person']))
            # print(type(detail['person']))
            detail2.append(detail['code'])
            detail2.append(detail['is_ha'])
            detail0.append(detail2)
        detail0=json.dumps(detail0, ensure_ascii=False)
        # print(ss['m_id'], ss['date_cn'], ss['time_cn'], ss['minute'], ss['l_cn_abbr'], ss['match_num'], ss['status'],
        #       ss['h_cn_abbr'], ss['a_cn_abbr'], ss['hts_a'], ss['fs_h'],str(detail0))

        sql2 = "insert into qm_score_fb(id,date1,time1,minute1,lcn_abbr,num_cn,status,hcn_abbr,acn_abbr,hts_h,hts_a," \
               "ascore,hscore,score_detail)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s'," \
               "'%s','%s')" % (ss['m_id'], ss['date_cn'], ss['time_cn'], ss['minute'], ss['l_cn_abbr'],
                ss['match_num'], fb_status[ss['status']],ss['h_cn_abbr'], ss['a_cn_abbr'],ss['hts_h'],ss['hts_a'], ss['fs_a'],
                ss['fs_h'],detail0.replace("'",""))
        try:
            # print(sql2)
            cursor.execute(sql2)
            conn.commit()
            print('插入成功！qm_score_fb', ss['m_id'], ss['match_num'])
        except pymysql.err.IntegrityError as e:
            sql3 = "update qm_score_fb set status='%s',minute1='%s',ascore='%s',hscore='%s',score_detail='%s' where id ='%s'" % (
                fb_status[ss['status']],ss['minute'], ss['fs_a'], ss['fs_h'], detail0.replace("'",""), ss['m_id'])
            cursor.execute(sql3)
            conn.commit()
            print('更新成功！qm_score_bb', ss['m_id'], ss['match_num'])

if __name__ == '__main__':
    chuangjian_qm_score_fb()
    get_fb_score()
    close_conn()
    print('ok')

