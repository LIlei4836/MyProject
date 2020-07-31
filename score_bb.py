import requests
import json
import re
import time
import urllib.request
import pymysql
import warnings
from userAgents import get_html

warnings.filterwarnings("ignore")
# conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='root',database='foryou')
conn = pymysql.connect(host='103.80.18.238', port=3306, user='hhjc', password='ZjMxMGUzODE4Y2UyZWEyM2M1MmQyNzJh',database='hhjc')
cursor = conn.cursor()

def close_conn():
    cursor.close()
    # 关闭光标对象
    conn.close()

#创建数据表score_fb存储足球比分数据。
def chuangjian_qm_score_bb():
    sql1="""CREATE TABLE IF NOT EXISTS qm_score_bb(
   id  VARCHAR(30) PRIMARY KEY comment '球赛id',
   date1 VARCHAR(30) comment '球赛日期',
   time1 VARCHAR(30) comment '比赛时间',
   lcn_abbr VARCHAR(30) comment '联赛名字',
   num_cn VARCHAR(30) comment '赛事编号',
   status VARCHAR(30) comment '状态 2进行中，-1结束，0未开始',
   acn_abbr VARCHAR(30) comment '客队名字',
   hcn_abbr VARCHAR(30) comment '主队名字',
   ascore VARCHAR(30) comment '客队总比分',
   hscore VARCHAR(30) comment '主队总比分',
   score_detail TEXT comment '比分详情json串,as客，hs主'
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    cursor.execute(sql1)
    print('创建成功！chuangjian_qm_score_bb')
    conn.commit()

#时间戳 毫秒级
t=time.time()
t=int(t*10000)

def get_bb_socre():
    #篮球数据接口：
    url='https://i.sporttery.cn/zfapp_v1_1/bk_live_match_gsm/get_bk_live_list?cb=get_list_bk&_='+str(t)
    # response = requests.get(url)
    # s=response.text
    s = get_html(url)
    #将接口中获取到的数据转化成标准的json格式
    pat4 = 'get_list_bk\((.*)\);'
    results=re.compile(pat4).findall(s)
    #用json.loads方法将字符串转化成json格式。备用
    s=json.loads(results[0])
    #将得到的比分数组按条解析出来，处理
    for i in s['data']:
        # 经过对图片接口的分析，能够获取到图片真实地址及球队名字，与球赛id相关联。
        url_zhutu_jiekou='http://i.sporttery.cn/zfapp_v1_1/bk_match_info/get_team_data?f_callback=access_bk_logo&tids[]='+str(i['hid'])+'&_='+str(t)
        # response = requests.get(url_zhutu_jiekou)
        # tu = response.text
        tu = get_html(url_zhutu_jiekou)
        pat_tu = 'access_bk_logo\((.*)\);'
        results = re.compile(pat_tu).findall(tu)
        url_tu = json.loads(results[0])
        zhutu_name = url_tu['data'][str(i['hid'])]['team_abbr']
        zhutu_url = url_tu['data'][str(i['hid'])]['team_pic']
        # print('(主)', zhutu_name, zhutu_url)
        # 根据主队图片名字，及图片url，下载到本地,并且将图片命名为球队名字。
        urllib.request.urlretrieve(zhutu_url, filename='/www/hhjc/public/pic_bb/' + zhutu_name + '.png')



        #https://i.sporttery.cn/zfapp_v1_1/bk_match_info/get_team_data?f_callback=access_bk_logo&tids[]=187&_=1552036138837
        url_ketu_jiekou='http://i.sporttery.cn/zfapp_v1_1/bk_match_info/get_team_data?f_callback=access_bk_logo&tids[]='+str(i['aid'])+'&_='+str(t)
        # response = requests.get(url_ketu_jiekou)
        # tu = response.text
        tu = get_html(url_ketu_jiekou)
        pat_tu = 'access_bk_logo\((.*)\);'
        results = re.compile(pat_tu).findall(tu)
        url_tu = json.loads(results[0])
        ketu_name = url_tu['data'][str(i['aid'])]['team_abbr']
        ketu_url = url_tu['data'][str(i['aid'])]['team_pic']
        # print('(客)', ketu_name,ketu_url)
        urllib.request.urlretrieve(ketu_url, filename='/www/hhjc/public/pic_bb/' + ketu_name + '.png')

        #根据球赛id获取比赛每小节比分情况
        bb_score_details_url='https://i.sporttery.cn/zfapp_v1_1/bk_live_match_gsm/get_bk_live?cb=get_detail&mid='+str(i['id'])+'&_='+str(t)
        # response = requests.get(bb_score_details_url)
        # bb_score_details = response.text
        bb_score_details = get_html(bb_score_details_url)
        # print(bb_score_details)
        pat_details = 'get_detail\((.*)\);'
        results = re.compile(pat_details).findall(bb_score_details)
        details = json.loads(results[0])
        ss=details['data'][0]
        # print(ss)
        #创建一个json串存储比分详情。
        details=[]
        #as1为客，hs1为主
        details.append(ss['as1'])
        details.append(ss['hs1'])
        details.append(ss['as2'])
        details.append(ss['hs2'])
        details.append(ss['as3'])
        details.append(ss['hs3'])
        details.append(ss['as4'])
        details.append(ss['hs4'])
        details.append(ss['as5'])
        details.append(ss['hs5'])
        details.append(ss['as6'])
        details.append(ss['hs6'])
        details.append(ss['as7'])
        details.append(ss['hs7'])

        details=json.dumps(details)
        print(details)
        sql2="insert into qm_score_bb(id,date1,time1,lcn_abbr,num_cn,status,acn_abbr,hcn_abbr,ascore,hscore,score_detail)VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(ss["id"],ss["date"],ss["time"],ss["lcn_abbr"],ss["num_cn"],ss["status"],ss["acn_abbr"],ss["hcn_abbr"],ss["ascore"],ss["hscore"],details.replace("'",""))
        try:
            cursor.execute(sql2)
            conn.commit()
            print('插入成功！qm_score_bb', ss['id'], ss['num_cn'])
        except pymysql.err.IntegrityError as e:
            # print(e)
            sql3= "update qm_score_bb set status='%s',ascore='%s',hscore='%s',score_detail='%s' where id ='%s'" % (
                ss['status'],ss['ascore'],ss['hscore'],details.replace("'",""),ss['id'])
            cursor.execute(sql3)
            conn.commit()
            print('更新成功！qm_score_bb', ss['id'], ss['num_cn'])
if __name__ == '__main__':
    chuangjian_qm_score_bb()
    get_bb_socre()
    close_conn()