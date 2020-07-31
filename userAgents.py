import random
import requests
import linecache
import os
import sys

#每页100条
#读取host.txt 返回所有的ip（list形式）
os.chdir(sys.path[0])  # os.chdir() 方法用于改变当前工作目录到指定的路径。
hosturl = os.path.abspath('host.txt')

def get_dailichi():
    count = len(open(hosturl, 'rU').readlines())
    n = random.randint(0, count-5)
    count = linecache.getline(hosturl, n)
    ip = count.strip('\n').split('\t')
    try:
        proxy = 'http:\\' + ip[0] + ':' + ip[1]
        ip0 = {'proxy': proxy}
        return ip0,ip[0]
    except Exception as e:
        get_dailichi()

def get_userAgent():
    user_agent_list = [
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
        "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
        "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)"
    ]
    a = random.sample(user_agent_list, 1)  # 用户代理列表中随机取出一个
    user_agent0 = a[0]
    # headers = {'user-agent': user_agent0}
    return user_agent0

def del_badip(ip):
    # hosturl = 'host.txt'
    with open(hosturl,'r') as r:
        lines=r.readlines()
    with open(hosturl,'w') as w:
        for l in lines:
           if ip not in l:
              w.write(l)
    print('删除成功',ip)


#传入url，加上反爬虫措施 str类型
def get_html(url1):
    ip0,ip=get_dailichi()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        # "Host":    "info.sporttery.cn",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent":get_userAgent()
    }
    try:
        s = requests.get(url1,proxies=ip0,headers=headers,timeout=5)#requests.get(),使用路径，ip代理，用户代理
        html = s.text
        # print(s)
        # print(html)
        return html  # 返回获取的html
    except requests.exceptions.ConnectionError as e:
        print('代理报错',ip0,ip)
        # del_badip(ip)
        get_html(url1)
    except requests.exceptions.ReadTimeout as e:
        print('访问超时',ip0,ip)
        # del_badip(ip)
        get_html(url1)
        # pass
    # 之前用html=s.text 会出现中文乱码问题，改成.content,就好了



#传入url，加上反爬虫措施 str类型
def get_html_bytes(url1):
    # print('111')
    try:
        ip0,ip=get_dailichi()
    except TypeError:
        ip0, ip = get_dailichi()
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        # "Host":    "info.sporttery.cn",
        "Upgrade-Insecure-Requests": "1",
    }
    headers["User-Agent"]=get_userAgent()

    # print(ip0,user_agent0)
    try:
        s = requests.get(url1,timeout=5,proxies=ip0,headers=headers)#requests.get(),使用路径，ip代理，用户代理
        html = s.content
        # print(html)
        return html  # 返回获取的html
    except Exception as e:
        # print('代理报错',ip0,ip)
        get_html_bytes(url1)
        # del_badip(ip)
        # get_html_bytes(url1)
