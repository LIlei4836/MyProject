import pymysql
import time

conn = pymysql.connect(host='129.211.25.69', port=3306, user='hhjc', password='j5TfPyGMbsrTHtHc',database='hhjc')
cursor = conn.cursor()


#update qm_bb_sf set state=%s,time1=%s,s=%s,f=%s where id =%s" % (results1['data']['_' +result[i][0][5]]['mnl']['single'],
#                        int(time.time()),result[i][1][1],result[i][1][0],result[i][0][5])
num_bb=time.time()-1578357200
for num in range(124296,124426):

    sql="update qm_bb_info set time1=time1+'%s',time3=time3+'%s' where id ='%s'"%(num_bb,num_bb,num)
    print(sql)
    cursor.execute(sql)
    conn.commit()

    sql = "update qm_fb_info set time1=time1+'%s',time3=time3+'%s' where id ='%s'" % (num_bb, num_bb, num)
    print(sql)
    cursor.execute(sql)
    conn.commit()