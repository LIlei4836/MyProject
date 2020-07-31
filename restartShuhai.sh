#!/bin/sh

ps -ef|grep shuhai_ws_lll.py|grep -v grep|cut -c 9-15| xargs kill -s 9
nohup python3 /root/lll/data/shuhai_qihuo/shuhai_ws_lll.py > /dev/null 2>&1 &

