# -*- coding: utf-8 -*-
# @Time       : 2020/6/17 15:32
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : ftpConf.py
# @Software   : PyCharm
# @description: 本脚本的作用为 FTP 下载 MR文件的 配置文件 ,
#               可以设置 下载的日期, 下载的基站列表, 下载的 MR服务器 配置等






import os


# 前一天的日期 '20200616'

dateList = ['20200727','20200728']


# 本地目录 和远程目录
localPathList = [os.path.abspath('.\\mr_data\\zip\\{}\\'.format(tDate)) for tDate in dateList ]
ftpPathList = ['/home/richuser/l3fw_mr/kpi_import/{}/'.format(tDate) for tDate in dateList ]




# MR服务器列表
mrServer = {
    'MR1': {'host': '10.100.162.112','user': 'richuser', 'password': 'mr@20invenT'},
    'MR2': {'host': '10.100.162.111','user': 'richuser', 'password': 'mr@20invenT'},
    'MR3': {'host': '10.100.162.110','user': 'richuser', 'password': 'mr@20invenT'},
    'MR4': {'host': '10.100.162.109','user': 'richuser', 'password': 'mr@20invenT'},
    'MR5': {'host': '10.100.162.108','user': 'richuser', 'password': 'mr@20invenT'},
    'MR6': {'host': '10.100.162.115','user': 'richuser', 'password': 'mr@20invenT'},
    'MR7': {'host': '10.100.162.117','user': 'richuser', 'password': 'mr@20invenT'},
    'MR8': {'host': '10.100.162.119','user': 'richuser', 'password': 'mr@20invenT'},
    'MR9': {'host': '10.100.162.120','user': 'richuser', 'password': 'mr@20invenT'},
    # 'MR10':{'host': '10.100.162.131','user': 'richuser', 'password': 'mr@20invenT'},
    # 'MR11':{'host': '10.100.162.133','user': 'richuser', 'password': 'mr@20invenT'},
    # 'MR12':{'host': '10.100.162.135','user': 'richuser', 'password': 'mr@20invenT'},
    # 'MR13':{'host': '10.100.162.137','user': 'richuser', 'password': 'mr@20invenT'}
}


# 要下载数据的基站列表
enbList = ['774050', ]
