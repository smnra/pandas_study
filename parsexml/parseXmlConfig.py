# -*- coding: utf-8 -*-
# @Time       : 2020/6/30 11:36
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : parseXmlConfig.py
# @Software   : PyCharm
# @description: 本脚本的作用为  xml 文件解析的 配置文件



import os

# xml文件所在目录
xmlPath = os.path.abspath('.\\mr_data\\xml\\')

# csv文件保存目录
csvPath = os.path.abspath('.\\mr_data\\csv\\')

# 需要解压的MR文件类型

parseType = ['MRO','MRS','MRE']
parseVender = ['NSN','ZTE','HUAWEI']
