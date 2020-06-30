# -*- coding: utf-8 -*-
# @Time       : 2020/6/19 16:10
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : unzipConfig.py
# @Software   : PyCharm
# @description: 本脚本的作用为 解压缩 .xml.gz 的配置 文件



import os

# 压缩文件的文件夹路径 和 xml保存的文件夹路径
zipPath = os.path.abspath('.\\mr_data\\zip\\')
xmlPath = os.path.abspath('.\\mr_data\\xml\\')