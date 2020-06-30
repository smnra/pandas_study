# -*- coding: utf-8 -*-
# @Time       : 2020/6/29 9:54
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : unzipFile_test.py
# @Software   : PyCharm
# @description: 本脚本的作用为


import zipfile,os

zipFilePath = r"E:\工具\资料\宝鸡\研究\Python\python3\pandas_study\mr_data\zip\zte\FDD-LTE_MRS_ZTE_OMC33_40169_20200517000000.zip"
xmlDirPath = r"E:\工具\资料\宝鸡\研究\Python\python3\pandas_study\mr_data\xml\zte"

zipTempFile = zipfile.ZipFile(zipFilePath, 'r')
for f_in in zipTempFile.namelist():
    zipTempFile.extract(f_in, xmlDirPath)
    print('解压缩：{}'.format(zipFilePath))