# -*- coding: utf-8 -*-
# @Time       : 2020/7/13 14:56
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : execSql.py
# @Software   : PyCharm
# @description: 本脚本的作用为  执行sql 生成各种表
#  根据MDT数据 创建虚拟路测表， 然后分析各种无线覆盖类问题，
#  包含弱覆盖， 过覆盖， 重叠覆盖分析，mod3干扰分析，方位角差异，PCI分析等.
#  然后生成表导出







import os
import numpy as np
import pandas as pd
from io import StringIO

from multiprocessing import Pool,current_process
from multiprocessing import cpu_count

try:
    import dbConfig as dbConfig
except ModuleNotFoundError :
    import db.dbConfig as dbConfig

from getfile import getFileList


try:
    import dbConnect as dbConnect
except ModuleNotFoundError :
    import db.dbConnect as dbConnect


try:
    import createTable as createTable
except ModuleNotFoundError :
    import db.createTable as createTable





if __name__=='__main__':
    sqlFileList = getFileList(dbConfig.sqlPath,r'\.sql')
