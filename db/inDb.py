# -*- coding: utf-8 -*-
# @Time       : 2019/12/17 11:27
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : inDb.py
# @Software   : PyCharm
# @description: 本脚本的作用为 入库 MRS数据 到oracle中    psycopg2 copy_from

import os,time
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



def insert_date(conn, curs,tableName, dbType, data):
    try:
        if dbType == 'postgres':
            if not curs:
                conn, curs = dbConnect.connDb(dbConfig.usedDbType)

            # postgresql 大量数据入库 使用强大的copy_from()，是postgresSQ的内置函数
            curs.copy_from(data, tableName, sep='\t', null='NULL')
            curs.execute("commit;")
            return True
        elif dbType == 'oracle':
            # 如果是oracle 数据库 使用 SqLoader 入库
            # createOraclCtl()
            pass
    except Exception as e:
        print(str(e))
        time.sleep(6000)
        insert_date(conn, curs, dbType, data)

    curs.execute("rollback;")
    return False
def inDb(conn,curs,csvFile,tableName,dbType):
    print(tableName , csvFile)

    # postgresql 大量数据入库
    # 强大的copy_from()，是postgresSQ的内置函数
    try:
        output = StringIO()

        fileReader = pd.read_csv(
            csvFile,  # 读取的文件
            header=None,  # 指定 title 所在的行,  None 为添加默认列名
            # index_col='MROID', # 指定列为索引
            index_col=None,  # 不指定列索引, 即自动生成列索引

            # 指定需要读取的列
            # usecols=['CITY', 'SDATE', 'GRIDX', 'GRIDY', 'S_RSRP', 'MROID'],

            # 每次读取固定的行数(chunksize=6), 返回一个可迭代的文件读取对象
            chunksize=10000,

            iterator=True,

            # 打印读取数据信息统计
            verbose=True,

            # 如果设定为True并且parse_dates 可用，那么pandas将尝试转换为日期类型，
            # 如果可以转换，转换方法并解析。在某些情况下会快5~10倍
            # infer_datetime_format=True,
            # parse_dates=['SDATE'],

            # 使用的分析引擎。可以选择C或者是python。C引擎快但是Python引擎功能更加完备。'
            engine='c',
            # engine: {‘c’, ‘python’}, optional

            # 引号，用作标识开始和解释的字符，引号内的分割符将被忽略。
            quotechar="'",

            # 指定 解析时间列的函数名
            # date_parser=parse_dates,

            # 读取文件时遇到和列数不对应的行，此时会报错。若报错行可以忽略，则添加以下参数
            error_bad_lines=False,

            # 读取文件的字符集
            encoding='utf-8',

            # 一组用于替换NA / NaN的值。如果传参，需要制定特定列的空值。默认为‘1.
            # IND’, ‘1.#QNAN’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘nan’`.
            na_values = 'NULL',

            # 指定某些列的数据类型
            dtype=object
            # dtype={'CITY': object,
            #        'GRIDX': np.float16,
            #        'GRIDY': np.float16,
            #        'S_RSRP': np.int16,
            #        }

            )

        for subDf in fileReader:
            print("当前行: " + str(fileReader._currow))
            # 删除无效数据
            subDf.drop(subDf[subDf[4] != '15'].index, inplace=True)
            # 删除第四列不为'15' 的行

            # 替换控制为 'NULL'
            subDf.replace(np.NaN, 'NULL', inplace=True)

            # dataframe类型转换为IO缓冲区中的str类型
            subDf.to_csv(output, sep='\t', index=False, header=False)
            output1 = output.getvalue()

            result = 1
            while result:
                result = not insert_date(conn, curs,tableName,dbType,StringIO(output1))

    except Exception as e:
        print(str(e))





if __name__ == '__main__':
    # try:
        # 初始化数据库连接
        conn, curs = dbConnect.connDb(dbConfig.usedDbType)

        # 获取csv文件列表
        csvFileList = getFileList(dbConfig.csvPath, r'.+\.csv')
        for csvFile in csvFileList:
            tableName,fileExtName = os.path.splitext(os.path.split(csvFile)[-1])
            tableName = tableName.lower()

            # 检测表是否存在.
            isExistTable = dbConnect.isTableExist(conn, curs,tableName,dbConfig.usedDbType)
            # if not isExistTable: createTable.main()   # 不存在就执行创建表的脚本

            inDb(conn, curs, csvFile, tableName,dbConfig.usedDbType)

    # except Exception as e:
    #     print(str(e))
    #
    # finally:
    #     if conn: dbConnect.close(conn,curs)


