# -*- coding: utf-8 -*-
# @Time       : 2019/12/13 16:05
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : createTable.py
# @Software   : PyCharm
# @description: 本脚本的作用为 创建数据库表结构



import os
import psycopg2 as postgres
import xml.etree.cElementTree as ET
from getfile import getFileList

try:
    import dBConnect as dBConnect
except ModuleNotFoundError :
    import db.dbConnect as dBConnect


try:
    import dbConfig as dbConfig
except ModuleNotFoundError :
    import db.dbConfig as dbConfig






def readXML(xmlFileName):
    # 读取xml文件, 并解析内容到列表
    titleList = []

    if os.path.exists(xmlFileName):
        tree = ET.parse(xmlFileName)
        root = tree.getroot()

        # eNB 标签内容提取
        enbTag = root.find("eNB")

        # measurement 标签提取
        if enbTag :
            measurementTagList = enbTag.findall("measurement")
            for measurementTag in measurementTagList:
                mr = {'mr': '', 'title': []}
                mr['mr'] = measurementTag.get('mrName', '').replace("MR.", "MRS_")
                values = measurementTag.find("smr").text
                values = values.strip(' ').replace('.','_')
                mr['title'] = mr['title'] + values.split(" ")
                titleList.append(mr)
                # print(mr)

        print("解析完成:{}".format(xmlFileName))

    else:
        print("File Not Found!")

    return titleList



def createTable(titleList, curs,dbType):
    # 拼接 建表语句, 并执行建表语句.
    for title in titleList:

        if dbType.lower()=='postgres':
            createTableStr = "create table if not exists " + title['mr'] + \
                         "_15MI (enb_id text, startTime text, endTime text, reportTime text, period integer, eci text, " + \
                         " integer, ".join(title['title']) + \
                         " integer );"

        elif dbType.lower() == 'oracle':
            createTableStr = "create table if not exists " + title['mr'] + \
                             "_15MI (enb_id varchar2(32), startTime varchar2(32), endTime varchar2(32), reportTime varchar2(32), period varchar2(32), eci varchar2(32), " + \
                             " varchar2(32), ".join(title['title']) + \
                             " varchar2(32) );"

        # 执行建表语句
        try:
            curs.execute(createTableStr)
            curs.execute('commit;')
        except Exception as e:
            print(str(e))

        print(createTableStr)



def main():
    try:
        # 初始化数据库连接
        conn, curs = dBConnect.connDb(dbConfig.usedDbType)

        #获取MRS样本文件
        xmlFile = getFileList(dbConfig.xmlPath, r'.+_MRS_.+\.xml')[0]

        if curs:
            # 创建MRo的表 和 MRE的表
            dBConnect.execSql(curs, dbConfig.MRO_15MI_Str_pg)
            dBConnect.execSql(curs, dbConfig.MRO_RIP_15MI_Str_pg)
            dBConnect.execSql(curs, dbConfig.MRE_15MI_Str_pg)
            curs.execute('commit;')

            # 获取 表头结构, 列名信息
            titleList = readXML(xmlFile)
            # 创建MRS的表
            createTable(titleList, curs, dbConfig.usedDbType)



    except Exception as e:
        print(e)

    finally:
        if conn: dBConnect.close(conn,curs)

if __name__=='__main__':
    main()