# -*- coding: utf-8 -*-
# @Time       : 2020/6/17 10:53
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : main.py
# @Software   : PyCharm
# @description: 本脚本的作用为


import os
from multiprocessing import Pool,current_process
from multiprocessing import cpu_count

from getfile import getFileList

from ftp import ftp

from unzip import unzip
from unzip import unzipConfig as unzipConf

from parsexml import parseXmlMro, parseXmlMrs, parseXmlMre
from parsexml import parseXmlConfig as xmlConf

from db import createTable, dbConnect, inDb
from db import dbConfig as dbConf



def ftpMrFileMP():
    # 多进程下载MR文件

    # 多进程消息回调函数
    def callback(x):
        print(' {}'.format(current_process().name,x))

    print(u'CPU核心数为{},设置进程池最大进程数为:{}'.format(cpu_count(),cpu_count()))
    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())

    # 获取 要进行下载的 服务器参数和目录的列表
    taskParamList = ftp.getTaskParamList()

    for taskParamter in taskParamList:
        # 获取FTP上的 文件列表 多进程
        '''每次循环将会用空闲出来的子进程去调用目标'''
        po.apply_async(ftp.getfileList, args=(taskParamter['server'],
                       taskParamter['ftpPathEnbid'], taskParamter['localPathEnbid']),
                       callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")




def unzipMrFileMP():
    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())

    # 创建文件夹
    unzipDirPath = os.path.abspath(unzipConf.xmlPath)
    if not os.path.exists(unzipDirPath):
        os.makedirs(unzipDirPath)

    # 获取压缩文件列表
    zipFileList = getFileList(unzipConf.zipPath, r'\.')

    def callback(x):
        print(' {}'.format(current_process().name, x))

    for zipFile in zipFileList:
        # 解压缩文件
        # unzip.unzipFile(zipFile, unzipDirPath)
        po.apply_async(unzip.unzipFile, args=(zipFile, unzipDirPath), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")


def xmlToCsvMP():
    # XML文件解析为csv文件
    pass



def parseMroToCsvMP():
    # 多进程消息回调函数
    def callback(x):
        pid = current_process().pid
        pname = current_process().name
        print('{},{},{}'.format(pname,pid,x))

    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())
    print(u'CPU核心数为{},设置进程池最大进程数为:{}'.format(cpu_count(),cpu_count()))

    # xml文件夹路径
    xmlPath = xmlConf.xmlPath

    # 保存csv文件夹路径
    csvPathMro = xmlConf.csvPathMro

    # 获取xml文件列表
    xmlFileListMro= getFileList(xmlPath, r'.+_MRO_.+\.xml')

    for xmlFileMro in xmlFileListMro:
        # result = parseXmlMro.toCSV(xmlFile,csvPath)      # 解析xml,并保存为csv文件

        # 多进程解析xml,并保存为csv文件
        po.apply_async(parseXmlMro.toCSV, args=(xmlFileMro,csvPathMro), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")


def parseMrsToCsvMP():
    # 多进程消息回调函数
    def callback(x):
        pid = current_process().pid
        pname = current_process().name
        print('{},{},{}'.format(pname,pid,x))

    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())
    print(u'CPU核心数为{},设置进程池最大进程数为:{}'.format(cpu_count(),cpu_count()))

    # 从配置文件导入xml文件夹路径
    xmlPath = xmlConf.xmlPath

    # 从配置文件导入 保存csv文件夹路径
    csvPathMrs = xmlConf.csvPathMrs

    xmlFileListMrs = getFileList(xmlPath,r'.+_MRS_.+\.xml')

    for xmlFileMrs in xmlFileListMrs:
        '''每次循环将会用空闲出来的子进程去调用目标'''
        # parseXmlMrs.toCSV(xmlFileMrs, csvPathMrs)

        po.apply_async(parseXmlMrs.toCSV, args=(xmlFileMrs,csvPathMrs), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")


def parseMreToCsvMP():
    # 多进程消息回调函数
    def callback(x):
        pid = current_process().pid
        pname = current_process().name
        print('{},{},{}'.format(pname,pid,x))

    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())
    print(u'CPU核心数为{},设置进程池最大进程数为:{}'.format(cpu_count(),cpu_count()))

    # 从配置文件导入xml文件夹路径
    xmlPath = xmlConf.xmlPath

    # 从配置文件导入 保存csv文件夹路径
    csvPathMre = xmlConf.csvPathMre

    xmlFileListMre = getFileList(xmlPath,r'.+_MRE_.+\.xml')

    for xmlFileMre in xmlFileListMre:
        '''每次循环将会用空闲出来的子进程去调用目标'''
        # parseXmlMre.toCSV(xmlFileMre,csvPathMre)
        po.apply_async(parseXmlMre.toCSV, args=(xmlFileMre,csvPathMre), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")


def csvInDb():
    # 初始化数据库连接
    conn, curs = dbConnect.connDb(dbConf.usedDbType)

    # 获取csv文件列表
    csvFileList = getFileList(dbConf.csvPath, r'.+\.csv')
    for csvFile in csvFileList:
        tableName, fileExtName = os.path.splitext(os.path.split(csvFile)[-1])
        tableName = tableName.lower()

        try:
            if not conn:
                conn, curs = dbConnect.connDb(dbConf.usedDbType)

            # 检测表是否存在.
            # isExistTable = dbConnect.isTableExist(conn, curs, tableName, dbConf.usedDbType)
            # if not isExistTable: createTable.main()   # 不存在就执行创建表的脚本

            inDb.inDb(conn, curs, csvFile, tableName, dbConf.usedDbType)

        except Exception as e:
            print(str(e))


    if conn: dbConnect.close(conn,curs)


def csvToKpiMP():
    # 将解析出的mdt数据 计算各种无线问题的KPI 和分析
    # 如 弱覆盖率,重叠覆盖问题分析,PCI分析, mod3干扰分析, 覆盖方向分析等
    pass


def kpiToGeomMP():
    # 使用 kpi 表 转化为地理对象  可以使用常见的 地理工具展示, 如mapinfo, qgis arcgis等.
    pass





if __name__ == '__main__':
    # FTP下载MR文件
    # ftpMrFileMP()

    # # 解压缩MR压缩包
    # unzipMrFileMP()
    #
    # # MRO XML文件解析为csv文件
    # parseMroToCsvMP()
    #
    # # MRS XML文件解析为csv文件
    # parseMrsToCsvMP()
    #
    # # MRE XML文件解析为csv文件
    # parseMreToCsvMP()

    # csv文件入库
    csvInDb()

    # 将解析出的mdt数据 计算各种无线问题的KPI 和分析
    # csvToKpiMP()
    #
    # # 使用 kpi 表 转化为地理对象
    # kpiToGeomMP()