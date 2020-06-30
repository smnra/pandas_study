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
from ftp import ftp
from unzip import unzip
from unzip import unzipConfig as unzipConf






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




def unzipMrFile():
    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())

    # 创建文件夹
    unzipDirPath = os.path.abspath(unzipConf.xmlPath)
    if not os.path.exists(unzipDirPath):
        os.makedirs(unzipDirPath)

    # 获取压缩文件列表
    zipFileList = unzip.getZipPathList(unzipConf.zipPath, r'\.')

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

if __name__ == '__main__':
    # FTP下载MR文件
    ftpMrFileMP()

    # 解压缩MR压缩包
    unzipMrFile()

