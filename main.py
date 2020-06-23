# -*- coding: utf-8 -*-
# @Time       : 2020/6/17 10:53
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : main.py
# @Software   : PyCharm
# @description: 本脚本的作用为

from multiprocessing import Pool,current_process
from multiprocessing import cpu_count
from ftp import ftp






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







if __name__ == '__main__':
    ftpMrFileMP()