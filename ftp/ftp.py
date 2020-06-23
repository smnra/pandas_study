# -*- coding: utf-8 -*-
# @Time       : 2020/6/17 11:05
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : ftp.py
# @Software   : PyCharm
# @description: 本脚本的作用为 FTP下载的二次封装






import os
from ftplib import FTP
from multiprocessing import Pool,current_process
from multiprocessing import cpu_count

try:
    import ftpConfig as ftpConf
except ModuleNotFoundError :
    import ftp.ftpConfig as ftpConf




def initFtpConn(mrServer):
    # 此函数初始化FTP连接 返回一个ftp 的conn对象
    # mrServer是ftp的字典 格式: {'host': '10.100.162.111','user': 'richuser', 'password': 'mr@20invenT'}
    try:
        conn = FTP(mrServer['host'])
        conn.login(mrServer['user'], mrServer['password'])
        return conn
    except Exception as e :
        print(str(e))
        return None


def getEnbList(conn,mrDirPath):
    # 获取FTP上的 enbid 列表
    pathList = []
    # 获取MR数据目录的基站列表
    conn.dir(mrDirPath, pathList.append)
    enbList = [f.split(None, 8)[-1] for f in pathList if f.startswith('d')]
    return list(enbList)


def getfileList(mrServer,ftpBasePath,localBasePath):
    # dir ftpBasePath 目录放进列表, 对列表进行遍历,
    # 如果是目录 以当前级目录为参数,迭代自己,如果是文件 下载文件.
    # 并返回 文件和文件夹的路径列表

    # 初始化FTP连接
    conn = initFtpConn(mrServer)
    conn.cwd(ftpBasePath)  # ftp上 cd 到startDirPath 目录
    if not os.path.isdir(localBasePath):
        os.makedirs(localBasePath)
    os.chdir(localBasePath)    # 切换本地目录 到 localBasePath
    print(ftpBasePath,localBasePath)

    # ftp 当前目录下的文件列表
    pathList = []             # ftp 当前目录ls -l 列表
    ftpDirAbsPathList = []    # 保存 文件夹列表
    ftpFileAbsPathList = []   # 保存 文件列表
    conn.dir(ftpBasePath,pathList.append)
    for path in pathList:

        ftpPathName = path.split(None, 8)[-1]                    # ftp文件夹名
        ftpPathAbsPath = ftpBasePath + ftpPathName  # ftp文件夹绝对路径

        if path.startswith('d'):
            # 'drwxr-xr-x    2 1000     1000        24576 Jun 15 16:00 773590'
            # 以字符 'd' 开头的 就是 文件夹
            ftpDirAbsPathList.append(ftpPathAbsPath)  # 添加到列表

            # 如果本地路径不存在 文件夹 则创建文件夹
            localDirAbsPath = os.path.join(localBasePath,ftpPathName)
            if not os.path.isdir(localDirAbsPath):
                os.makedirs(localDirAbsPath)

            # 迭代 此文件夹
            tempList = getfileList(mrServer, ftpPathAbsPath,localDirAbsPath)
            # 合并列表
            ftpDirAbsPathList += list(tempList[0])
            ftpFileAbsPathList += list(tempList[1])

        elif path.startswith('-'):
            # '-rw-rw-r--  1 1000 1000  29417 Jun 15 15:58 FDD-LTE_MRS_NSN_OMC_773590_20200615234500.unzip.gz'
            # 以 '-' 开头就是文件 需要 下载到本地目录中
            localPathAbsPath = os.path.join(localBasePath,ftpPathName)
            ftpFileAbsPathList.append(localPathAbsPath)  # 添加到列表

            outf = open(localPathAbsPath, "wb")
            try:
                conn.retrbinary("RETR {}".format(ftpPathAbsPath), outf.write,4096)
                print("下载完成: {}".format(ftpPathAbsPath))
            except Exception as e:
                print(str(e))
            finally:
                outf.close()

    conn.quit()
    return  [list(ftpDirAbsPathList) , list(ftpFileAbsPathList)]




def getTaskParamList():
    # 从服务器上 获取基站ID列表,并组装成 远程目录 和 本地目录 的 列表 返回
    # 以便在 其他 脚本中进行多进程 下载
    tastList = []

    for i in range(0, len(ftpConf.ftpPathList)):
        # 远程目录和本地目录  循环日期 下载
        ftpPath = ftpConf.ftpPathList[i]
        localPath = ftpConf.localPathList[i]

        for mrServerName in ftpConf.mrServer.keys():
            # 循环 MR服务器 下载
            # {'host': '10.100.162.111', 'user': 'richuser', 'password': 'mr@20invenT'}
            mrServer = ftpConf.mrServer[mrServerName]

            # 初始化ftp连接
            conn = initFtpConn(mrServer)
            #获取FTP上的 enbid 列表
            enbList = getEnbList(conn,ftpPath)
            # 关闭ftp连接
            conn.quit()

            # 循环 基站id 下载
            for enbid in enbList:
                ftpPathEnbid = ftpPath + enbid + '/'
                localPathEnbid = localPath  + '\\' + enbid + '\\'

                # 获取FTP上的 文件列表
                tastList.append({'server': mrServer,
                                 'ftpPathEnbid': ftpPathEnbid,
                                 'localPathEnbid': localPathEnbid
                                 })


    return list(tastList)


if __name__ == '__main__':
    print(u'CPU核心数为{},设置进程池最大进程数为:{}'.format(cpu_count(), cpu_count()))
    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())


    def callback(x):
        print(' {}'.format(current_process().name, x))


    # 获取 要进行下载的 服务器参数和目录的列表
    taskParamList = getTaskParamList()

    for taskParamter in taskParamList:
        # 获取FTP上的 文件列表 多进程
        '''每次循环将会用空闲出来的子进程去调用目标'''
        po.apply_async(getfileList, args=(taskParamter['server'],
                       taskParamter['ftpPathEnbid'], taskParamter['localPathEnbid']),
                       callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")