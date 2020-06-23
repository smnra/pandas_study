# -*- coding: utf-8 -*-
# @Time       : 2020/6/19 16:10
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : unzip.py
# @Software   : PyCharm
# @description: 本脚本的作用为



import os
import shutil
import gzip
from multiprocessing import Pool,current_process
from multiprocessing import cpu_count

try:
    import unzipConfig as unzipConf
except ModuleNotFoundError :
    import unzip.unzipConfig as unzipConf





def getZipPathList(zipDirPath, *filterStrList):
    # 获取一个目录下 所有的满足filterStrList 条件的  文件的列表 包含子目录
    # 返回 文件列表
    # 基准目录
    zipDirPathAbs = os.path.abspath(zipDirPath)
    zipFileList = []    # 用于保存基准目录下符合过滤条件的 文件列表
    zipDirList = []     # 用于保存基准目录下的 文件夹列表

    def fixSubName(subStrList, Str):
        for subStr in subStrList:
            if subStr not in Str: return False
        return True


    # 获取基准目录下所有的路径 列表
    _, zipFileList, zipDirList = os.walk(zipDirPathAbs)

    return [os.path.abspath(zipfile) for zipfile in zipFileList if fixSubName(filterStrList, zipfile) ]


def unzipFile(zipFile,unzipDir):
    zipfilePathAbs = os.path.abspath(zipFile)
    unzipDirPathAbs = os.path.abspath(unzipDir)
    # 分割 文件名
    zipfileName = os.path.split(zipfilePathAbs)[-2]  # 此处需要一个文件名 不包含扩展名 坑能需要 修改*********************

    # 解压出的xml的绝对路径
    xmlFilePathAbs= os.path.join(unzipDirPathAbs + zipfileName)

    try:
        with gzip.open(zipfilePathAbs, 'rb') as f_in:
            with open(xmlFilePathAbs, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                print('解压缩：{}'.format(zipfilePathAbs))
                return xmlFilePathAbs
    except Exception as error:
        print(str(zipfilePathAbs) + ' ' + str(error))
        return ''


if __name__ == '__main__':
    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())

    # 创建文件夹
    unzipDirPath = os.path.abspath(unzipConf.xmlPath)
    if not os.path.exists(unzipDirPath):
        os.makedirs(unzipDirPath)

    # 获取压缩文件列表
    zipFileList = getZipPathList(unzipConf.zipPath, '.gz','MRS')

    def callback(x):
        print(' {}'.format(current_process().name, x))

    for zipFile in zipFileList:
        # 解压缩文件
        # unzipFile(zipFile, unzipDirPath)

        po.apply_async(unzipFile, args=(unzipDirPath,),
                       callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")