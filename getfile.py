# -*- coding: utf-8 -*-
# @Time       : 2020/6/30 14:13
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : getfile.py
# @Software   : PyCharm
# @description: 本脚本的作用为

import os,re

def getFileList(DirPath, rgeStr):
    # 获取一个目录下 所有的满足filterStrList 条件的  文件的列表 包含子目录
    # 返回 文件列表
    # 基准目录
    DirPathAbs = os.path.abspath(DirPath)
    resultFileList = []    # 用于保存基准目录下符合过滤条件的 文件列表

    def fixSubName(rgeStr, tagStr):
        # 使用正则表达式匹配 字符串,匹配成功 返回True
        tempFlag = re.search(rgeStr,tagStr)
        if tempFlag :
            return True
        else:
            return False

    # 获取基准目录下所有的路径 列表
    for rootPath, dirList, fileList in os.walk(DirPathAbs):
        for file in fileList :
            if fixSubName(rgeStr, file) :
                resultFileList.append(os.path.abspath(os.path.join(rootPath,file)))

    return list(resultFileList)

if __name__=='__main__':
    fileList = getFileList('./mr_data/', r'.+_MRO_.+\.gz')
    print(fileList)