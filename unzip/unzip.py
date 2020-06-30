# -*- coding: utf-8 -*-
# @Time       : 2020/6/19 16:10
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : unzip.py
# @Software   : PyCharm
# @description: 本脚本的作用为   通用解压缩模块



import os,re
import shutil
import gzip,zipfile,tarfile,bz2,rarfile

from multiprocessing import Pool,current_process
from multiprocessing import cpu_count

from getfile import getFileList


try:
    import unzipConfig as unzipConf
except ModuleNotFoundError :
    import unzip.unzipConfig as unzipConf




def unzipFile(zipFile,unzipDir):
    print(zipFile)
    zipfilePathAbs = os.path.abspath(zipFile)
    unzipDirPathAbs = os.path.abspath(unzipDir)
    # 分割 文件名
    zipfileName = os.path.split(zipfilePathAbs)[-1]
    zipfileName, zipfileType = zipfileName.split('.',1) # 此处分为 压缩文件的文件名 和 压缩文件的扩展名

    # 解压出的xml的绝对路径
    xmlFileName= zipfileName + '.xml'

    if zipfileType == 'xml.gz': unzipGz(zipfilePathAbs, unzipDirPathAbs, xmlFileName)
    if zipfileType.lower() in ['tar.gz','tgz' ]:
        unzipTgz(zipfilePathAbs, unzipDirPathAbs,'allInOne')
    if zipfileType == 'zip': unzipZip(zipfilePathAbs, unzipDirPathAbs,'allInOne')
    if 'rar' in zipfileType  : unzipRar(zipfilePathAbs, unzipDirPathAbs, 'allInOne')
    if zipfileType == 'bz2': unzipBz2(zipfilePathAbs, unzipDirPathAbs, xmlFileName)






def unzipRar(zipFilePath,tagDirPath,unzipType):
    try:
        rarFile = rarfile.RarFile(zipFilePath)
        if unzipType !='allInOne':

            rarFile.extractall(tagDirPath)   # 按目录结构解压
            return tagDirPath
        else:
            if rarFile:                      # 所有文件解压到同一个目录下
                fileList = rarFile.infolist()
                for file in fileList:
                    if not file.isdir():
                        rarFile.extract(file, path=tagDirPath)
                        src = os.path.abspath(os.path.join(tagDirPath, file.filename))
                        dst = os.path.abspath(os.path.join(tagDirPath, file.filename.split('/')[-1]))
                        shutil.move(src, dst)
                return
            else:
                print('文件不是tar文件')
                return None
    except Exception as e:
        print(str(e))
        return None
    finally:
        rarFile.close()

"""
解压报错:
rarfile.RarCannotExec: Unrar not installed? (rarfile.UNRAR_TOOL='unrar')

rar压缩包的算法并不对外公开，所以其它软件想压缩或解压rar文件，必须通过cmd调用rar.exe。
所以，怀疑rarfile其实也是调用的rar.exe或unrar.exe
解决方案：
据winrar的目录中的unrar.exe，拷贝到我的python脚本目录下，再执行就ok了；
或者环境变量path中加入unrar.exe所在目录；
PyCharm的话，可以将unrar.exe复制到项目的venv/Scripts下。

"""







def unzipTgz(zipFilePath,tagDirPath,unzipType):
    try:
        tarFile = tarfile.open(zipFilePath)
        if unzipType !='allInOne':
            tarFile.extractall(tagDirPath)   # 按目录结构解压
            return tagDirPath
        else:
            if tarFile:                      # 所有文件解压到同一个目录下
                fileList = tarFile.getmembers()
                for file in fileList:
                    if file.isfile():
                        tarFile.extract(file.name, path=tagDirPath)
                        src = os.path.abspath(os.path.join(tagDirPath, file.name))
                        dst = os.path.abspath(os.path.join(tagDirPath, file.name.split('/')[-1]))
                        shutil.move(src, dst)
                return
            else:
                print('文件不是tar文件')
                return None
    except Exception as e:
        print(str(e))
        return None
    finally:
        tarFile.close()


def unzipZip(zipFilePath,tagDirPath,unzipType):
    # zipFilePath 为压缩包的绝对路径, tagDirPath 解压目录的绝对路径
    try:
        zipFile = zipfile.ZipFile(zipFilePath, 'r')
        if zipFile and unzipType !='allInOne':
            zipFile.extractall(tagDirPath)   # 按目录结构解压
            return tagDirPath

        elif zipFile and unzipType == 'allInOne':   # 解压到同一目录下
            for file in zipFile.infolist():
                if not file.is_dir():
                    zipFile.extract(file, tagDirPath)
                    src = os.path.abspath(os.path.join(tagDirPath, file.filename))
                    dst = os.path.abspath(os.path.join(tagDirPath, file.filename.split('/')[-1]))
                    shutil.move(src, dst)
            return tagDirPath

    except Exception as error:
        print(str(zipFilePath) + ': ' + str(error))
        return None
    finally:
        zipFile.close()





def unzipGz(zipFilePath,tagDirPath,xmlFileName):
    # zipFilePath 为压缩包的绝对路径, tagDirPath 解压目录的绝对路径,  xmlFileName 为解压后的文件名
    xmlFilePathAbs = os.path.join(tagDirPath,xmlFileName)
    try:
        with gzip.open(zipFilePath, 'rb') as f_in:
            with open(xmlFilePathAbs, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                print('解压缩：{}'.format(zipFilePath))
                return xmlFilePathAbs
    except Exception as error:
        print(str(zipFilePath) + ' ' + str(error))
        return ''




def unzipBz2(zipFilePath,tagDirPath,xmlFileName):
    # zipFilePath 为压缩包的绝对路径, tagDirPath 解压目录的绝对路径,  xmlFileName 为解压后的文件名
    xmlFilePathAbs = os.path.join(tagDirPath,xmlFileName)
    try:
        with bz2.open(zipFilePath, 'rb') as f_in:
            with open(xmlFilePathAbs, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                print('解压缩：{}'.format(zipFilePath))
                return xmlFilePathAbs
    except Exception as error:
        print(str(zipFilePath) + ' ' + str(error))
        return ''



def testMuP(arg1):
    print(arg1)






if __name__ == '__main__':
    # 最大的进程数为  为 CPU的核心数.
    po = Pool(cpu_count())

    # 创建文件夹
    unzipDirPath = os.path.abspath(unzipConf.xmlPath)
    if not os.path.exists(unzipDirPath):
        os.makedirs(unzipDirPath)

    # 获取压缩文件列表
    zipFileList = getFileList(unzipConf.zipPath, r'.+\.')

    def callback(x):
        print(' {}'.format(current_process().name, x))

    for zipFile in zipFileList:
        # 解压缩文件
        # unzipFile(zipFile, unzipDirPath)

        po.apply_async(unzipFile, args=(zipFile, unzipDirPath), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")