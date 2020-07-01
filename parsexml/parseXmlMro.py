# -*- coding: utf-8 -*-
# @Time       : 2020/6/30 14:02
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : parseXmlMro.py
# @Software   : PyCharm
# @description: 本脚本的作用为 解析 MRO 文件






import os,time
import xml.etree.cElementTree as ET
from getfile import getFileList

from multiprocessing import Pool,current_process
from multiprocessing import cpu_count


try:
    import parseXmlConfig as xmlConf
except ModuleNotFoundError :
    import parsexml.parseXmlConfig as xmlConf








def readXML(xmlFileName):
    # 读取xml文件, 并解析内容到列表
    xmlDict = {'enbId': '',
               'startTime': '',
               'endTime': '',
               'reportTime': '',
               'period': '',
               'mr': [],
               'rip': []
               }


    if os.path.exists(xmlFileName):
        tree = ET.parse(xmlFileName)
        root = tree.getroot()

        # fileHeader标签内容提取
        fileHeaderTag = root.find("fileHeader")    #查找 fileHeader 标签, 返回值为 标签属性节点的 字典
        xmlDict['startTime'] = fileHeaderTag.get('startTime','').replace("T"," ").split(".")[0]
        xmlDict['endTime'] = fileHeaderTag.get('endTime','').replace("T"," ").split(".")[0]
        xmlDict['reportTime'] = fileHeaderTag.get('reportTime','').replace("T"," ").split(".")[0]
        xmlDict['period'] = fileHeaderTag.get('period','')

        # eNB 标签内容提取
        enbTag = root.find("eNB")
        xmlDict['enbId'] = enbTag.get('id','')

        # measurement 标签提取
        if enbTag and len(xmlDict['startTime'])==19:
            measurementTagList = enbTag.findall("measurement")
            xmlDict['mr'].append(measurementTagToStr(measurementTagList[0]))
            xmlDict['rip'].append(measurementRipTagToStr(measurementTagList[1]))
            # print(xmlDict)
        else:
            print("异常数据:{}!".format(xmlFileName))

        print("解析完成:{}".format(xmlFileName))

    else:
        print("File Not Found!")

    return xmlDict


def measurementTagToStr(measurementTag):
    # 解析 measurement 标签信息 为 字典
    vList = []
    mrName = "MRO_15MI" # mr的类型也是表名 例如 "MRO_15MI"

    objectTagList = measurementTag.findall("object")   # 获取object 标签列表
    if objectTagList:
        for objectTag in objectTagList:
            # 遍历 object 标签
            eci = objectTag.get('id', '')
            mmeUeS1apId = objectTag.get('MmeUeS1apId', '')
            mmeGroupId = objectTag.get('MmeGroupId', '')
            mmeCode = objectTag.get('MmeCode', '')
            timeStamp = "'" + objectTag.get('TimeStamp', '').replace("T"," ").split(".")[0] + "'"

            # 获取ECI MmeUeS1apId  MmeGroupId  MmeCode  TimeStamp . (xpath  /measurement/object/@id )

            vTagList = objectTag.findall('v')
            vList = vList+ [[eci,mmeUeS1apId,mmeGroupId,mmeCode,timeStamp] +
                            removeEndSpace(vTag.text," ").split(" ") for vTag in vTagList if vTagList]

    return {mrName: vList}


def measurementRipTagToStr(measurementTag):
    # 解析 RIP 标签
    vList = []
    mrName = "MRO_RIP_15MI" # mr的类型也是表名 例如 "MRO_15MI"

    objectTagList = measurementTag.findall("object")   # 获取object 标签列表
    if objectTagList:
        for objectTag in objectTagList:
            # 遍历 object 标签
            eci = objectTag.get('id', '')
            mmeUeS1apId = objectTag.get('MmeUeS1apId', '')
            mmeGroupId = objectTag.get('MmeGroupId', '')
            mmeCode = objectTag.get('MmeCode', '')
            timeStamp = "'" + objectTag.get('TimeStamp', '').replace("T"," ").split(".")[0] +"'"

            # 获取ECI MmeUeS1apId  MmeGroupId  MmeCode  TimeStamp . (xpath  /measurement/object/@id )

            vTagList = objectTag.findall('v')
            vList = vList+ [[eci,mmeUeS1apId,mmeGroupId,mmeCode,timeStamp] +
                            removeEndSpace(vTag.text," ").split(" ") for vTag in vTagList if vTagList]

    return {mrName: vList}



def removeEndSpace(str,chr):
    str = str.strip(chr)
    if str[-1] == chr:
        return removeEndSpace(str, chr)
    else:
        return str





def toCSV(xmlFile,csvPath):
    start = time.time()

    valueCount = 0
    # 插入数据条数的计数器  超过500条 commit

    # 创建文件夹
    if not os.path.isdir(csvPath):os.makedirs(csvPath)


    if os.path.isfile(xmlFile):
        result = readXML(xmlFile)

        if len(result.get('mr','')) > 0:
            mroStrPart1 = "'" + result.get('enbId','') + "'," + "'" +\
                             result.get('startTime','') + "'," + "'" + \
                             result.get('endTime','') + "'," + "'" + \
                             result.get('reportTime','') + "'," + \
                             result.get('period',0) + ','


            for mr in result['mr']:   # 处理 result['mr']
                csv = []
                mrData =  list(mr.items())[0]
                for mdata in mrData[1]:
                    # 拼接insert 语句
                    mroStrPart2 = "'" + mdata[0] + "',"
                    mroStrPart3 = ','.join(mdata[1:])
                    mroStr = mroStrPart1 + mroStrPart2 + mroStrPart3 + '\n'
                    csv.append(mroStr.replace("NIL",""))

                mroCsvPath = os.path.abspath(os.path.join(csvPath, mrData[0] + r'.csv'))
                with open(mroCsvPath, 'a+', encoding=('utf-8')) as f:  # 保存本页内容到文件
                    f.writelines(csv)

            print("导出CSV文件完成:", mroCsvPath)



            for rip in result['rip']:   # 处理 result['rip']
                csvRip = []
                ripData = list(rip.items())[0]
                for mdata in ripData[1]:
                    # 拼接insert 语句
                    mroRipStrPart2 = "'" + mdata[0] + "',"
                    mroRipStrPart3 = ','.join(mdata[1:])
                    mroRipStr = mroStrPart1 + mroRipStrPart2 + mroRipStrPart3 + '\n'
                    csvRip.append(mroRipStr.replace("NIL", ""))

                ripCsvPath = os.path.abspath(os.path.join(csvPath, ripData[0] + r'.csv'))
                with open(ripCsvPath, 'a+', encoding=('utf-8')) as f:  # 保存本页内容到文件
                    f.writelines(csvRip)

            print("导出CSV文件完成:", ripCsvPath)

    print("xml文件导出完成:",str(time.time() - start))
    return xmlFile


if __name__ =="__main__":
    # 多进程消息回调函数
    def callback(x):
        print(' {}'.format(current_process().name,x))

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
        # result = toCSV(xmlFile,csvPath)      # 解析xml,并保存为csv文件

        # 多进程解析xml,并保存为csv文件
        po.apply_async(toCSV, args=(xmlFileMro,csvPathMro), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")
