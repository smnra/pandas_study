# -*- coding: utf-8 -*-
# @Time       : 2020/6/30 11:37
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : parseXmlMrs.py
# @Software   : PyCharm
# @description: 本脚本的作用为 MRS XML 文件解析 并保存到CSV文件中






import os
import xml.etree.cElementTree as ET
from multiprocessing import Pool,current_process
from multiprocessing import cpu_count

try:
    import parseXmlConfig as xmlConf
except ModuleNotFoundError :
    import parsexml.parseXmlConfig as xmlConf


from getfile import getFileList



def readXML(xmlFileName):
    # 读取xml文件, 并解析内容到列表
    xmlDict = {'enbId': '',
               'startTime': '',
               'endTime': '',
               'reportTime': '',
               'period': '',
               'mr': []
               }
    if os.path.exists(xmlFileName):
        try:
            tree = ET.parse(xmlFileName)
            root = tree.getroot()
        except Exception as e:
            print(str(e))
            return xmlDict

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
            for measurementTag in measurementTagList:
                xmlDict['mr'].append(measurementTagToStr(measurementTag))
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
    mrName = measurementTag.get('mrName', '').replace("MR.","MRS_") + "_15MI" # mr的类型也是表名 例如 "MRS_RSRP_15MI", "MRS_RSRQ_15MI"  等

    objectTagList = measurementTag.findall("object")   # 获取object 标签列表
    if objectTagList:
        for objectTag in objectTagList:
            # 遍历 object 标签
            eci = objectTag.get('id', '')
            # 获取ECI (xpath  /measurement/object/@id )

            vTagList = objectTag.findall('v')
            vList = vList+ [[eci] + removeEndSpace(vTag.text," ").split(" ") for vTag in vTagList if vTagList]
            # MRS 数据
            # if vTagList:
            #     for vTag in vTagList:
            #         vList.append([eci] + removeEndSpace(vTag.text," ").split(" "))  # MRS 数据

    return {mrName: vList}


def removeEndSpace(str,chr):
    str = str.strip(chr)
    if str[-1] == chr:
        return removeEndSpace(str, chr)
    else:
        return str









def toCSV(xmlFile, csvPath):
    pid = current_process().pid
    pname = current_process().name
    print(' {},{},'.format(pname, pid))

    if not os.path.isdir(csvPath):
        os.makedirs(csvPath)

    if os.path.isfile(xmlFile):
        result = readXML(xmlFile)

        # 确定结果没有异常,且 存在数据.
        if len(result.get('mr','')) > 0:
            insertSqlPart1 = "'" + result.get('enbId','') + "'," + "'" + result.get('startTime','') + "'," + "'" + result.get('endTime','') + "'," + "'" + result.get('reportTime','') + "'," + result.get('period',0) + ','

            for mr in result['mr']:
                mrData =  list(mr.items())[0]
                csv = []
                for mdata in mrData[1]:
                    insertSqlPart2 = "'" + mdata[0] + "',"
                    insertSqlPart3 = ','.join(mdata[1:])

                    csv.append(insertSqlPart1 + insertSqlPart2 + insertSqlPart3 + '\n')

                mrsCsvPath = os.path.abspath(os.path.join(csvPath, mrData[0] + r'.csv'))
                with open(mrsCsvPath, 'a+', encoding=('utf-8')) as f:  # 保存本页内容到文件
                    f.writelines(csv)
                    print(mrsCsvPath)





if __name__ == '__main__':
    # 多进程消息回调函数
    def callback(x):
        pid = current_process().pid
        pname = current_process().name
        print(' {},{},{}'.format(pname,pid,x))

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
        # toCSV(xmlFileMrs, csvPathMrs)

        po.apply_async(toCSV, args=(xmlFileMrs,csvPathMrs), callback=callback)

    print("----start----")
    po.close()  # 关闭进程池，关闭后po不再接受新的请求
    po.join()  # 等待po中的所有子进程执行完成，必须放在close语句之后
    '''如果没有添加join()，会导致有的代码没有运行就已经结束了'''
    print("-----end-----")


