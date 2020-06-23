# -*- coding: utf-8 -*-
# @Time       : 2020/6/12 12:02
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : read_file.py
# @Software   : PyCharm
# @description: 本脚本的作用为

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib



# # 大文件读取方法一  每次读取固定的行数(chunksize=6),返回一个可迭代的文件读取对象
# fileReader = pd.read_csv('./mdt.csv',chunksize=30)
# for subDf in fileReader:
#     print(subDf)
#


# 注意使用chunksize 或者iterator 参数分块读入会将整个文件读入到一个Dataframe
# # 大文件读取方法二 ,  iterator=True 返回一个可迭代的文件读取对象
# fileReader = pd.read_csv('./mdt.csv', iterator=True)    # 使用迭代方式读取大文件
# loop = True
# while True:
#     try:
#         chunk = fileReader.get_chunk(100)
#         print(chunk)
#     except StopIteration:
#         loop = False
#         print("Iteration is stopped.")
#
#


# 将字符串转化为时间类型
# def parse_dates(x):
#     return dt.datetime.strptime(x,'%Y%m%d %H:%M:%S')


df = pd.read_csv('./mdt.csv',       # 读取的文件
                header=0,          # 指定 title 所在的行
                # index_col='MROID', # 指定列为索引
                index_col=None, # 不指定列索引, 即自动生成列索引

                # 指定需要读取的列
                usecols=['CITY', 'SDATE', 'GRIDX', 'GRIDY', 'S_RSRP','MROID'],

                # 需要读取的行数
                nrows = 1000,

                # 打印读取数据信息统计
                verbose = True,

                # 如果设定为True并且parse_dates 可用，那么pandas将尝试转换为日期类型，
                # 如果可以转换，转换方法并解析。在某些情况下会快5~10倍
                infer_datetime_format = True,
                parse_dates=['SDATE'],

                # 指定 解析时间列的函数名
                # date_parser=parse_dates,

                # 读取文件的字符集
                encoding = 'utf-8',

                # 指定某些列的数据类型
                dtype = {'CITY': object,
                         'GRIDX': np.float16,
                         'GRIDY': np.float16,
                         'S_RSRP': np.int16,
                         }

                )


print(df.info())


df_sort = df.sort_values(['GRIDX','GRIDY'],ascending=False)
# ['GRIDX','GRIDY'] 是需要排序的字段名(索引名) ,
# ascending=False 是倒序 True 是正序


# 直接对df['S_RSRP'] 调用max()方法
df['S_RSRP'].max()



# 生成 图表
# 实际上是生成了一个plt中的figure
df['S_RSRP'].plot()

# 获取数据集中的最大值，结果是973
MaxValue = df['S_RSRP'].max()
MinValue = df['S_RSRP'].min()
MeanValue = df['S_RSRP'].mean()



# 得到具有最高出生次数的名字,这里的结果只有一个Mel，在最大值不唯一的情况下返回列表
MaxName = df['MROID'][df['S_RSRP'] == MinValue].values[0]
MaxName_1 = df[['GRIDX','GRIDY']][df['S_RSRP'] == MinValue].values


# 传入标注的文字
Text = str(MaxValue) + ' - ' + MaxName


# 加入注释
plt.annotate(Text,xy=(MinValue,MaxValue),xytext=(8,0),xycoords=('axes fraction','data'),textcoords='offset points')

plt.show()



# group by
groupFlied = df.groupby('GRIDX')        # 按 'GRIDX' 列分组 统计
groupFlied_1 = df.groupby(['GRIDX','GRIDY']) # 按 ['GRIDX','GRIDY']列分组统计

groupFlied.count()     #分组计数
groupFlied.sum()     #分组求和



#  修改列数据
# 使用 匿名函数修改 S_RSRP的值
# lambda 为 如果参数如果 x>=-100 则值为x 本身 否则为 -100
df['S_RSRP'] = df.S_RSRP.apply(lambda x: x if x>=-100 else -100)

# lambda 为 如果参数如果 x=='xianyang'  修改为 大写 'XIANYANG'
df['CITY'] = df.CITY.apply(lambda x: x if x=='XIANYANG' else x.upper())
pass


name = df.groupby('S_RSRP')