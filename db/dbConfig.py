# -*- coding: utf-8 -*-
# @Time       : 2020/7/1 10:36
# @Author     : SMnRa
# @Email      : smnra@163.com
# @File       : dbConfig.py
# @Software   : PyCharm
# @description: 本脚本的作用为 数据库 配置文件





import os

# xml文件所在目录
xmlPath = os.path.abspath('.\\mr_data\\xml\\')


# csv文件保存目录
csvPathMro = os.path.abspath('.\\mr_data\\csv\\mro\\')
csvPathMrs = os.path.abspath('.\\mr_data\\csv\\mrs\\')
csvPathMre = os.path.abspath('.\\mr_data\\csv\\mre\\')

# 当前使用的数据库是 oracle 还是 postgres
usedDbType = 'postgres'


# 数据库服务器连接参数
postgresServer = {'host': '10.231.143.105',
                  'port': '5432',
                  'user': 'postgres',
                  'password': 'root',
                  'dbname': 'postgres'
                  }

oracleServer = {'host': '10.231.142.8',
                'port': '1521',
                'user': 'c##fast',
                'password': 'fast*123',
                'servername': 'fast'
                }

# 建表语句
MRO_15MI_Str_ora = """
create table  if not exists MRO_15MI as
(
	enbId VARCHAR2(32),
	startTime VARCHAR2(32),
	endTime VARCHAR2(32),
	reportTime VARCHAR2(32),
	period NUMBER,
	eci VARCHAR2(32),
	MmeUeS1apId  NUMBER,
	MmeGroupId NUMBER,
	MmeCode NUMBER,
	TimeStamp2 VARCHAR2(32),
	MR_LteScRSRP NUMBER,
	MR_LteNcRSRP NUMBER,
	MR_LteScRSRQ NUMBER,
	MR_LteNcRSRQ NUMBER,
	MR_LteScTadv NUMBER,
	MR_LteScPHR NUMBER,
	MR_LteScAOA NUMBER,
	MR_LteScPlrULQci1 NUMBER,
	MR_LteScPlrULQci2 NUMBER,
	MR_LteScPlrULQci3 NUMBER,
	MR_LteScPlrULQci4 NUMBER,
	MR_LteScPlrULQci5 NUMBER,
	MR_LteScPlrULQci6 NUMBER,
	MR_LteScPlrULQci7 NUMBER,
	MR_LteScPlrULQci8 NUMBER,
	MR_LteScPlrULQci9 NUMBER,
	MR_LteScPlrDLQci1 NUMBER,
	MR_LteScPlrDLQci2 NUMBER, 
	MR_LteScPlrDLQci3 NUMBER, 
	MR_LteScPlrDLQci4 NUMBER, 
	MR_LteScPlrDLQci5 NUMBER, 
	MR_LteScPlrDLQci6 NUMBER, 
	MR_LteScPlrDLQci7 NUMBER, 
	MR_LteScPlrDLQci8 NUMBER, 
	MR_LteScPlrDLQci9 NUMBER,
	MR_LteScSinrUL NUMBER,
	MR_LteScEarfcn NUMBER, 
	MR_LteScPci NUMBER,
	MR_LteSccgi NUMBER,
	MR_LteNcEarfcn NUMBER,
	MR_LteNcPci NUMBER, 
	MR_GsmNcellBcch NUMBER, 
	MR_GsmNcellCarrierRSSI NUMBER, 
	MR_GsmNcellNcc NUMBER, 
	MR_GsmNcellBcc NUMBER, 
	MR_UtraCpichRSCP NUMBER, 
	MR_UtraCpichEcNo NUMBER, 
	MR_UtraCellParameterId NUMBER, 
	MR_Longitude NUMBER, 
	MR_Latitude NUMBER
);

"""


MRO_RIP_15MI_Str_ora = """
create table if not exists MRO_RIP_15MI as
(
	enbId VARCHAR2(32),
	startTime VARCHAR2(32),
	endTime VARCHAR2(32),
	reportTime VARCHAR2(32),
	period NUMBER,
	eci VARCHAR2(32),
	MmeUeS1apId  NUMBER,
	MmeGroupId NUMBER,
	MmeCode NUMBER,
	TimeStamp2 VARCHAR2(32),
	MR_LteScRIP NUMBER
);

"""


MRE_15MI_Str_ora = """
create table if not exists MRE_15MI as
(
	enbId VARCHAR2(32),
	startTime VARCHAR2(32),
	endTime VARCHAR2(32),
	reportTime VARCHAR2(32),
	period NUMBER,
	eci VARCHAR2(32),
	MmeUeS1apId  NUMBER,
	MmeGroupId NUMBER,
	MmeCode NUMBER,
	TimeStamp2 VARCHAR2(32),
	EventType VARCHAR2(8),
	MR_LteScRSRP  NUMBER,
	MR_LteScRSRQ  NUMBER,
	MR_LteScEarfcn  NUMBER,
	MR_LteScPci  NUMBER,
	MR_LteScCgi    VARCHAR2(32),
	MR_LteNcRSRP  NUMBER,
	MR_LteNcRSRQ  NUMBER,
	MR_LteNcEarfcn NUMBER,
	MR_LteNcPci  NUMBER,
	MR_GsmNcellBcch  NUMBER,
	MR_GsmNcellCarrierRSSI  NUMBER,
	MR_GsmNcellNcc  NUMBER,
	MR_GsmNcellBcc  NUMBER,
	MR_UtraCpichRSCP  NUMBER,
	MR_UtraCarrierRSSI  NUMBER,
	MR_UtraCpichEcNo  NUMBER,
	MR_UtraCellParameterId NUMBER
);
"""




MRO_15MI_Str_pg = """
create table if not exists MRO_15MI
(
	enbId VARCHAR(32),
	startTime VARCHAR(32),
	endTime VARCHAR(32),
	reportTime VARCHAR(32),
	period integer,
	eci VARCHAR(32),
	MmeUeS1apId  integer,
	MmeGroupId integer,
	MmeCode integer,
	TimeStamp2 VARCHAR(32),
	MR_LteScRSRP integer,
	MR_LteNcRSRP integer,
	MR_LteScRSRQ integer,
	MR_LteNcRSRQ integer,
	MR_LteScTadv integer,
	MR_LteScPHR integer,
	MR_LteScAOA integer,
	MR_LteScPlrULQci1 integer,
	MR_LteScPlrULQci2 integer,
	MR_LteScPlrULQci3 integer,
	MR_LteScPlrULQci4 integer,
	MR_LteScPlrULQci5 integer,
	MR_LteScPlrULQci6 integer,
	MR_LteScPlrULQci7 integer,
	MR_LteScPlrULQci8 integer,
	MR_LteScPlrULQci9 integer,
	MR_LteScPlrDLQci1 integer,
	MR_LteScPlrDLQci2 integer, 
	MR_LteScPlrDLQci3 integer, 
	MR_LteScPlrDLQci4 integer, 
	MR_LteScPlrDLQci5 integer, 
	MR_LteScPlrDLQci6 integer, 
	MR_LteScPlrDLQci7 integer, 
	MR_LteScPlrDLQci8 integer, 
	MR_LteScPlrDLQci9 integer,
	MR_LteScSinrUL integer,
	MR_LteScEarfcn integer, 
	MR_LteScPci integer,
	MR_LteSccgi integer,
	MR_LteNcEarfcn integer,
	MR_LteNcPci integer, 
	MR_GsmNcellBcch integer, 
	MR_GsmNcellCarrierRSSI integer, 
	MR_GsmNcellNcc integer, 
	MR_GsmNcellBcc integer, 
	MR_UtraCpichRSCP integer, 
	MR_UtraCpichEcNo integer, 
	MR_UtraCellParameterId integer, 
	MR_Longitude integer, 
	MR_Latitude integer
);

"""


MRO_RIP_15MI_Str_pg = """
create table if not exists MRO_RIP_15MI
(
	enbId VARCHAR(32),
	startTime VARCHAR(32),
	endTime VARCHAR(32),
	reportTime VARCHAR(32),
	period integer,
	eci VARCHAR(32),
	MmeUeS1apId  integer,
	MmeGroupId integer,
	MmeCode integer,
	TimeStamp2 VARCHAR(32),
	MR_LteScRIP integer
);

"""


MRE_15MI_Str_pg = """
create table if not exists MRE_15MI
(
	enbId VARCHAR(32),
	startTime VARCHAR(32),
	endTime VARCHAR(32),
	reportTime VARCHAR(32),
	period integer,
	eci VARCHAR(32),
	MmeUeS1apId  integer,
	MmeGroupId integer,
	MmeCode integer,
	TimeStamp2 VARCHAR(32),
	EventType VARCHAR(8),
	MR_LteScRSRP  integer,
	MR_LteScRSRQ  integer,
	MR_LteScEarfcn  integer,
	MR_LteScPci  integer,
	MR_LteScCgi    VARCHAR(32),
	MR_LteNcRSRP  integer,
	MR_LteNcRSRQ  integer,
	MR_LteNcEarfcn integer,
	MR_LteNcPci  integer,
	MR_GsmNcellBcch  integer,
	MR_GsmNcellCarrierRSSI  integer,
	MR_GsmNcellNcc  integer,
	MR_GsmNcellBcc  integer,
	MR_UtraCpichRSCP  integer,
	MR_UtraCarrierRSSI  integer,
	MR_UtraCpichEcNo  integer,
	MR_UtraCellParameterId integer
);
"""

