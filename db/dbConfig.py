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

# sql文件所在目录
sqlPath = os.path.abspath('.\\mr_data\\sql\\')


# csv文件保存目录
csvPath = os.path.abspath('.\\mr_data\\csv\\')

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
	period VARCHAR2(32),
	eci VARCHAR2(32),
	MmeUeS1apId  VARCHAR2(32),
	MmeGroupId VARCHAR2(32),
	MmeCode VARCHAR2(32),
	TimeStamp2 VARCHAR2(32),
	MR_LteScRSRP VARCHAR2(32),
	MR_LteNcRSRP VARCHAR2(32),
	MR_LteScRSRQ VARCHAR2(32),
	MR_LteNcRSRQ VARCHAR2(32),
	MR_LteScTadv VARCHAR2(32),
	MR_LteScPHR VARCHAR2(32),
	MR_LteScAOA VARCHAR2(32),
	MR_LteScPlrULQci1 VARCHAR2(32),
	MR_LteScPlrULQci2 VARCHAR2(32),
	MR_LteScPlrULQci3 VARCHAR2(32),
	MR_LteScPlrULQci4 VARCHAR2(32),
	MR_LteScPlrULQci5 VARCHAR2(32),
	MR_LteScPlrULQci6 VARCHAR2(32),
	MR_LteScPlrULQci7 VARCHAR2(32),
	MR_LteScPlrULQci8 VARCHAR2(32),
	MR_LteScPlrULQci9 VARCHAR2(32),
	MR_LteScPlrDLQci1 VARCHAR2(32),
	MR_LteScPlrDLQci2 VARCHAR2(32), 
	MR_LteScPlrDLQci3 VARCHAR2(32), 
	MR_LteScPlrDLQci4 VARCHAR2(32), 
	MR_LteScPlrDLQci5 VARCHAR2(32), 
	MR_LteScPlrDLQci6 VARCHAR2(32), 
	MR_LteScPlrDLQci7 VARCHAR2(32), 
	MR_LteScPlrDLQci8 VARCHAR2(32), 
	MR_LteScPlrDLQci9 VARCHAR2(32),
	MR_LteScSinrUL VARCHAR2(32),
	MR_LteScEarfcn VARCHAR2(32), 
	MR_LteScPci VARCHAR2(32),
	MR_LteSccgi VARCHAR2(32),
	MR_LteNcEarfcn VARCHAR2(32),
	MR_LteNcPci VARCHAR2(32), 
	MR_GsmNcellBcch VARCHAR2(32), 
	MR_GsmNcellCarrierRSSI VARCHAR2(32), 
	MR_GsmNcellNcc VARCHAR2(32), 
	MR_GsmNcellBcc VARCHAR2(32), 
	MR_UtraCpichRSCP VARCHAR2(32), 
	MR_UtraCpichEcNo VARCHAR2(32), 
	MR_UtraCellParameterId VARCHAR2(32), 
	MR_Longitude VARCHAR2(32), 
	MR_Latitude VARCHAR2(32)
);

"""


MRO_RIP_15MI_Str_ora = """
create table if not exists MRO_RIP_15MI as
(
	enbId VARCHAR2(32),
	startTime VARCHAR2(32),
	endTime VARCHAR2(32),
	reportTime VARCHAR2(32),
	period VARCHAR2(32),
	eci VARCHAR2(32),
	MmeUeS1apId  VARCHAR2(32),
	MmeGroupId VARCHAR2(32),
	MmeCode VARCHAR2(32),
	TimeStamp2 VARCHAR2(32),
	MR_LteScRIP VARCHAR2(32)
);

"""


MRE_15MI_Str_ora = """
create table if not exists MRE_15MI as
(
	enbId VARCHAR2(32),
	startTime VARCHAR2(32),
	endTime VARCHAR2(32),
	reportTime VARCHAR2(32),
	period VARCHAR2(32),
	eci VARCHAR2(32),
	MmeUeS1apId  VARCHAR2(32),
	MmeGroupId VARCHAR2(32),
	MmeCode VARCHAR2(32),
	TimeStamp2 VARCHAR2(32),
	EventType VARCHAR2(8),
	MR_LteScRSRP  VARCHAR2(32),
	MR_LteScRSRQ  VARCHAR2(32),
	MR_LteScEarfcn  VARCHAR2(32),
	MR_LteScPci  VARCHAR2(32),
	MR_LteScCgi    VARCHAR2(32),
	MR_LteNcRSRP  VARCHAR2(32),
	MR_LteNcRSRQ  VARCHAR2(32),
	MR_LteNcEarfcn VARCHAR2(32),
	MR_LteNcPci  VARCHAR2(32),
	MR_GsmNcellBcch  VARCHAR2(32),
	MR_GsmNcellCarrierRSSI  VARCHAR2(32),
	MR_GsmNcellNcc  VARCHAR2(32),
	MR_GsmNcellBcc  VARCHAR2(32),
	MR_UtraCpichRSCP  VARCHAR2(32),
	MR_UtraCarrierRSSI  VARCHAR2(32),
	MR_UtraCpichEcNo  VARCHAR2(32),
	MR_UtraCellParameterId VARCHAR2(32)
);
"""




MRO_15MI_Str_pg = """
create table if not exists MRO_15MI
(
	enbId text,
	startTime text,
	endTime text,
	reportTime text,
	period text,
	eci text,
	MmeUeS1apId  text,
	MmeGroupId text,
	MmeCode text,
	TimeStamp2 text,
	MR_LteScRSRP text,
	MR_LteNcRSRP texttext,
	MR_LteScRSRQ text,
	MR_LteNcRSRQ text,
	MR_LteScTadv text,
	MR_LteScPHR text,
	MR_LteScAOA text,
	MR_LteScPlrULQci1 text,
	MR_LteScPlrULQci2 text,
	MR_LteScPlrULQci3 text,
	MR_LteScPlrULQci4 text,
	MR_LteScPlrULQci5 text,
	MR_LteScPlrULQci6 text,
	MR_LteScPlrULQci7 text,
	MR_LteScPlrULQci8 text,
	MR_LteScPlrULQci9 text,
	MR_LteScPlrDLQci1 text,
	MR_LteScPlrDLQci2 text, 
	MR_LteScPlrDLQci3 text, 
	MR_LteScPlrDLQci4 text, 
	MR_LteScPlrDLQci5 text, 
	MR_LteScPlrDLQci6 text, 
	MR_LteScPlrDLQci7 text, 
	MR_LteScPlrDLQci8 text, 
	MR_LteScPlrDLQci9 text,
	MR_LteScSinrUL text,
	MR_LteScEarfcn text, 
	MR_LteScPci text,
	MR_LteSccgi text,
	MR_LteNcEarfcn text,
	MR_LteNcPci text, 
	MR_GsmNcellBcch text, 
	MR_GsmNcellCarrierRSSI text, 
	MR_GsmNcellNcc text, 
	MR_GsmNcellBcc text, 
	MR_UtraCpichRSCP text, 
	MR_UtraCpichEcNo text, 
	MR_UtraCellParameterId text, 
	MR_Longitude text, 
	MR_Latitude text
);

"""


MRO_RIP_15MI_Str_pg = """
create table if not exists MRO_RIP_15MI
(
	enbId text,
	startTime text,
	endTime text,
	reportTime text,
	period text,
	eci text,
	MmeUeS1apId  text,
	MmeGroupId text,
	MmeCode text,
	TimeStamp2 text,
	MR_LteScRIP text
);

"""


MRE_15MI_Str_pg = """
create table if not exists MRE_15MI
(
	enbId text,
	startTime text,
	endTime text,
	reportTime text,
	period text,
	eci text,
	MmeUeS1apId text,
	MmeGroupId text,
	MmeCode text,
	TimeStamp2 text,
	EventType text,
	MR_LteScRSRP text,
	MR_LteScRSRQ text,
	MR_LteScEarfcn text,
	MR_LteScPci text,
	MR_LteScCgi text,
	MR_LteNcRSRP text,
	MR_LteNcRSRQ  text,
	MR_LteNcEarfcn text,
	MR_LteNcPci  text,
	MR_GsmNcellBcch  text,
	MR_GsmNcellCarrierRSSI  text,
	MR_GsmNcellNcc  text,
	MR_GsmNcellBcc  text,
	MR_UtraCpichRSCP  text,
	MR_UtraCarrierRSSI  text,
	MR_UtraCpichEcNo  text,
	MR_UtraCellParameterId text
);
"""

