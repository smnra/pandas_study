--创建 postgis 插件
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;

SET enable_seqscan TO on;
SET enable_indexscan TO on;
set force_parallel_mode =on;
set max_parallel_workers_per_gather =0;


-- drop table if EXISTS UE1_DT_Date_temp;
--建表 DT数据导入临时表
CREATE TABLE UE1_DT_Date_temp
(
    No varchar(255),
    UETime varchar(255),
    PCTime varchar(255),
    Lon varchar(255),
    Lat varchar(255),
    TAC varchar(255),
    "eNodeB ID" varchar(255),
    ECI varchar(255),
    "Cell ID" varchar(255),
    SectorID varchar(255),
    "EARFCN DL" varchar(255),
    PCI varchar(255),
    RSRP varchar(255),
    RSRQ varchar(255),
    SINR varchar(255),
    "Cell 1st EARFCN" varchar(255),
    "Cell 1st PCI" varchar(255),
    "Cell 1st RSRP" varchar(255),

    "Cell 2nd EARFCN" varchar(255),
    "Cell 2nd PCI" varchar(255),
    "Cell 2nd RSRP" varchar(255),

    "Cell 3rd EARFCN" varchar(255),
    "Cell 3rd PCI" varchar(255),
    "Cell 3rd RSRP" varchar(255),
 
    "Cell 4th EARFCN" varchar(255),
    "Cell 4th PCI" varchar(255),
    "Cell 4th RSRP" varchar(255),

    "Cell 5th EARFCN" varchar(255),
    "Cell 5th PCI" varchar(255),
    "Cell 5th RSRP" varchar(255),

    "Cell 6th EARFCN" varchar(255),
    "Cell 6th PCI" varchar(255),
    "Cell 6th RSRP" varchar(255)
);
alter table UE1_DT_Date_temp owner to postgres;

--向临时表里导入数据
copy UE1_DT_Date_temp FROM 'D:\DT\UE1_1.csv' CSV HEADER ENCODING 'gbk' DELIMITER ','  NULL '' quote '"';
copy UE1_DT_Date_temp FROM 'D:\DT\UE1_2.csv' CSV HEADER ENCODING 'gbk' DELIMITER ','  NULL '' quote '"';

/*copy UE1_DT_Date_temp FROM 'D:\DT\UE2_1.csv' CSV HEADER ENCODING 'gbk' DELIMITER ','  NULL '' quote '"';
copy UE1_DT_Date_temp FROM 'D:\DT\UE2_2.csv' CSV HEADER ENCODING 'gbk' DELIMITER ','  NULL '' quote '"';
*/


--修改 UE1_DT_Date_temp 表 的类型然后创建到表 UE1_DT_Date 中


-- drop table  if EXISTS UE1_DT_Date;
create table UE1_DT_Date as
    (select
        case when No='' then null else to_number(No,'9999999999999999') end as No,
--         case when UETime='' then null else to_date(UETime,'hh24:mi:ss.ms') end as UETime,
--         case when PCTime='' then null else to_date(PCTime,'hh24:mi:ss.ms') end as PCTime,
        case when UETime='' then null else to_timestamp('2018-01-01 '||UETime,'yyyy-MM-dd hh24:mi:ss.ms') end as UETime,
        case when PCTime='' then null else to_timestamp('2018-01-01 '||PCTime,'yyyy-MM-dd hh24:mi:ss.ms') end as PCTime,
        case when Lon='' then null else Lon::numeric end as Lon,
        case when Lat='' then null else Lat::numeric end as Lat,
        case when TAC='' then null else to_number(TAC,'9999999999999999') end as TAC,
        case when "eNodeB ID"='' then null else to_number("eNodeB ID",'9999999999999999') end as "eNodeB ID",
        case when ECI='' then null else to_number(ECI,'9999999999999999') end as ECI,
        case when "Cell ID"='' then null else to_number("Cell ID",'9999999999999999') end as "Cell ID",
        case when SectorID='' then null else to_number(SectorID,'9999999999999999') end as SectorID,
        case when "EARFCN DL"='' then null else to_number("EARFCN DL",'9999999999999999') end as "EARFCN DL",
        case when PCI='' then null else to_number(PCI,'9999999999999999') end as PCI,
        case when RSRP='' then null else RSRP::numeric end as RSRP,
        case when RSRQ='' then null else RSRQ::numeric end as RSRQ,
        case when SINR='' then null else SINR::numeric end as SINR,

        case when "Cell 1st EARFCN"='' then null else to_number("Cell 1st EARFCN",'9999999999999999') end as "Cell 1st EARFCN",
        case when "Cell 1st PCI"='' then null else to_number("Cell 1st PCI",'9999999999999999') end as "Cell 1st PCI",
        case when "Cell 1st RSRP"='' then null else "Cell 1st RSRP"::numeric end as "Cell 1st RSRP",

        case when "Cell 2nd EARFCN"='' then null else to_number("Cell 2nd EARFCN",'9999999999999999') end as "Cell 2nd EARFCN",
        case when "Cell 2nd PCI"='' then null else to_number("Cell 2nd PCI",'9999999999999999') end as "Cell 2nd PCI",
        case when "Cell 2nd RSRP"='' then null else "Cell 2nd RSRP"::numeric end as "Cell 2nd RSRP",

        case when "Cell 3rd EARFCN"='' then null else to_number("Cell 3rd EARFCN",'9999999999999999') end as "Cell 3rd EARFCN",
        case when "Cell 3rd PCI"='' then null else to_number("Cell 3rd PCI",'9999999999999999') end as "Cell 3rd PCI",
        case when "Cell 3rd RSRP"='' then null else "Cell 3rd RSRP"::numeric end as "Cell 3rd RSRP",

        case when "Cell 4th EARFCN"='' then null else to_number("Cell 4th EARFCN",'9999999999999999') end as "Cell 4th EARFCN",
        case when "Cell 4th PCI"='' then null else to_number("Cell 4th PCI",'9999999999999999') end as "Cell 4th PCI",
        case when "Cell 4th RSRP"='' then null else "Cell 4th RSRP"::numeric end as "Cell 4th RSRP",

        case when "Cell 5th EARFCN"='' then null else to_number("Cell 5th EARFCN",'9999999999999999') end as "Cell 5th EARFCN",
        case when "Cell 5th PCI"='' then null else to_number("Cell 5th PCI",'9999999999999999') end as "Cell 5th PCI",
        case when "Cell 5th RSRP"='' then null else "Cell 5th RSRP"::numeric end as "Cell 5th RSRP",

        case when "Cell 6th EARFCN"='' then null else to_number("Cell 6th EARFCN",'9999999999999999') end as "Cell 6th EARFCN",
        case when "Cell 6th PCI"='' then null else to_number("Cell 6th PCI",'9999999999999999') end as "Cell 6th PCI",
        case when "Cell 6th RSRP"='' then null else "Cell 6th RSRP"::numeric end as "Cell 6th RSRP"
    -- select*
    from
         UE1_DT_Date_temp
    );

--清理临时表 UE1_DT_Date_temp
drop table UE1_DT_Date_temp;
select count(*) from UE1_DT_Date;



-- drop table if EXISTS sample_point;
-- 创建点
create table  sample_point as
    (select
        ROW_NUMBER() OVER() as No,
        sample.UETime,
        sample.PCTime,
        sample.Lon,
        sample.Lat,
        sample.TAC,
        sample."eNodeB ID",
        sample.ECI,
        sample."Cell ID",
        sample.SectorID,
        sample."EARFCN DL",
        sample.PCI,
        sample.RSRP,
        sample.RSRQ,
        sample.SINR,
        sample."Cell 1st EARFCN",
        sample."Cell 1st PCI",
        sample."Cell 1st RSRP",
        
        sample."Cell 2nd EARFCN",
        sample."Cell 2nd PCI",
        sample."Cell 2nd RSRP",
        
        sample."Cell 3rd EARFCN",
        sample."Cell 3rd PCI",
        sample."Cell 3rd RSRP",
        
        sample."Cell 4th EARFCN",
        sample."Cell 4th PCI",
        sample."Cell 4th RSRP",
        
        sample."Cell 5th EARFCN",
        sample."Cell 5th PCI",
        sample."Cell 5th RSRP",
        
        sample."Cell 6th EARFCN",
        sample."Cell 6th PCI",
        sample."Cell 6th RSRP",
        ST_PointFromText(
            'POINT(' || to_char(sample.Lon, '999.9999999') || to_char(sample.Lat, '999.9999999') || ')',
        4326) as point
    from UE1_DT_Date sample
    
    where
        PCTime is not null
       and Lon is not null
       and Lat is not null
       and eci is not null
       and "EARFCN DL" is not null
--        and "eNodeB ID" = 773891

);

/* 创建空间索引 */
CREATE INDEX point_gist ON sample_point USING gist(point);






-- 导入小区工参数据
-- drop table "4G-小区级工参";
CREATE TABLE "4G-小区级工参"
(
    网元编号 TEXT,
    小区别名 TEXT,
    小区标识 TEXT,
    小区网管名称 TEXT,
    省份 TEXT,
    所属城市 TEXT,
    所属区县 TEXT,
    乡镇 TEXT,
    所属eNodeB标识 INT,
    小区标识码 INT,
    所属扇区编号 TEXT,
    设备厂家EutranCell标识 TEXT,
    设备厂家 TEXT,
    所属行政区域类型 TEXT,
    是否采集MR TEXT,
    经度 NUMERIC,
    纬度 NUMERIC,
    相关联的天线列表 TEXT,
    天线数 INT,
    双工方式 TEXT,
    采用的cp类型 TEXT,
    子帧配置类型 TEXT,
    特殊子帧配置类型 TEXT,
    是否为RRU小区 TEXT,
    上行频点 NUMERIC,
    下行频点 NUMERIC,
    物理小区识别码 INT,
    物理小区识别码列表 TEXT,
    小区配置的载频发射功率 TEXT,
    "参考信号（RS）的每RE平均发射功率" TEXT,
    A类符号上每RE平均功率与RS占用的RE平均功率的 TEXT,
    B类符号上每RE平均功率与RS占用的RE平均功率的 TEXT,
    A类符号功率比值 TEXT,
    B类符号功率比值 TEXT,
    广播信道功率 TEXT,
    最大传输功率 TEXT,
    跟踪区编码 INT,
    跟踪区列表 INT,
    运行状态 TEXT,
    状态变更时间 TEXT,
    小区覆盖类型 TEXT,
    小区覆盖范围 TEXT,
    PLMN标识的列表 TEXT,
    小区MBMS开关 TEXT,
    频段指示 TEXT,
    中心载频的信道号 TEXT,
    带宽 TEXT,
    下行循环前缀长度 TEXT,
    上行循环前缀长度 TEXT,
    上行带宽 TEXT,
    下行带宽 TEXT,
    小区激活状态 TEXT,
    高速小区指示 TEXT,
    发送和接收模式 TEXT,
    工作模式 TEXT,
    前导格式 TEXT,
    小区是否闭塞 TEXT,
    是否为省际边界小区 TEXT,
    省际边界小区相邻省份名称 TEXT,
    CSFB回落网络优先级 TEXT,
    是否高铁二层小区 TEXT,
    是否电信共享小区 TEXT,
    建站单位 TEXT,
    共享方式 TEXT,
    是否载波聚合小区 TEXT,
    "是否载波聚合小区(采集)" TEXT,
    载波聚合类型 TEXT,
    "载波聚合类型(采集)" TEXT,
    载波聚合频段组合 TEXT,
    "载波聚合频段组合(采集)" TEXT,
    "主载波小区CELL ID" TEXT,
    "主载波小区CELL ID(采集)" TEXT,
    自定义字段1 TEXT,
    自定义字段2 TEXT,
    自定义字段3 TEXT,
    自定义字段4 TEXT,
    自定义字段5 TEXT,
    自定义字段6 TEXT,
    自定义字段7 TEXT,
    自定义字段8 TEXT,
    自定义字段9 TEXT,
    自定义字段10 TEXT,
    DN INT
);
alter table "4G-小区级工参" owner to postgres;

-- 导入工参数据
copy "4G-小区级工参" FROM 'D:\DT\lte_param.csv' CSV HEADER ENCODING 'gbk' DELIMITER ','  NULL '' quote '"';




--  导入天线参数表
-- drop table "陕西LTE网络天线数据维护";
CREATE TABLE "陕西LTE网络天线数据维护"
(
    天线编号 TEXT,
    天线名称 TEXT,
    省份 TEXT,
    所属城市 TEXT,
    所属区县 TEXT,
    经度 NUMERIC,
    纬度 NUMERIC,
    所属物理站编号 TEXT,
    所属RRU TEXT,
    天线方向角 NUMERIC,
    天线挂高 NUMERIC,
    电子下倾角 NUMERIC,
    机械倾角 NUMERIC,
    天线类型 TEXT,
    美化类型 TEXT,
    天线厂家 TEXT,
    天线集采型号 TEXT,
    天线端口数量 TEXT,
    水平半功率角 TEXT,
    垂直半功率角 TEXT,
    天线增益 TEXT,
    天线环境照片附件列表 TEXT,
    天线与覆盖目标照片附件列表 TEXT,
    地理化呈现照片附件 TEXT,
    存在问题照片附件列表 TEXT,
    天线到铁轨的垂直距离 TEXT,
    天线安装物理基础类型 TEXT,
    所属塔桅GID TEXT,
    "关联的机房/设备放置点编号" TEXT,
    "经度（WG84）" NUMERIC,
    "纬度（WG84）" NUMERIC,
    天线厂家型号 TEXT,
    塔放型号 TEXT,
    塔放上行允许最大增益 TEXT,
    塔放下行允许最大增益 TEXT,
    塔放上行当前增益 TEXT,
    塔放下行当前增益 TEXT,
    "收、发馈线长度" TEXT,
    天线电调方式 TEXT,
    入网时间 TEXT,
    天线支持频段 TEXT,
    辐射类型 TEXT,
    天线所属平台 TEXT,
    电子下倾角可调范围 TEXT,
    "天线条码（SN编码）" TEXT,
    是否安装塔顶放大器 TEXT,
    自定义字段1 TEXT,
    自定义字段2 TEXT,
    自定义字段3 TEXT,
    自定义字段4 TEXT,
    自定义字段5 TEXT,
    自定义字段6 TEXT,
    自定义字段7 TEXT,
    自定义字段8 TEXT,
    自定义字段9 TEXT,
    自定义字段10 TEXT
);
alter table "陕西LTE网络天线数据维护" owner to postgres;

-- 导入工参数据
copy "陕西LTE网络天线数据维护" FROM 'D:\DT\antina.csv' CSV HEADER ENCODING 'gbk' DELIMITER ','  NULL '' quote '"';




-- 创建扇区图形 cell_sector;
-- drop table cell_sector;
create table cell_sector as
(select
    cell_paramter.*,
    case
        when cell_paramter."小区覆盖类型" = '室内' then
            ST_Buffer(
                st_pointfromtext(
                    'POINT(' || cell_paramter."经度" || ' ' ||  cell_paramter."纬度" || ')'
                , 4326)
            , 0.00007, 'quad_segs=8')
        
        when cell_paramter."小区覆盖类型" = '室外' then
            ST_LineFromText(
                'LINESTRING(' || cell_paramter."经度" || ' '
                                || cell_paramter."纬度" || ', '
                                || cell_paramter."经度" +  0.00050 * sin(radians(cell_paramter."天线方向角")) || ' '
                                || cell_paramter."纬度" +  0.00050 * cos(radians(cell_paramter."天线方向角"))
                || ')'
            ,4326)
        else null
        end  as cell_sector,


    'POINT(' || cell_paramter."经度" || ' ' ||  cell_paramter."纬度" || ')' as site_node,


    case
        when cell_paramter."小区覆盖类型" = '室内' then
            'POINT(' || cell_paramter."经度" || ' ' ||  cell_paramter."纬度" || ')'
        
        when cell_paramter."小区覆盖类型" = '室外' then
            'POINT(' || cell_paramter."经度" +  0.00035 * sin(radians(cell_paramter."天线方向角")) || ' '
                      || cell_paramter."纬度" +  0.00035 * cos(radians(cell_paramter."天线方向角"))
            || ')'
        
        else null
        end  as line_node

from
    (select
        cell_param."小区标识",
        cell_param."小区别名",
        cell_param."所属城市",
        cell_param."所属区县",
        cell_param."乡镇",
        cell_param."跟踪区编码",
        cell_param."所属enodeb标识",
        cell_param."小区标识码",
        cell_param."所属enodeb标识" * 256 + cell_param."小区标识码" as ECI,
        cell_param."物理小区识别码",
        cell_param."设备厂家",
        cell_param."所属行政区域类型",
        cell_param."经度",
        cell_param."纬度",
        cell_param."小区覆盖类型",
        antenna."天线挂高",
        antenna."天线方向角",
        antenna."机械倾角",
        antenna."电子下倾角"
        
    from
         "4G-小区级工参" as cell_param
    inner join "陕西LTE网络天线数据维护" antenna on antenna."天线编号" = cell_param."相关联的天线列表"
    ) as cell_paramter
);






-- 创建拉线图层 cell_point_line
drop table if EXISTS cell_point_line;
create table cell_point_line as
    (select
         sample_point.ECI,
         -- st_pointfromtext(cell_sector.line_node,4326)
         ST_MakeLine(sample_point.point,
                     st_pointfromtext(cell_sector.line_node,4326)
         ) as cell_point_line
    from
         sample_point
    inner join cell_sector on cell_sector.ECI = sample_point.ECI
    
    );











drop table if EXISTS sub_polygon;
-- 生成 sub_polygon 图形
create table sub_polygon as
    (select
        ROW_NUMBER() OVER() as id,                           -- id  行号
        ST_Perimeter(polygon.polygon::geography) as perimeters,       -- 周长
        ST_Area(polygon.polygon::geography ) as area,       -- 面积
        ST_Astext(ST_Centroid(polygon.polygon)) as centroid_text,-- 重心点
        polygon.*
    from
        (select
            eci_buff_union.eci,
            ST_NumGeometries(eci_buff_union.unicon) as sub_geo_count,    --获取子图形的个数
            (ST_Dump(eci_buff_union.unicon)).path[1] as sub_index,      --提取 set 中的索引
            (ST_Dump(eci_buff_union.unicon)).geom as polygon            --提取 set 中的地理对象
            -- st_astext((ST_Dump(eci_buff_union.unicon)).geom)
        from
            (SELECT
                   sample_point.ECI,
                   st_unaryunion(st_union(ST_Buffer(sample_point.point, 0.00003, 'quad_segs=8'))) AS unicon
            FROM  sample_point
            
            GROUP BY
                   sample_point.ECI
            ) as eci_buff_union
        -- st_geometryn(b.un, generate_series(1, st_numgeometries(b.un)))
        ) as polygon
    );

/* 创建索引 */
CREATE INDEX sub_polygon_gist ON sub_polygon USING gist(polygon);
CREATE INDEX sub_polygon_index ON sub_polygon (id);
CREATE INDEX sub_polygon_eci ON sub_polygon (eci);



SELECT * from sub_polygon;






drop table if EXISTS sub_polygon_kpi_base;
-- sub_polygon 级的指标 基础表格 sub_polygon_kpi_base   此段脚本 运行非常缓慢, 所以单独拿出来生成表
create table sub_polygon_kpi_base as
        (SELECT
            sample_point.No,
            sample_point.UETime,
            sample_point.PCTime,
            sample_point.Lon,
            sample_point.Lat,
            sample_point.TAC,
            sample_point."eNodeB ID",
            sample_point.ECI,
            sample_point."Cell ID",
            sample_point.SectorID,
            sample_point."EARFCN DL",
            sample_point.PCI,
            sample_point.RSRP,
            sample_point.RSRQ,
            sample_point.SINR,
            sample_point."Cell 1st EARFCN",
            sample_point."Cell 1st PCI",
            sample_point."Cell 1st RSRP",
        
            sample_point."Cell 2nd EARFCN",
            sample_point."Cell 2nd PCI",
            sample_point."Cell 2nd RSRP",
        
            sample_point."Cell 3rd EARFCN",
            sample_point."Cell 3rd PCI",
            sample_point."Cell 3rd RSRP",
         
            sample_point."Cell 4th EARFCN",
            sample_point."Cell 4th PCI",
            sample_point."Cell 4th RSRP",
        
            sample_point."Cell 5th EARFCN",
            sample_point."Cell 5th PCI",
            sample_point."Cell 5th RSRP",
        
            sample_point."Cell 6th EARFCN",
            sample_point."Cell 6th PCI",
            sample_point."Cell 6th RSRP",
            sub_polygon.id as sub_polygon_id,
            sub_polygon.area,
            sub_polygon.perimeters,
            sub_polygon.centroid_text
        FROM
             sample_point,
             sub_polygon
        WHERE
             ST_Contains(sub_polygon.polygon, sample_point.point)
            
        );

create index sub_kpi_base_eci_index on sub_polygon_kpi_base (eci);
create index sub_kpi_base_cid_index on sub_polygon_kpi_base (sub_polygon_id);





drop table if EXISTS sub_polygon_kpi;
-- eci 和 路段 划分的 区域 sub_polygon  级别指标 sub_polygon_kpi
create table sub_polygon_kpi as
(select
    sub_polygon.id as "路段ID",
    sub_polygon.ECI AS "ECI",
    cell_sector."物理小区识别码" as "PCI",
    sub_polygon.sub_geo_count as "小区主覆盖的路段数",
    COALESCE(sub_polygon.sub_index,1) as "路段Index",
    -- kpi.centroid_text as "路段的中心点",
    -- kpi.area as "路段面积",
    kpi.perimeters/2 - 6  as "路段长度",

    kpi.avg_rsrp as "平均电平",

    kpi.polygon_sample as "有效采样点数",
    kpi.poor_cover_sample as "弱覆盖采样点数",
    kpi.poorcover_ratio as "弱覆盖比例",
    
    kpi.avg_sinr as "平均SINR",
    kpi.sinr_len2_sample as "SINR小于负2的采样点数",
    kpi.sinr_le5_sample as "SINR小于5的采样点数",
    kpi.sinr_len2_ratio as "SINR小于负2的比例",
    kpi.sinr_le5_ratio as "SINR小于5的比例",

    
    overshoot.min_distance as "覆盖距离_min",
    overshoot.sub_polygon_distance as "覆盖距离_本路段",
    overshoot.distance_diff as "过覆盖距离差异",
 
    kpi.overlap_sample as "重叠覆盖采样点数",
    kpi.overlap_ratio as "重叠覆盖比例",

    overlap_ncell_rn3.rn_1_n_pci AS "重叠覆盖影响最强的小区_1_PCI",
    overlap_ncell_rn3.rn_1_n_avg_rsrp AS "重叠覆盖影响最强的小区_1_RSRP",
    overlap_ncell_rn3.rn_1_n_count AS "重叠覆盖影响最强的小区_1_采样点数",

    overlap_ncell_rn3.rn_2_n_pci AS "重叠覆盖影响最强的小区_2_PCI",
    overlap_ncell_rn3.rn_2_n_avg_rsrp AS "重叠覆盖影响最强的小区_2_RSRP",
    overlap_ncell_rn3.rn_2_n_count AS "重叠覆盖影响最强的小区_2_采样点数",

    overlap_ncell_rn3.rn_3_n_pci AS "重叠覆盖影响最强的小区_3_PCI",
    overlap_ncell_rn3.rn_3_n_avg_rsrp AS "重叠覆盖影响最强的小区_3_RSRP",
    overlap_ncell_rn3.rn_3_n_count AS "重叠覆盖影响最强的小区_3_采样点数",

    kpi.strongcover_sample as "电平过强覆盖采样点数",
    kpi.strongcover_ratio as "过强覆盖比例",

    azimuth_diff."天线方向角" as "天线方位角_工参",
    azimuth_diff.cell_Azimuth as "小区覆盖方位角",
    round(azimuth_diff.Azimuth_diff::numeric,2) as "方位角差异",

    mod3.mod3_pci_count as "模三干扰PCI的数",
    mod3.mod3_sample as "模三干扰总采样点数",
    mod3.mod3_1st_pci as "模三干扰最强邻小区_1_PCI",
    mod3.mod3_1st_sample AS "模三干扰最强邻小区_1_采样点数",
    mod3.mod3_2nd_pci as "模三干扰最强邻小区_2_PCI",
    mod3.mod3_2nd_sample AS "模三干扰最强邻小区_2_采样点数",
    mod3.mod3_3rd_pci as "模三干扰最强邻小区_3_PCI",
    mod3.mod3_3rd_sample as "模三干扰最强邻小区_3_采样点数",

    (case when kpi.perimeters/2 - 6 < 20 then True else False end ) as "是否为覆盖过短路段",
    (case when kpi.poorcover_ratio >= 0.97 and kpi.polygon_sample >= 10 then True else False end ) as "是否为弱覆盖路段",
    (case when kpi.sinr_len2_ratio >= 0.05 and kpi.polygon_sample >= 10 then True else False end ) as "是否为差SINR路段",

    (case when overshoot.distance_diff >= 2 and overshoot.min_distance >= 200 then True else False end ) as "是否为过覆盖路段",
    
    (case when kpi.overlap_ratio >= 0.2 and kpi.polygon_sample >= 10 then True else False end ) as "是否为重叠覆盖路段",

    (case when kpi.strongcover_ratio >= 0.2 and kpi.polygon_sample >= 10 then True else False end ) as "是否为过强覆盖路段",

    (case when azimuth_diff.Azimuth_diff >= 90  and kpi.polygon_sample >= 10 then True else False end ) as "是否为方位角差异过大路段",

    sub_polygon.polygon

from
    sub_polygon

left join
    cell_sector on cell_sector.ECI = sub_polygon.eci

left join
    (select
        sub_polygon_kpi_base.sub_polygon_id,
        max(sub_polygon_kpi_base.centroid_text) as centroid_text,
        round(max(sub_polygon_kpi_base.area)::numeric,2) as area,
        round(max(sub_polygon_kpi_base.perimeters)::numeric,2) as perimeters,
        round(avg(sub_polygon_kpi_base.RSRP),2) as avg_rsrp,
    
        count(*) as polygon_sample,
        sum(case when sub_polygon_kpi_base.RSRP < -110 then 1 else 0 end) as poor_cover_sample,
        round(sum(case when sub_polygon_kpi_base.RSRP < -110 then 1 else 0 end) / count(*)::numeric,4) as poorcover_ratio,
    
        round(avg(sub_polygon_kpi_base.SINR),2) as avg_sinr,
        sum(case when sub_polygon_kpi_base.SINR <= -2 then 1 else 0 end) as sinr_len2_sample,
        sum(case when sub_polygon_kpi_base.SINR <= 5 then 1 else 0 end) as sinr_le5_sample,

        round(sum(case when sub_polygon_kpi_base.SINR <= -2 then 1 else 0 end) / count(*)::numeric,4) as sinr_len2_ratio,
        round(sum(case when sub_polygon_kpi_base.SINR <= 5 then 1 else 0 end) / count(*)::numeric,4) as sinr_le5_ratio,

        
        sum(case when overlap_sample.overlap_number >= 3 then 1 else 0 end) as overlap_sample,
        round(sum(case when overlap_sample.overlap_number >= 3 then 1 else 0 end) / count(*)::numeric,4) as overlap_ratio,

        sum(case when strong_cover.strongcover_number >= 3 then 1 else 0 end) as strongcover_sample,
        round(sum(case when strong_cover.strongcover_number >= 3 then 1 else 0 end) / count(*)::numeric,4) as strongcover_ratio
    
    from
        sub_polygon_kpi_base

        -- 计算重叠覆盖度
        left join
            (select
                sample.No,
                sample.PCTime,
                sample.Lon,
                sample.Lat,
                sample.TAC,
                sample."eNodeB ID",
                sample.ECI,
                sample.SectorID,
                sample."EARFCN DL",
                sample.PCI,
                sample.RSRP,
        
                (case
                    when
                         sample."Cell 1st RSRP" - sample."Cell 2nd RSRP" <= 6
                         and sample."Cell 1st RSRP" >= -110
                         and sample."Cell 2nd RSRP" is not null
                         and not (sample."Cell 2nd PCI" = sample.PCI and sample."Cell 2nd EARFCN" = sample."EARFCN DL")
                    then 1
                    else 0 end +
                    
                case
                    when
                         sample."Cell 1st RSRP" - sample."Cell 3rd RSRP" <= 6
                         and sample."Cell 1st RSRP" >= -110
                         and sample."Cell 3rd RSRP" is not null
                         and not (sample."Cell 3rd PCI" = sample.PCI and sample."Cell 3rd EARFCN" = sample."EARFCN DL")
                    then 1
                    else 0 end +
                    
                case
                    when
                         sample."Cell 1st RSRP" - sample."Cell 4th RSRP" <= 6
                         and sample."Cell 1st RSRP" >= -110
                         and sample."Cell 4th RSRP" is not null
                         and not (sample."Cell 4th PCI" = sample.PCI and sample."Cell 4th EARFCN" = sample."EARFCN DL")
                    then 1
                    else 0 end +
                    
                case
                    when
                         sample."Cell 1st RSRP" - sample."Cell 5th RSRP" <= 6
                         and sample."Cell 1st RSRP" >= -110
                         and sample."Cell 5th RSRP" is not null
                         and not (sample."Cell 5th PCI" = sample.PCI and sample."Cell 5th EARFCN" = sample."EARFCN DL")
                    then 1
                    else 0 end +
                    
                case
                    when
                         sample."Cell 1st RSRP" - sample."Cell 6th RSRP" <= 6
                         and sample."Cell 1st RSRP" >= -110
                         and sample."Cell 6th RSRP" is not null
                         and not (sample."Cell 6th PCI" = sample.PCI and sample."Cell 6th EARFCN" = sample."EARFCN DL")
                    then 1
                    else 0 end
        
                ) as overlap_number
            from
                 sub_polygon_kpi_base sample
            
            where
                sample.RSRP >= -110    -- 主覆盖小区rsrp >= -110
                and sample."Cell 1st RSRP" >= -110  -- 邻小区rsrp >= -110

            
            ) as overlap_sample on overlap_sample.No = sub_polygon_kpi_base.No

       
    left join
        -- 电平过强比例
        (select
            sample.No,
            sample.PCTime,
            sample.Lon,
            sample.Lat,
            sample.TAC,
            sample."eNodeB ID",
            sample.ECI,
            sample.SectorID,
            sample."EARFCN DL",
            sample.PCI,
            sample.RSRP,
            
            (case
                when
                     sample."Cell 1st RSRP" >= -90
                     and sample."Cell 1st RSRP" is not null
                     and not (sample."Cell 1st PCI" = sample.PCI and sample."Cell 1st EARFCN" = sample."EARFCN DL")
                
                then 1
                else 0 end +
                
            case
                when
                     sample."Cell 2nd RSRP" >= -90
                     and sample."Cell 2nd RSRP" is not null
                     and not (sample."Cell 2nd PCI" = sample.PCI and sample."Cell 2nd EARFCN" = sample."EARFCN DL")
                then 1
                else 0 end +
                
            case
                when
                     sample."Cell 3rd RSRP" >= -90
                     and sample."Cell 3rd RSRP" is not null
                     and not (sample."Cell 3rd PCI" = sample.PCI and sample."Cell 3rd EARFCN" = sample."EARFCN DL")
                then 1
                else 0 end +
                
            case
                when
                     sample."Cell 4th RSRP" >= -90
                     and sample."Cell 4th RSRP" is not null
                     and not (sample."Cell 4th PCI" = sample.PCI and sample."Cell 4th EARFCN" = sample."EARFCN DL")
                then 1
                else 0 end +
                
            case
                when
                     sample."Cell 5th RSRP" >= -90
                     and sample."Cell 5th RSRP" is not null
                     and not (sample."Cell 5th PCI" = sample.PCI and sample."Cell 5th EARFCN" = sample."EARFCN DL")
                then 1
                else 0 end +
                
            case
                when
                     sample."Cell 6th RSRP" >= -90
                     and sample."Cell 6th RSRP" is not null
                     and not (sample."Cell 6th PCI" = sample.PCI and sample."Cell 6th EARFCN" = sample."EARFCN DL")
                then 1
                else 0 end
                
            ) as strongcover_number
        from
            sub_polygon_kpi_base sample
            
        where
            sample."Cell 1st RSRP" >= -90

        ) as strong_cover on strong_cover.No = sub_polygon_kpi_base.No
        
    group by
        sub_polygon_kpi_base.sub_polygon_id
       
    ) as kpi on kpi.sub_polygon_id = sub_polygon.id


        
        -- 计算 方位角差异
        left join
            (select
                azimuth.eci,
                azimuth."天线方向角",
                round(azimuth.cell_Azimuth::numeric,2) as cell_Azimuth,
                
                round(abs((case when azimuth.cell_Azimuth > 180
                           then abs(360 - azimuth.cell_Azimuth)
                           else azimuth.cell_Azimuth
                           end ) -
                     (case when azimuth."天线方向角" > 180
                            then abs(360 - azimuth."天线方向角")
                            else azimuth."天线方向角"
                            end)
                    )::numeric,2) as Azimuth_diff
            from
                (select
                    eci_buff_union.eci,
                    degrees(ST_Azimuth(ST_PointFromText('POINT(' || cell_sector."经度" || ' ' || cell_sector."纬度" || ')', 4326),
                                       ST_Centroid(eci_buff_union.unicon))
                    ) AS cell_Azimuth,
                    
                    cell_sector."天线方向角"
                
                from
                     cell_sector
                inner join
                    (select
                        eci_buff_array.eci,
                        ST_Union(eci_buff_array.buffs) as unicon
                    from
                        (select
                            sample.ECI,
                                array(
                                    select
                                        sample_buff.buff
                                    from
                                         (select
                                             sample.No,
                                             sample.PCTime,
                                             sample.Lon,
                                             sample.Lat,
                                             sample.TAC,
                                             sample."eNodeB ID",
                                             sample.ECI,
                                             sample.SectorID,
                                             sample."EARFCN DL",
                                             sample.PCI,
                                             sample.RSRP,
                                             ST_Buffer(sample.point,  0.00003, 'quad_segs=8') as buff
                                         from
                                             sample_point sample
                                         ) as sample_buff
                                    where
                                        sample.ECI = sample_buff.ECI
                                    ) as buffs
                            from
                                UE1_DT_Date sample
                            
                            where
                                PCTime is not null
                                and Lon is not null
                                and Lat is not null
                                and eci is not null
                                and "EARFCN DL" is not null
                            group by
                                sample.ECI
                        )  as eci_buff_array
                    ) as eci_buff_union on cell_sector.ECI = eci_buff_union.ECI
            
            ) as azimuth
        
        )  as azimuth_diff on azimuth_diff.ECI = sub_polygon.eci
   

        
        
        left join
        -- 过覆盖, 路段中心点与基站的距离
        (select
            sub_polygon_distance.eci,
            sub_polygon_distance.id,
            sub_polygon_distance.sub_geo_count,
            round(sub_polygon_distance.sub_polygon_distance::numeric,2) as sub_polygon_distance,
            round(min_distance.min_distance::numeric,2) as min_distance,
            round((sub_polygon_distance.sub_polygon_distance/min_distance.min_distance)::numeric,2) as distance_diff
            
        from
            (select
                sub_polygon.id,
                sub_polygon.eci,
                sub_polygon.sub_geo_count,
                ST_Distance(ST_Pointfromtext(cell_sector.site_node,4326)::geography,ST_Pointfromtext(sub_polygon.centroid_text,4326)::geography) as sub_polygon_distance
            from
                sub_polygon
                inner join
                    cell_sector on cell_sector.eci = sub_polygon.eci
            ) as sub_polygon_distance
            
            left join
                (select
                    distance.eci,
                    min(polygon_distance) as min_distance
                from
                    (select
                        sub_polygon.id,
                        sub_polygon.eci,
                        sub_polygon.sub_geo_count,
                        ST_Distance(ST_Pointfromtext(cell_sector.site_node,4326)::geography,ST_Pointfromtext(sub_polygon.centroid_text,4326)::geography) as polygon_distance
                    from
                        sub_polygon
                        inner join
                            cell_sector on cell_sector.eci = sub_polygon.eci
        /*            where
                        sub_polygon.perimeters/2 - 12 >= 10       -- 过滤 覆盖过短的路段*/
                        
                    ) as distance
                group by
                    distance.eci
                ) as min_distance on min_distance.eci = sub_polygon_distance.eci
        
        ) as overshoot on overshoot.id = sub_polygon.id



        left join
        -- 重叠覆盖小区 影响最强的三个邻区  本路段内 重叠覆盖采样的的第一个邻区默认为电平最强邻小区,取个数 量最多的三个邻小区
        (select
            n_cell_rn.sub_polygon_id,
        
            max(CASE WHEN n_cell_rn.rn = 1 THEN n_cell_rn.n_pci END)   AS rn_1_n_pci,
            max(CASE WHEN n_cell_rn.rn = 1 THEN n_cell_rn.ncell_avg_rsrp END)   AS rn_1_n_avg_rsrp,
            max(CASE WHEN n_cell_rn.rn = 1 THEN n_cell_rn.ncell_count END)   AS rn_1_n_count,
        
            max(CASE WHEN n_cell_rn.rn = 2 THEN n_cell_rn.n_pci END)   AS rn_2_n_pci,
            max(CASE WHEN n_cell_rn.rn = 2 THEN n_cell_rn.ncell_avg_rsrp END)   AS rn_2_n_avg_rsrp,
            max(CASE WHEN n_cell_rn.rn = 2 THEN n_cell_rn.ncell_count END)   AS rn_2_n_count,
        
            max(CASE WHEN n_cell_rn.rn = 3 THEN n_cell_rn.n_pci END)   AS rn_3_n_pci,
            max(CASE WHEN n_cell_rn.rn = 3 THEN n_cell_rn.ncell_avg_rsrp END)   AS rn_3_n_avg_rsrp,
            max(CASE WHEN n_cell_rn.rn = 3 THEN n_cell_rn.ncell_count END)   AS rn_3_n_count
        
        from
            (select
                n_cell_order.sub_polygon_id,
                n_cell_order.centroid_text,
                n_cell_order.n_pci,
                n_cell_order.ncell_avg_rsrp,
                n_cell_order.ncell_count,
                n_cell_order.ncell_reference,
                row_number()
                    OVER (
                    PARTITION BY n_cell_order.sub_polygon_id
                    ORDER BY  n_cell_order.ncell_count DESC,n_cell_order.ncell_avg_rsrp DESC
                    ) AS rn
                   
            from
                (select
                    n_cell.sub_polygon_id,
                    n_cell.centroid_text,
                    n_cell.n_pci,
                    round(avg(n_cell.n_rsrp)::numeric,2) as ncell_avg_rsrp,
                    count(n_cell.n_pci) as ncell_count,
                    round(avg(n_cell.n_rsrp)*count(n_cell.n_rsrp)::numeric,2)  as ncell_reference
                
                from
                     (SELECT
                        sample.No,
                        sample.PCTime,
                        sample.Lon,
                        sample.Lat,
                        sample.ECI,
                        sample.pci,
                        sample.sub_polygon_id,
                        sample.centroid_text,
                        sample.RSRP,
    
                       unnest(ARRAY ["Cell 1st PCI",
                                      "Cell 2nd PCI",
                                      "Cell 3rd PCI",
                                      "Cell 4th PCI",
                                      "Cell 5th PCI",
                                      "Cell 6th PCI"]) AS n_pci,
    
                       unnest(ARRAY  ["Cell 1st RSRP",
                                      "Cell 2nd RSRP",
                                      "Cell 3rd RSRP",
                                      "Cell 4th RSRP",
                                      "Cell 5th RSRP",
                                      "Cell 6th RSRP"]) AS n_rsrp,
                            (case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 2nd RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 2nd RSRP" is not null
                                     and not (sample."Cell 2nd PCI" = sample.ECI and sample."Cell 2nd EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 3rd RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 3rd RSRP" is not null
                                     and not (sample."Cell 3rd PCI" = sample.PCI and sample."Cell 3rd EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 4th RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 4th RSRP" is not null
                                     and not (sample."Cell 4th PCI" = sample.PCI and sample."Cell 4th EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 5th RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 5th RSRP" is not null
                                     and not (sample."Cell 5th PCI" = sample.PCI and sample."Cell 5th EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 6th RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 6th RSRP" is not null
                                     and not (sample."Cell 6th PCI" = sample.PCI and sample."Cell 6th EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end
                    
                            ) as overlap_number
    
                     FROM
                       sub_polygon_kpi_base sample
                     ) AS n_cell
                     

                where
                    overlap_number >= 3
                    and n_cell.pci <> n_cell.n_pci
                    /* 邻小区列表中一般第1个邻小区 与主服务小区的频点和PCI 是一样的( 同一个小区),
                       如果有其他邻小区的信号比主服务小区的信号强, 则主服务小区在邻小区中的位置会按找电平强度 降序排列.
                    
                       除主服务小区以外,有两个邻小区满足重叠覆盖的条件 (是重叠覆盖采样点)*/
                group by
                    n_cell.sub_polygon_id,
                    n_cell.centroid_text,
                    n_cell.n_pci
                ) as n_cell_order
        
            ) as n_cell_rn
        
        where
            n_cell_rn.rn <= 3       -- 取影响最强的三个邻小区
        
        group by
            n_cell_rn.sub_polygon_id
        
        order by
            n_cell_rn.sub_polygon_id
        ) as overlap_ncell_rn3 on overlap_ncell_rn3.sub_polygon_id = sub_polygon.id


        
        
    left join
        -- 模三干扰
        (select
            mod3_pci_count_3rd.sub_polygon_id,
            mod3_pci_count_3rd.mod3_pci_count,
            mod3_count.mod3_sample,
            mod3_pci_count_3rd.mod3_1st_pci,
            mod3_pci_count_3rd.mod3_1st_sample,
            mod3_pci_count_3rd.mod3_2nd_pci,
            mod3_pci_count_3rd.mod3_2nd_sample,
            mod3_pci_count_3rd.mod3_3rd_pci,
            mod3_pci_count_3rd.mod3_3rd_sample
            
        from
             -- 模三干扰前三的邻小区信息
            (select
                mod3_pci_count_rn.sub_polygon_id,
                count(*) as mod3_pci_count,
                max(case when mod3_pci_count_rn.rn = 1 then mod3_pci_count_rn.mod3_pci  end) as "mod3_1st_pci",
                max(case when mod3_pci_count_rn.rn = 1 then mod3_pci_count_rn.mod3_pci_count  end) as "mod3_1st_sample",
            
                max(case when mod3_pci_count_rn.rn = 2 then mod3_pci_count_rn.mod3_pci  end) as "mod3_2nd_pci",
                max(case when mod3_pci_count_rn.rn = 2 then mod3_pci_count_rn.mod3_pci_count  end) as "mod3_2nd_sample",
                
                max(case when mod3_pci_count_rn.rn = 3 then mod3_pci_count_rn.mod3_pci  end) as "mod3_3rd_pci",
                max(case when mod3_pci_count_rn.rn = 3 then mod3_pci_count_rn.mod3_pci_count  end) as "mod3_3rd_sample"
                
            from
                (select
                    mod3_pci_count.sub_polygon_id,
                    mod3_pci_count.mod3_pci,
                    mod3_pci_count.mod3_pci_count,
                    row_number()
                        OVER (
                            PARTITION BY mod3_pci_count.sub_polygon_id
                            ORDER BY mod3_pci_count.mod3_pci_count desc
                            ) AS rn
                       
                from
                    (select
                        mod3.sub_polygon_id,
                        mod3.mod3_pci,
                        count(*) as mod3_pci_count
                    from
                        (select
                            mod3.sub_polygon_id,
                            regexp_split_to_table(unnest(ARRAY[mod3.mod3_pci])::varchar,',') as mod3_pci
                        
                        from
                            (select
                                   sample.No,
                                   sample.PCTime,
                                   sample.Lon,
                                   sample.Lat,
                                   sample.ECI,
                                   sample.PCI,
                                   sample.sub_polygon_id,
                                   sample.centroid_text,
                                   sample.RSRP,
                            
                               (case
                                    when sample.RSRP - sample."Cell 1st RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 1st PCI"%3
                                             and not (sample."Cell 1st PCI" = sample.PCI and sample."Cell 1st EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 1st PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 2nd RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 2nd PCI"%3
                                             and not (sample."Cell 2nd PCI" = sample.PCI and sample."Cell 2nd EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 2nd PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 3rd RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 3rd PCI"%3
                                             and not (sample."Cell 3rd PCI" = sample.PCI and sample."Cell 3rd EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 3rd PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 4th RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 4th PCI"%3
                                             and not (sample."Cell 4th PCI" = sample.PCI and sample."Cell 4th EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 4th PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 5th RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 5th PCI"%3
                                             and not (sample."Cell 5th PCI" = sample.PCI and sample."Cell 5th EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 5th PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 6th RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 6th PCI"%3
                                             and not (sample."Cell 6th PCI" = sample.PCI and sample."Cell 6th EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 6th PCI" || ''
                                    else '' end
                               ) as mod3_pci                               
                            
                            from
                                 sub_polygon_kpi_base sample
                            
                            where
                                    sample.RSRP >= -110    -- 主覆盖小区rsrp >= -110
                                and sample."Cell 1st RSRP" >= -110  -- 邻小区rsrp >= -110
                                
                                -- and sub_polygon_id = 32
                            
                            ) as mod3
                        ) as mod3
                    
                    where
                        mod3.mod3_pci <> ''
                    
                    group by
                        mod3.sub_polygon_id,
                        mod3.mod3_pci
                    ) as mod3_pci_count
                
                ) as mod3_pci_count_rn
                
            group by
                mod3_pci_count_rn.sub_polygon_id
            ) as mod3_pci_count_3rd
        
        
            inner join
                -- 模三干扰的总采样点
                (select
                    mod3.sub_polygon_id,
                    count(*) as mod3_sample
                from
                    (select
                        mod3.sub_polygon_id,
                        regexp_split_to_table(unnest(ARRAY[mod3.mod3_pci])::varchar,',') as mod3_pci
                    
                    from
                        (select
                               sample.No,
                               sample.PCTime,
                               sample.Lon,
                               sample.Lat,
                               sample.ECI,
                               sample.PCI,
                               sample.sub_polygon_id,
                               sample.centroid_text,
                               sample.RSRP,
                        
                               (case
                                    when sample.RSRP - sample."Cell 1st RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 1st PCI"%3
                                             and not (sample."Cell 1st PCI" = sample.PCI and sample."Cell 1st EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 1st PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 2nd RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 2nd PCI"%3
                                             and not (sample."Cell 2nd PCI" = sample.PCI and sample."Cell 2nd EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 2nd PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 3rd RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 3rd PCI"%3
                                             and not (sample."Cell 3rd PCI" = sample.PCI and sample."Cell 3rd EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 3rd PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 4th RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 4th PCI"%3
                                             and not (sample."Cell 4th PCI" = sample.PCI and sample."Cell 4th EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 4th PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 5th RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 5th PCI"%3
                                             and not (sample."Cell 5th PCI" = sample.PCI and sample."Cell 5th EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 5th PCI" || ','
                                    else '' end ||
                                
                               case
                                    when sample.RSRP - sample."Cell 6th RSRP" <= 6
                                             and sample.RSRP >= -110
                                             and sample.PCI%3 = sample."Cell 6th PCI"%3
                                             and not (sample."Cell 6th PCI" = sample.PCI and sample."Cell 6th EARFCN" = sample."EARFCN DL")
                                          then sample."Cell 6th PCI" || ''
                                    else '' end
                               ) as mod3_pci
                        
                        from
                             sub_polygon_kpi_base sample
                        
                        where
                                sample.RSRP >= -110    -- 主覆盖小区rsrp >= -110
                            and sample."Cell 1st RSRP" >= -110  -- 邻小区rsrp >= -110
                            
                            -- and sub_polygon_id = 32
                        
                        ) as mod3
                    ) as mod3
                
                where
                    mod3.mod3_pci <> ''
                
                group by
                    mod3.sub_polygon_id
                ) as mod3_count on mod3_count.sub_polygon_id = mod3_pci_count_3rd.sub_polygon_id

        ) as mod3 on mod3.sub_polygon_id = sub_polygon.id
        
);

















create table all_kpi as
-- 总体指标
    (select
        round(avg(base.RSRP)::numeric,2) as avg_rsrp,
        
        count(*) as total_sample,
        sum(case when base.RSRP < -110 then 1 else 0 end) as poor_cover_sample,
        round(sum(case when base.RSRP < -110 then 1 else 0 end) / count(*)::numeric,4) as poorcover_ratio,
    
        sum(case when base.SINR < -2 then 1 else 0 end) as poor_sinr_n2_sample,
        round(sum(case when base.SINR < -2 then 1 else 0 end) / count(*)::numeric,4) as poor_sinr_n2_ratio,
    
        sum(case when base.SINR < 5 then 1 else 0 end) as poor_sinr_5_sample,
        round(sum(case when base.SINR < 5 then 1 else 0 end) / count(*)::numeric,4) as poor_sinr_5_ratio,
        
        sum(case when overlap_sample.overlap_number >= 2 then 1 else 0 end) as overlap_sample,
        round(sum(case when overlap_sample.overlap_number >= 2 then 1 else 0 end) / count(*)::numeric,4) as overlap_ratio
        
        
    from
        sub_polygon_kpi_base base
        
        -- 计算重叠覆盖度
            left join
                (select
                    sample.No,
                    sample.PCTime,
                    sample.Lon,
                    sample.Lat,
                    sample.TAC,
                    sample."eNodeB ID",
                    sample.ECI,
                    sample.SectorID,
                    sample."EARFCN DL",
                    sample.PCI,
                    sample.RSRP,
            
                            (case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 2nd RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 2nd RSRP" is not null
                                     and not (sample."Cell 2nd PCI" = sample.ECI and sample."Cell 2nd EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 3rd RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 3rd RSRP" is not null
                                     and not (sample."Cell 3rd PCI" = sample.PCI and sample."Cell 3rd EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 4th RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 4th RSRP" is not null
                                     and not (sample."Cell 4th PCI" = sample.PCI and sample."Cell 4th EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 5th RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 5th RSRP" is not null
                                     and not (sample."Cell 5th PCI" = sample.PCI and sample."Cell 5th EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end +
                                
                            case
                                when
                                     sample."Cell 1st RSRP" - sample."Cell 6th RSRP" <= 6
                                     and sample."Cell 1st RSRP" >= -110
                                     and sample."Cell 6th RSRP" is not null
                                     and not (sample."Cell 6th PCI" = sample.PCI and sample."Cell 6th EARFCN" = sample."EARFCN DL")
                                then 1
                                else 0 end
                    
                            ) as overlap_number
                from
                     sub_polygon_kpi_base sample
                
                where
                    sample."Cell 1st RSRP" >= -110
                    and PCTime is not null
                    and Lon is not null
                    and Lat is not null
                    and eci is not null
                    and "EARFCN DL" is not null
                
                ) as overlap_sample on overlap_sample.No = base.No
    
        );

    












































