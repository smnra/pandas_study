

-- SINR 分析

drop table IF EXISTS buff;
-- 创建buff 表  为临时表
CREATE TABLE buff AS
SELECT
    db_scan.*,
    ST_Buffer(db_scan.point, 0.0003, 'quad_segs=8') AS buff
FROM
    (SELECT *,
        st_clusterdbscan(point, 0.0006, 20) OVER ()   AS cluster_id
     FROM sample_point
     WHERE sinr <= -2
    ) as db_scan
WHERE
    db_scan.cluster_id is not null;





--删除表 sinr_polygon
drop table IF EXISTS sinr_polygon;

-- 生成 sinr_polygon  图形  sinr 聚类生成的 polygon  过滤条件为 sinr <= -2
CREATE TABLE sinr_polygon AS
select
    ST_Perimeter(polygon.polygon::geography) as perimeters,       -- 周长
    ST_Area(polygon.polygon::geography ) as area,       -- 面积
    ST_Astext(ST_Centroid(polygon.polygon)) as centroid_text,-- 重心点
    polygon.*
FROM
    (SELECT
        unset_collection.id,
        ST_NumGeometries(unset_collection.polygon) as sub_geo_count,    --获取子图形的个数
        (ST_Dump(unset_collection.polygon)).geom as polygon            --提取 set 中的地理对象
    FROM
        (SELECT
            id_collection_geo.id,
            st_unaryunion(id_collection_geo.collection_geo) as polygon
        FROM
            (SELECT
                row_number() over() as id,
                collection_geo.collection_geo
            FROM
                (SELECT
                    unnest(ST_ClusterIntersecting(union_geo)) as  collection_geo
                FROM
                    (SELECT
                        buff.cluster_id,
                        st_union(buff.buff) as union_geo
                    FROM buff
                    GROUP BY buff.cluster_id
                    ) as union_geo
                ) as collection_geo
            ) as id_collection_geo
        ) as unset_collection
    ) as polygon;


/* 创建索引 */
CREATE INDEX sinr_polygon_gist ON sinr_polygon USING gist(polygon);

CREATE INDEX sinr_polygon_index
    ON sinr_polygon (id);




--删除表 sinr_polygon_kpi_base
drop table IF EXISTS sinr_polygon_kpi_base;

-- sinr_polygon 级的指标 基础表格 sinr_polygon_kpi_base   此段脚本 运行非常缓慢, 所以单独拿出来生成表
create table sinr_polygon_kpi_base as
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
            sinr_polygon.id as sinr_polygon_id,
            sinr_polygon.area,
            sinr_polygon.perimeters,
            sinr_polygon.centroid_text
        FROM
             sample_point,
             sinr_polygon
        WHERE
             ST_Contains(sinr_polygon.polygon, sample_point.point)
        );
    


create index sinr_kpi_base_eci_index on sinr_polygon_kpi_base (eci);
create index sinr_kpi_base_cid_index on sinr_polygon_kpi_base (sinr_polygon_id);

















drop table IF EXISTS sinr_polygon_kpi;

--  sinr <= -2  划分的 区域 sinr_polygon  级别指标 sinr_polygon_kpi
create table sinr_polygon_kpi as
(select
    sinr_polygon.id as "路段ID",
/*    sinr_polygon.ECI AS "ECI",
    cell_sector."物理小区识别码" as "PCI",*/
    sinr_polygon.sub_geo_count as "小区主覆盖的路段数",
/*    COALESCE(sinr_polygon.sub_index,1) as "路段Index",*/
    (kpi.perimeters/2) - 60 as "路段长度",
    kpi.eci_count  as "本路段主覆盖小区的数量",
    kpi.avg_rsrp as "平均电平",

    kpi.polygon_sample as "有效采样点数",
/*    kpi.poor_cover_sample as "弱覆盖采样点数",*/
/*    kpi.poorcover_ratio as "弱覆盖比例",*/
    mod3.mod3_sample as "模三干扰总采样点数",
/*    kpi.avg_sinr as "平均SINR",*/
    kpi.sinr_len2_sample as "SINR小于负2的采样点数",
    kpi.sinr_le5_sample as "SINR小于5的采样点数",
    kpi.sinr_len2_ratio as "SINR小于负2的比例",
    kpi.sinr_le5_ratio as "SINR小于5的比例",

    
/*    overshoot.min_distance as "覆盖距离_min",
    overshoot.sub_polygon_distance as "覆盖距离_本路段",
    overshoot.distance_diff as "过覆盖距离差异",*/
 
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

/*    azimuth_diff."天线方向角" as "天线方位角_工参",
    azimuth_diff.cell_Azimuth as "小区覆盖方位角",
    round(azimuth_diff.Azimuth_diff::numeric,2) as "方位角差异",*/

/*    mod3.mod3_pci_count as "模三干扰PCI的数",*/
/*    mod3.mod3_1st_pci as "模三干扰最强邻小区_1_PCI",
    mod3.mod3_1st_sample "模三干扰最强邻小区_1_采样点数",
    mod3.mod3_2nd_pci as "模三干扰最强邻小区_2_PCI",
    mod3.mod3_2nd_sample"模三干扰最强邻小区_2_采样点数",
    mod3.mod3_3rd_pci as "模三干扰最强邻小区_3_PCI",
    mod3.mod3_3rd_sample as "模三干扰最强邻小区_3_采样点数",*/

    (case when kpi.perimeters/2 - 60 < 20 then True else False end ) as "是否为覆盖过短路段",

/*    (case when kpi.poorcover_ratio >= 0.97 and kpi.polygon_sample >= 10 then True else False end ) as "是否为弱覆盖路段",*/
/*    (case when kpi.sinr_len2_ratio >= 0.05 and kpi.polygon_sample >= 10 then True else False end ) as "是否为差SINR路段",*/


/*    (case when overshoot.distance_diff >= 2 and overshoot.min_distance >= 200 then True else False end ) as "是否为过覆盖路段",*/
    
    (case when kpi.overlap_ratio >= 0.2 and kpi.polygon_sample >= 10 then True else False end ) as "是否为重叠覆盖路段",

    (case when kpi.strongcover_ratio >= 0.2 and kpi.polygon_sample >= 10 then True else False end ) as "是否为过强覆盖路段",

/*    (case when azimuth_diff.Azimuth_diff >= 90  and kpi.polygon_sample >= 10 then True else False end ) as "是否为方位角差异过大路段",*/
    (case when mod3.mod3_sample/kpi.polygon_sample >= 0.2 and kpi.polygon_sample >= 10 then True else False end ) as "是否为存在模三干扰路段",
    
    sinr_polygon.polygon

from
    sinr_polygon

/*left join
    cell_sector on cell_sector.ECI = sub_polygon.eci*/

left join
    (select
        base.sinr_polygon_id,
        max(base.centroid_text) as centroid_text,
        round(max(base.area)::numeric,2) as area,
        round(max(base.perimeters)::numeric,2) as perimeters,
        round(avg(base.RSRP),2) as avg_rsrp,
        count(DISTINCT base.ECI) as eci_count,
        count(*) as polygon_sample,
        sum(case when base.RSRP < -110 then 1 else 0 end) as poor_cover_sample,
        round(sum(case when base.RSRP < -110 then 1 else 0 end) / count(*)::numeric,4) as poorcover_ratio,
    
        round(avg(base.SINR),2) as avg_sinr,
        sum(case when base.SINR <= -2 then 1 else 0 end) as sinr_len2_sample,
        sum(case when base.SINR <= 5 then 1 else 0 end) as sinr_le5_sample,

        round(sum(case when base.SINR <= -2 then 1 else 0 end) / count(*)::numeric,4) as sinr_len2_ratio,
        round(sum(case when base.SINR <= 5 then 1 else 0 end) / count(*)::numeric,4) as sinr_le5_ratio,

        
        sum(case when overlap_sample.overlap_number >= 3 then 1 else 0 end) as overlap_sample,
        round(sum(case when overlap_sample.overlap_number >= 3 then 1 else 0 end) / count(*)::numeric,4) as overlap_ratio,

        sum(case when strong_cover.strongcover_number >= 3 then 1 else 0 end) as strongcover_sample,
        round(sum(case when strong_cover.strongcover_number >= 3 then 1 else 0 end) / count(*)::numeric,4) as strongcover_ratio
    
    from
        sinr_polygon_kpi_base base

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
                 sinr_polygon_kpi_base sample
            
            where
                    sample.RSRP >= -110    -- 主覆盖小区rsrp >= -110
                and sample."Cell 1st RSRP" >= -110  -- 邻小区rsrp >= -110

            
            ) as overlap_sample on overlap_sample.No = base.No

       
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
            sinr_polygon_kpi_base sample
            
        where
            sample."Cell 1st RSRP" >= -90
        ) as strong_cover on strong_cover.No = base.No
    
    group by
        base.sinr_polygon_id
       
    ) as kpi on kpi.sinr_polygon_id = sinr_polygon.id



        
 

        left join
        -- 重叠覆盖小区 影响最强的三个邻区  本路段内 重叠覆盖采样的的第一个邻区默认为电平最强邻小区,取个数 量最多的三个邻小区
        (select
            n_cell_rn.sinr_polygon_id,
        
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
                n_cell_order.sinr_polygon_id,
                n_cell_order.centroid_text,
                n_cell_order.n_pci,
                n_cell_order.ncell_avg_rsrp,
                n_cell_order.ncell_count,
                n_cell_order.ncell_reference,
                row_number()
                    OVER (
                    PARTITION BY n_cell_order.sinr_polygon_id
                    ORDER BY  n_cell_order.ncell_count DESC,n_cell_order.ncell_avg_rsrp DESC
                    ) AS rn
                   
            from
                (select
                    n_cell.sinr_polygon_id,
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
                        sample.sinr_polygon_id,
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
                       sinr_polygon_kpi_base sample
                     ) AS n_cell
                     

                where
                    overlap_number >= 3
                    and n_cell.pci <> n_cell.n_pci
                    /* 邻小区列表中一般第1个邻小区 与主服务小区的频点和PCI 是一样的( 同一个小区),
                       如果有其他邻小区的信号比主服务小区的信号强, 则主服务小区在邻小区中的位置会按找电平强度 降序排列.
                    
                       除主服务小区以外,有两个邻小区满足重叠覆盖的条件 (是重叠覆盖采样点)*/
                group by
                    n_cell.sinr_polygon_id,
                    n_cell.centroid_text,
                    n_cell.n_pci
                ) as n_cell_order
        
            ) as n_cell_rn
        
        where
            n_cell_rn.rn <= 3       -- 取影响最强的三个邻小区
        
        group by
            n_cell_rn.sinr_polygon_id
        
        order by
            n_cell_rn.sinr_polygon_id
        ) as overlap_ncell_rn3 on overlap_ncell_rn3.sinr_polygon_id = sinr_polygon.id


        
/*
    left join
        -- 模三干扰
        (select
            mod3_pci_count_3rd.sinr_polygon_id,
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
                mod3_pci_count_rn.sinr_polygon_id,
                count(*) as mod3_pci_count,
                max(case when mod3_pci_count_rn.rn = 1 then mod3_pci_count_rn.mod3_pci  end) as "mod3_1st_pci",
                max(case when mod3_pci_count_rn.rn = 1 then mod3_pci_count_rn.mod3_pci_count  end) as "mod3_1st_sample",
            
                max(case when mod3_pci_count_rn.rn = 2 then mod3_pci_count_rn.mod3_pci  end) as "mod3_2nd_pci",
                max(case when mod3_pci_count_rn.rn = 2 then mod3_pci_count_rn.mod3_pci_count  end) as "mod3_2nd_sample",
                
                max(case when mod3_pci_count_rn.rn = 3 then mod3_pci_count_rn.mod3_pci  end) as "mod3_3rd_pci",
                max(case when mod3_pci_count_rn.rn = 3 then mod3_pci_count_rn.mod3_pci_count  end) as "mod3_3rd_sample"
                
            from
                (select
                    mod3_pci_count.sinr_polygon_id,
                    mod3_pci_count.mod3_pci,
                    mod3_pci_count.mod3_pci_count,
                    row_number()
                        OVER (
                            PARTITION BY mod3_pci_count.sinr_polygon_id
                            ORDER BY mod3_pci_count.mod3_pci_count desc
                            ) AS rn
                       
                from
                    (select
                        mod3.sinr_polygon_id,
                        mod3.mod3_pci,
                        count(*) as mod3_pci_count
                    from
                        (select
                            mod3.sinr_polygon_id,
                            regexp_split_to_table(unnest(ARRAY[mod3.mod3_pci])::varchar,',') as mod3_pci
                        
                        from
                            (select
                                   sample.No,
                                   sample.PCTime,
                                   sample.Lon,
                                   sample.Lat,
                                   sample.ECI,
                                   sample.PCI,
                                   sample.sinr_polygon_id,
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
                                 sinr_polygon_kpi_base sample
                            
                            where
                                    sample.RSRP >= -110    -- 主覆盖小区rsrp >= -110
                                and sample."Cell 1st RSRP" >= -110  -- 邻小区rsrp >= -110
                                
                                -- and sub_polygon_id = 32
                            
                            ) as mod3
                        ) as mod3
                    
                    where
                        mod3.mod3_pci <> ''
                    
                    group by
                        mod3.sinr_polygon_id,
                        mod3.mod3_pci
                    ) as mod3_pci_count
                
                ) as mod3_pci_count_rn
                
            group by
                mod3_pci_count_rn.sinr_polygon_id
            ) as mod3_pci_count_3rd
        
        */
            LEFT JOIN
                -- 模三干扰的总采样点
                (select
                    mod3.sinr_polygon_id,
                    count(*) as mod3_sample
                from
                    (select
                        mod3.sinr_polygon_id,
                        regexp_split_to_table(unnest(ARRAY[mod3.mod3_pci])::varchar,',') as mod3_pci
                    
                    from
                        (select
                               sample.No,
                               sample.PCTime,
                               sample.Lon,
                               sample.Lat,
                               sample.ECI,
                               sample.PCI,
                               sample.sinr_polygon_id,
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
                             sinr_polygon_kpi_base sample
                        
                        where
                                sample.RSRP >= -110    -- 主覆盖小区rsrp >= -110
                            and sample."Cell 1st RSRP" >= -110  -- 邻小区rsrp >= -110
                            
                            -- and sub_polygon_id = 32
                        
                        ) as mod3
                    ) as mod3
                
                where
                    mod3.mod3_pci <> ''
                
                group by
                    mod3.sinr_polygon_id
                ) as mod3 on mod3.sinr_polygon_id = sinr_polygon.id

);


/* 表格导出  */
copy (SELECT * FROM sinr_polygon_kpi)
to 'D:\DT\export\sinr_polygon_kpi.csv' csv HEADER ENCODING 'gbk' DELIMITER ',' ;

copy (SELECT * FROM sub_polygon_kpi)
to 'D:\DT\export\sub_polygon_kpi.csv' csv HEADER ENCODING 'gbk' DELIMITER ',' ;














