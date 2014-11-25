-- [NOTE] SQL does not support comment line, we use it here only for clearity

-- dim_ip_area
CREATE TABLE IF NOT EXISTS dim_ip_area(
    area_id STRING COMMENT 'unique id',
    country STRING,
    province STRING,
    city STRING,
    district STRING,
    school STRING COMMENT 'net provider') ;

SELECT
    ip2region(ip, 'country') as country,
    ip2region(ip, 'province') as province,
    ip2region(ip, 'city') as city,
    ip2region(ip, 'district') as district,
    ip2region(ip, 'school') as school
FROM dw_log_detail
WHERE dt='20140212' and ip <> '';

SELECT
    md5(concat(country, province, city, district, school)) as area_id,
    country,
    province,
    city, 
    district, 
    school
FROM
    (SELECT
        ip2region(ip, 'country') as country,
        ip2region(ip, 'province') as province,
        ip2region(ip, 'city') as city,
        ip2region(ip, 'district') as district,
        ip2region(ip, 'school') as school
    FROM dw_log_detail
    WHERE dt='20140212' and ip <> '')a
GROUP BY country, province, city, district, school
LIMIT 10;

SELECT
    md5(concat(
            CASE WHEN country is NULL then '' else country end,
            CASE WHEN province is NULL then '' else province end,
            CASE WHEN city is NULL then '' else city end,
            CASE WHEN district is NULL then '' else district end,
            CASE WHEN school is NULL then '' else school end )) as area_id,
    country,
    province,
    city,
    district,
    school
FROM(
    SELECT
        ip2region(ip, 'country') as country,
        ip2region(ip, 'province') as province,
        ip2region(ip, 'city') as city,
        ip2region(ip, 'district') as district,
        ip2region(ip, 'school') as school
    FROM dw_log_detail
    WHERE dt='20140212' and ip <> ''
)t1
GROUP BY country, province, city, district, school
LIMIT 10;

INSERT OVERWRITE TABLE dim_ip_area
SELECT
    t3.area_id,
    t3.country,
    t3.province,
    t3.city,
    t3.district,
    t3.school
FROM (
    SELECT
        CASE WHEN dim.area_id is not NULL then dim.area_id ELSE t2.area_id END as area_id,
        CASE WHEN dim.country is not NULL then dim.country ELSE t2.country END as country,
        CASE WHEN dim.province is not NULL then dim.province ELSE t2.province END as province,
        CASE WHEN dim.city is not NULL then dim.city ELSE t2.city END as city,
        CASE WHEN dim.district is not NULL then dim.district ELSE t2.district END as district,
        CASE WHEN dim.school is not NULL then dim.school ELSE t2.school END as school
    FROM(
        SELECT
            md5(concat(
                    CASE WHEN country is NULL then '' else country end,
                    CASE WHEN province is NULL then '' else province end,
                    CASE WHEN city is NULL then '' else city end,
                    CASE WHEN district is NULL then '' else district end,
                    CASE WHEN school is NULL then '' else school end )) as area_id,
            country,
            province,
            city,
            district,
            school
        FROM(
            SELECT
                ip2region(ip, 'country') as country,
                ip2region(ip, 'province') as province,
                ip2region(ip, 'city') as city,
                ip2region(ip, 'district') as district,
                ip2region(ip, 'school') as school
            FROM dw_log_detail
            WHERE dt='20140212' and ip <> ''
        )t1
        GROUP BY country, province, city, district, school
    )t2
    FULL OUTER JOIN dim_ip_area dim
    ON t2.area_id = dim.area_id
)t3;

-- user's visit path
SELECT f.uid, f.url, f.time
FROM dw_log_fact f
JOIN dim_user_info u
ON f.uid = u.uid 
AND f.dt='20140212'
AND u.identity = 'user'
GROUP BY f.uid, f.url, f.time
ORDER BY uid, time
LIMIT 10;

CREATE TABLE IF NOT EXISTS adm_visit_path (
    uid STRING COMMENT 'user_id',
    url STRING COMMENT 'visit url',
    time DATETIME COMMENT 'visit time, format "YYYY-MM-DD HH:mm:ss"',
    cnt BIGINT COMMENT 'count by (uid, url, time), ie. counts of a "uid"  visit a "url" at "time"')
PARTITIONED BY(dt STRING);

ALTER TABLE adm_visit_path ADD IF NOT EXISTS PARTITION (dt='20140212');

INSERT OVERWRITE TABLE  adm_visit_path PARTITION (dt='20140212')
SELECT /*+ MAPJOIN(u) */
    f.uid, f.url, f.time, count(*) as cnt
FROM dw_log_fact f
JOIN dim_user_info u
ON f.uid = u.uid
AND f.dt='20140212'
AND u.identity = 'user'
GROUP BY f.uid, f.url, f.time;


CREATE VIEW adm_pv_top_100_view as
SELECT uid, pv, row_number() over (partition by 1 order by pv desc) as pv_rank
FROM (
    SELECT uid, sum(cnt) as pv
    FROM adm_visit_path p
    GROUP BY uid
    ORDER by pv desc
    LIMIT 100) a;

SELECT /*+ MAPJOIN(v) */
    p.uid, p.url, p.time, p.cnt, v.pv, v.pv_rank
FROM adm_visit_path p
JOIN adm_pv_top_100_view v
ON p.uid = v.uid
LIMIT 100;

-- TopK query
SELECT ip, url
FROM (
SELECT 
ip, url, 
row_number() over(partition by ip order by cnt desc) as rank
FROM (
     SELECT ip, url, count(*) as cnt
         FROM dw_log_detail
         GROUP BY ip, url
     ) t1
)t2
WHERE t2.rank <=3;

-- ip black_list
DROP TABLE if EXISTS tmp_ip_404;
DROP TABLE if EXISTS dw_ip_blacklist;

CREATE VIEW tmp_ip_404 AS
SELECT ip, time, url
FROM dw_log_detail
WHERE status=404
AND length(referer) <= 1 
AND dt='20140212';

-- simple join    
CREATE TABLE dw_ip_blacklist AS
SELECT t2.ip, t3.time, t3.url, t2.cnt as total_404_cnt
FROM (
SELECT t1.ip, t1.cnt 
FROM (
SELECT ip, count(*) as cnt
        FROM tmp_ip_404
        GROUP BY ip
) t1
WHERE t1.cnt > 100
) t2
JOIN tmp_ip_404 t3
ON t2.ip = t3.ip
GROUP BY t2.ip, t3.time, t3.url, t2.cnt
ORDER BY total_404_cnt desc
LIMIT 1000000;

DROP TABLE IF EXISTS dw_ip_blacklist;

-- mapjoin
CREATE TABLE dw_ip_blacklist AS
SELECT /*+ MAPJOIN(t2) */
    t2.ip, t3.time, t3.url, t2.cnt as total_404_cnt
FROM (
SELECT t1.ip, t1.cnt 
FROM (
SELECT ip, count(*) as cnt
    FROM tmp_ip_404
    GROUP BY ip
) t1
WHERE t1.cnt > 100
) t2
JOIN tmp_ip_404 t3
ON t2.ip = t3.ip
GROUP BY t2.ip, t3.time, t3.url, t2.cnt
ORDER BY total_404_cnt desc
LIMIT 1000000;








