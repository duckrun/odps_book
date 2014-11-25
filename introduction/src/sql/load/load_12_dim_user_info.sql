sql

INSERT INTO TABLE dim_user_info
SELECT
    t2.uid,
    t2.ip,
    t2.city,
    t2.device,
    t2.protocol,
    t2.identity,
    t2.agent
FROM (
    SELECT
        md5(concat(t1.ip, t1.device, t1.protocol, t1.identity, t1.agent)) as uid,
        t1.ip,
        ip2region(t1.ip, "city") as city,
        t1.device,
        t1.protocol,
        t1.identity,
        t1.agent
    FROM
        (SELECT ip, protocol, agent, device, identity
         FROM dw_log_detail
         WHERE dt='$bizdate$'
         GROUP BY ip, protocol, agent, device, identity
        ) t1
)t2
LEFT OUTER JOIN dim_user_info dim
ON t2.uid = dim.uid
WHERE dim.uid is null;
