sql

ALTER TABLE dw_log_fact ADD IF NOT EXISTS PARTITION (dt='$bizdate$');

INSERT OVERWRITE TABLE dw_log_fact PARTITION (dt='$bizdate$')
SELECT u.uid, d.time, d.method, d.url, d.status, d.size, d.referer
FROM dw_log_detail d
JOIN dim_user_info u
ON (d.ip = u.ip and d.protocol=u.protocol and d.agent=u.agent)
AND d.dt='$bizdate$'
AND u.dt='$bizdate$';
