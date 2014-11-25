sql

ALTER TABLE adm_user_measures ADD IF NOT EXISTS PARTITION (dt='$bizdate$');

INSERT OVERWRITE TABLE adm_user_measures PARTITION (dt='$bizdate$')
SELECT u.device, count(*) as pv, count(distinct u.uid) as uv
FROM dw_log_fact f
JOIN dim_user_info u
ON f.uid = u.uid
AND u.identity='user'
AND f.dt='$bizdate$'
AND u.dt='$bizdate$'
GROUP BY u.device;
