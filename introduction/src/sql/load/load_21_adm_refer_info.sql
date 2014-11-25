sql

ALTER TABLE adm_refer_info ADD IF NOT EXISTS PARTITION (dt='$bizdate$');

INSERT OVERWRITE TABLE adm_refer_info PARTITION (dt='$bizdate$')
SELECT referer, count(*) as cnt
FROM dw_log_fact
WHERE length(referer) > 1
  AND dt='$bizdate$'
GROUP BY referer;
