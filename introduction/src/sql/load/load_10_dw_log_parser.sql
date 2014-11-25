sql
ALTER TABLE dw_log_parser ADD IF NOT EXISTS PARTITION (dt='$bizdate$');

INSERT OVERWRITE TABLE dw_log_parser PARTITION(dt='$bizdate$')
SELECT ip, user, time,
    regexp_substr(request, "(^[^ ]+ )") as method,
    regexp_extract(request, "^[^ ]+ (.*) [^ ]+$") as url,
    regexp_substr(request, "([^ ]+$)") as protocol,
    status, size, referer, agent
FROM ods_log_tracker
WHERE dt='$bizdate$';
