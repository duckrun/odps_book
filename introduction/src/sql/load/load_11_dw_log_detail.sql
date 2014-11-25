sql
ALTER TABLE dw_log_detail ADD IF NOT EXISTS PARTITION (dt='$bizdate$');

INSERT OVERWRITE TABLE dw_log_detail PARTITION (dt='$bizdate$')
SELECT
    ip,
    time,
    method,
    url,
    protocol,
    status,
    size,
    regexp_extract(referer,"^[^/]+://([^/]+){1}") as referer,
    agent,
    CASE WHEN tolower(agent) RLIKE "android" then "android"
         WHEN tolower(agent) RLIKE "iphone" then "iphone"
         WHEN tolower(agent) RLIKE "ipad" then "ipad"
         WHEN tolower(agent) RLIKE "macintosh" then "macintosh"
         WHEN tolower(agent) RLIKE "windows phone" then "windows_phone"
         WHEN tolower(agent) RLIKE "windows" then "windows_pc"
         ELSE "unknown"
    END as device,
    CASE WHEN tolower(agent) RLIKE "(bot|spider|crawler|slurp)" then "crawler"
         WHEN tolower(agent) RLIKE "feed" or url RLIKE "feed" then "feed"
         WHEN tolower(agent)  not Rlike "(bot|spider|crawler|feed|slurp)"
            AND agent RLIKE "^[Mozilla|Opera]"
            AND url not RLIKE "feed"  then "user"
         ELSE "unknown"
    END as identity
FROM dw_log_parser
WHERE url not RLIKE "^[/]+wp-"
    AND dt='$bizdate$';
