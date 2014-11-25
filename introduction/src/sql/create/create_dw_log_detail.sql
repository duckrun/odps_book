DROP TABLE IF EXISTS dw_log_detail;

CREATE TABLE IF NOT EXISTS dw_log_detail(
    ip STRING COMMENT 'client ip address',
    time DATETIME,
    method STRING COMMENT 'HTTP request type, such as GET POST...',
    url STRING,
    protocol STRING,
    status BIGINT COMMENT 'HTTP reponse code from server',
    size BIGINT,
    referer STRING COMMENT 'referer domain',
    agent STRING,
    device STRING COMMENT 'android|iphone|ipad...',
    identity STRING  COMMENT 'identify: user, crawler, feed')
PARTITIONED BY(dt STRING);
