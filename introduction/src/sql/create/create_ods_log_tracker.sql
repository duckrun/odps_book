DROP TABLE IF EXISTS ods_log_tracker;
CREATE TABLE ods_log_tracker(
    ip STRING COMMENT 'client ip address',
    user STRING,
    time DATETIME,
    request STRING COMMENT 'HTTP request type + requested path without args + HTTP protocol version',
    status BIGINT COMMENT 'HTTP reponse code from server',
    size BIGINT,
    referer STRING,
    agent STRING)
COMMENT 'Log from coolshell.cn'
PARTITIONED BY(dt STRING);

