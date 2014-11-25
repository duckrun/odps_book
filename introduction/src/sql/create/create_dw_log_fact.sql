DROP TABLE IF EXISTS dw_log_fact;

CREATE TABLE IF NOT EXISTS dw_log_fact(
    uid STRING COMMENT 'unique user id',
    time DATETIME,
    method STRING COMMENT 'HTTP request type, such as GET POST...',
    url STRING,
    status BIGINT COMMENT 'HTTP reponse code from server',
    size BIGINT,
    referer STRING)
PARTITIONED BY(dt STRING);
