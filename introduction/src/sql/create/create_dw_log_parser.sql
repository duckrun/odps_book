DROP TABLE IF EXISTS dw_log_parser;
CREATE TABLE dw_log_parser(
    ip STRING COMMENT 'client ip address',
    user STRING,
    time DATETIME,
    method STRING COMMENT 'HTTP request type, such as GET POST...',
    url STRING,
    protocol STRING,
    status BIGINT COMMENT 'HTTP reponse code from server',
    size BIGINT,
    referer STRING,
    agent STRING)
PARTITIONED BY(dt STRING);
