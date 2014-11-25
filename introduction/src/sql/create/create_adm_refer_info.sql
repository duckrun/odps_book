DROP TABLE IF EXISTS adm_refer_info;
CREATE TABLE adm_refer_info(
    referer STRING,
    count BIGINT)
PARTITIONED BY(dt STRING);
