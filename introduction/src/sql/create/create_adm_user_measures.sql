DROP TABLE IF EXISTS adm_user_measures;
CREATE TABLE IF NOT EXISTS adm_user_measures(
    device STRING COMMENT 'such as android, iphone, ipad...',
    pv BIGINT,
    uv BIGINT)
PARTITIONED BY(dt STRING);
