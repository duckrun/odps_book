DROP TABLE IF EXISTS dim_user_info;

CREATE TABLE IF NOT EXISTS dim_user_info(
    uid STRING COMMENT 'unique user id',
    ip STRING COMMENT 'client ip address',
    city STRING,
    device STRING,
    protocol STRING,
    identity STRING  COMMENT 'user, crawler, feed',
    agent STRING);
