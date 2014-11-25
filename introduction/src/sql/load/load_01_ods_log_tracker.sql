sql
ALTER TABLE ods_log_tracker DROP IF EXISTS  PARTITION (dt='$bizdate$');
ALTER TABLE ods_log_tracker ADD PARTITION (dt='$bizdate$');

