#!/bin/bash

path=/var/log/nginx
bin=/home/admin/bin
console=$bin/clt/bin/odpscmd
dship=$bin/dship/dship

date=`date -d yesterday "+%Y%m%d"`
table="ods_log_tracker"
output=/home/admin/book2/output/$date
log=/home/admin/book2/log/$date

mkdir -p $output
mkdir -p $log

#1) create table/partition
function prepare() {
    $console -e "ALTER TABLE $table DROP IF EXISTS  PARTITION(dt='$date');"
    $console -e "ALTER TABLE $table ADD PARTITION (dt='$date');"
    return $?
}

function upload() {
    pssh -h host.txt -i "sh $bin/upload.sh $date $table/dt='$date'"
    return $?
}

function check() {
    t1=`date +%s`
    limit = 43200
    while true; do
        t2 = `date +%s`
        if [ $((t2-t1)) -gt $limit ]; then
            echo "time out"
            return 1;
        fi
        pslurp -h host.txt  -L $output $output result.txt >> $log/master.log 2>&1
        if [ $? -ne 0 ]; then
            sleep 60;
        else
            ret = 0
            for dir in `/bin/ls $output`; do
                grep 0 $output/$dir/result.txt >&/dev/null 
                if [ $? -ne 0 ]; then
                    echo "$dir failed."
                    ret = 1
                fi
            done
            return ret;
        fi
    done
}

function delete() {
    # delete older data files and tmp files
}
