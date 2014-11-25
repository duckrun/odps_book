#!/bin/bash

console=/home/admin/book2/clt/bin/odpscmd
dship=/home/admin/book2/dship/dship
path=/var/log/nginx

date=$1
#with partition, such as ods_log_tracker/dt=201302
table=$2
#gzip filename format: access.log.20140212.gz
file=$path/access.log.$date

output=/home/admin/book2/output/$date
mkdir -p $output

#1) gunzip

function parse() {
    if [ ! -f "$file.gz" ]; then
        echo "file not exist"
        return 1
    fi
    gunzip $file.gz
    python parse.py $file $file.clean $file.dirty 
    return $?
}

function upload() {
    $dship upload $file.clean $table >> $output/log.txt 2>&1
    if [ $? == 0 ]; then
        echo 0 >$output/result.txt
        return 0
    else
        echo 1 >$output/result.txt
        return 1
    fi
}

unzip_file && upload
exit $?

