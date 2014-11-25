#!/bin/bash

date=$1
dir=/home/admin/book2/output/
dship=/home/admin/odps_book/console/dship/dship

function download_table() {
    table=$1
    path=$2
    $dship download $table/dt=$date $path/$date/$table.csv
    if [ $? -ne 0 ]; then 
        echo "[FAILED] download $table/dt=$date $path/$date/$table.csv"
    fi
}

mkdir -p $dir
download_table adm_user_measures  $dir 
download_table adm_refer_info  $dir 
