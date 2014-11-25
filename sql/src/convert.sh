#!/bin/bash

iconv -f gb18030 -t utf8 /home/admin/odps_book/data/t_alibaba_data.csv |
    sed -e 's/月/-/; s/日//'|
        awk -F "," 'BEGIN{OFS=","}
        NR>1{
            split($NF,arr,"-");
            $NF=sprintf("%02d%02d",arr[1],arr[2]); 
            print $0
        }'

