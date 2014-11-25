#!/bin/bash

date=$1
dir=/home/admin/odps_book/introduction/src/sql/load/
clt=/home/admin/odps_book/console/clt/bin/odps
dship=/home/admin/odps_book/console/dship/dship

cd $dir

for f in `/bin/ls load_*.sql`; do
    echo $f
    tmp=$f.$date.tmp
    sed -e "s/\$bizdate\\$/$date/g;" $f > $tmp
    echo "$tmp"
    cat $tmp
    
    #clt command or dship command?
    echo "$f" | grep "dship" >&/dev/null
    if [ $? -ne 0 ]; then
        $clt -f $tmp
    else
        echo "$dship `/bin/cat $tmp` "
        $dship `/bin/cat $tmp` 
    fi
    if [ $? -ne 0 ]; then 
       echo "run $tmp failed."
       exit -1;
    fi
    rm -f $tmp
done
exit 0
