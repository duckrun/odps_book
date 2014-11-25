#!/bin/bash
path=/home/admin/book2/data

function parse() {
    date=$1
    mkdir -p $path/$date/
    python parse.py $path/coolshell_$date.log  $path/$date/output.log $path/$date/dirty.log
}
parse 20140301
exit 0
parse 20140212

parse 20140301
parse 20140302
parse 20140303
parse 20140304
parse 20140305
parse 20140306
parse 20140307

