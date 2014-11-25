#!/bin/bash
path=/home/admin/book2/data
#prime number: 1217, 2011, 601, 971, 1777, 797, 1009

/bin/awk 'NR%1217==1' $path/coolshell.log  > $path/coolshell_20140301.log
/bin/awk 'NR%2011==1' $path/coolshell.log  > $path/coolshell_20140302.log
/bin/awk 'NR%601==1' $path/coolshell.log  > $path/coolshell_20140303.log
/bin/awk 'NR%971==1' $path/coolshell.log  > $path/coolshell_20140304.log
/bin/awk 'NR%1777==1' $path/coolshell.log  > $path/coolshell_20140305.log
/bin/awk 'NR%797==1' $path/coolshell.log  > $path/coolshell_20140306.log
/bin/awk 'NR%1009==1' $path/coolshell.log  > $path/coolshell_20140307.log

function replace_date() {
    date=$1
    day=$2

    sed -e "s#12/Feb/2014#0$day/Mar/2014#g;" $path/coolshell_$date.log > $path/coolshell_$date.log.tmp
    mv $path/coolshell_$date.log.tmp $path/coolshell_$date.log
}

replace_date 20140301 1
replace_date 20140302 2
replace_date 20140303 3
replace_date 20140304 4
replace_date 20140305 5
replace_date 20140306 6
replace_date 20140307 7
