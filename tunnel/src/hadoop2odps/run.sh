#!/bin/bash

# ./run.sh hdfs://<ip>:<port>/<hadoop_path> <odps_destination_table> <odps_config_file>
source ./env.sh

for jarf in $(ls lib/*.jar);do
    libjars=$workdir/$jarf,$libjars
done
libjars=${libjars%,}

path=$1
table=$2
conf=$3

hadoop jar  build/hdfs2odps-0.1.jar \
-libjars $libjars\
 -h $path\
 -t $table\
 -c $conf

