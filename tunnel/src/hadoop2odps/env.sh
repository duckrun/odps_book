#!/bin/bash

workdir=$(cd $(dirname $BASH_SOURCE) && pwd)
cd $workdir

for jarf in $(ls lib/*.jar);do
    HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$workdir/$jarf
done

