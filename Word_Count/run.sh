#!/bin/bash
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main WordCount.java
jar cf WordCount.jar WordCount*.class
rm -rf ../tmp_output
$HADOOP_HOME/bin/hadoop jar WordCount.jar WordCount ../wc_input ../tmp_output
cp ../tmp_output/part-r-00000 ../wc_output/wc_result.txt
rm -rf ../tmp_output