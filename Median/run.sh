#!/bin/bash
$HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main Median.java
jar cf Median.jar Median*.class
rm -rf ../tmp_output
$HADOOP_HOME/bin/hadoop jar Median.jar Median ../wc_input ../tmp_output
g++ Sort_Print_Median.cpp -o Sort_Print_Median
./Sort_Print_Median
rm -rf ../tmp_output
