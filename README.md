#Solution for
###Insight Data Engineer Coding Challenge 
-Hang Chu, Cornell University


##How to run
This solution uses Hadoop MapReduce. I used the standard configuration of Hadoop (as suggested in the official tutorial). If the similar thing hasn't been done in your computer, do the following (assuming you are using Ubuntu 14.04):
		
* Download file hadoop-2.6.0.tar.gz at this link: 

		http://apache.mirrors.hoobly.com/hadoop/common/hadoop-2.6.0/

* Extract hadoop-2.6.0.tar.gz to {WHERE YOUR HADOOP IS}. For example, my {WHERE YOUR HADOOP IS} is ~/hadoop\-2.6.0

* Edit your ~/.bashrc

* Add the following lines to the end of file. For example, my {WHERE YOU INSTALLED JAVA} is /usr/lib/jvm/java-7-openjdk-amd64

		export HADOOP_HOME={WHERE YOUR HADOOP IS}
		export JAVA_HOME={WHERE YOU INSTALLED JAVA}
		export PATH=$JAVA_HOME/bin:$PATH
		export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar

* Download this code, extract it.

* In the directory where you extracted this code, do

		chmod +x run.sh
		./run.sh

##Brief Solution Description

###Task 1: Word Count
* I used Hadoop MapReduce: the mapper produces <word,1> pairs, the reducer sums pairs up. Source code can be found at ./Word\_Count/WordCount.java

###Task 2: Moving Median
* This is more difficult than Task 1, as for every line, the information of all previous lines is needed. I did this in two steps:
	* First use Hadoop MapReduce to compute the number of word per line, and write a triplet <line\_byte\_offset, filename, number\_of\_words> for each line. This is done in a scalable manner. After this is done, the input text files are transfered into a small intermediate file recording all the triplets. Source code can be found at ./Median/Median.java
	* A serial code for computing the moving medians, using the produced triplets. For optimal efficiency, I implemented this in c++. Source code can be found at ./Median/Sort\_Print\_Median.cpp