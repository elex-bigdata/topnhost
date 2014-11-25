https://github.com/elex-bigdata/topnhost.git
hadoop jar topnhost-0.0.1-SNAPSHOT.jar com.elex.yac.TopNHost 100 1 10
====参数====
第一个参数为topN
第二个参数为天数
第三个参数为过滤阈值

hadoop fs -get /yac/ton_host/topN/nation* nation.txt
hadoop fs -get /yac/ton_host/topN/part* result.txt

java -classpath .:topnhost-0.0.1-SNAPSHOT.jar:/usr/lib/hadoop-hdfs/* com.elex.yac.SR /home/hadoop/yac.txt