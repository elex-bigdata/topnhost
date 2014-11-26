#!/bin/sh
hadoop jar /home/hadoop/git_project_home/topnhost/target/topnhost-0.0.1-SNAPSHOT.jar com.elex.yac.TopNHost 100 30 1000 >> /home/hadoop/wuzhongju/yac.log 2>&1 &
hadoop fs -get /yac/ton_host/topN/nation* /home/hadoop/wuzhongju/yac/nation.txt
hadoop fs -get /yac/ton_host/topN/part* /home/hadoop/wuzhongju/yac/topn_host.csv
scp /home/hadoop/wuzhongju/yac/nation.txt /home/hadoop/wuzhongju/yac/topn_host.csv elex@162.243.114.236:~/topn_host
