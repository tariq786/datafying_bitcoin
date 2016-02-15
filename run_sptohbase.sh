#spark-submit --jars /usr/local/spark/lib/spark-examples-1.5.2-hadoop2.4.0.jar --jars /usr/local/hbase/lib/hbase-examples-1.1.2.jar sp_json.py localhost 2389 > sp_json.log

#spark-submit --driver-class-path /usr/local/spark/lib/spark-examples-1.5.2-hadoop2.4.0.jar sp_json.py localhost 2389 > sp_json.log

spark-submit --jars /usr/local/spark/lib/spark-examples-1.5.2-hadoop2.4.0.jar,/usr/local/hbase/lib/hbase-examples-1.1.2.jar sp_json.py localhost 2399 > sp_json.log
##spark-submit --jars /usr/local/spark/lib/spark-examples-1.5.2-hadoop2.4.0.jar,./hbase-0.92.1.jar,/usr/local/hbase/lib/hbase-examples-1.1.2.jar sp_json.py localhost 2399 > sp_json.log
