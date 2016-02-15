import sys  
import json  
from pyspark import SparkContext  
from pyspark.streaming import StreamingContext  

#function SaveRecord
def SaveRecord(rdd):  
    host = sys.argv[1]  
    table = 'cats'  
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
   #row key id,row key id, cfamily,column_name = cats_jon, column_value    
    datamap = rdd.map(lambda x: (str(json.loads(x)["id"]),[str(json.loads(x)["id"]),"cfamily","cats_json",x]) )  
    datamap.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)  
    

if __name__ == "__main__":  
    if len(sys.argv) != 3:  
      print("Usage: StreamCatsToHBase.py <hostname> <port>")  
      exit(-1)  
    
    sc = SparkContext(appName="StreamCatsToHBase")  
    ssc = StreamingContext(sc, 1)  
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) #argv1 = localhost, argv2 = portname
    print lines 
    lines.foreachRDD(SaveRecord)  
    
    ssc.start()       # Start the computation  
    ssc.awaitTermination() # Wait for the computation to terminate  
