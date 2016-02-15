from pyspark import SparkConf, SparkContext
from jsonrpc.authproxy import AuthServiceProxy
import json


#This is batch processing of Toshi (https://bitcoin.toshi.io/) bitcoin block's json stored
#in HDFS. Currently 187,990 blocks' json representation is
#stored in HDFS. The HDFS file size is around 7GB
#The output of this program is block_number and the corresponding
#transaction fee in units of Satoshi. This data is written to HBASE
#table.
#The program takes only 3 minutes to run. While the streaming version
#of the program takes 222 minutes. 
#It is a Good illustration of time-space(memory) tradeoff

conf = SparkConf().setMaster("local").setAppName("Toshi_bitcoin_tx_fee_calcultor")
sc = SparkContext(conf=conf)


#function SaveRecord: saves tx_fee for a block to hbase database
def SaveRecord(tx_fee_rdd):  
    host = 'localhost'  #sys.argv[1]  
    table = 'tx_fee_taoshi_batch'	#needs to be created before hand in hbase shell  
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
    #row key id,id, cfamily=tx_fee_col,column_name = tx_fee, column_value=x 
    #datamap = tx_fee_rdd.map(lambda x: ("tx_fee",x) )  
    #( rowkey , [ row key , column family , column name , value ] )
    datamap = tx_fee_rdd.map(lambda x: (str(x[0]),
					       	[str(x[0]),"tx_fee_col","tx_fee",str(x[1])])
    						)
 			
    datamap.saveAsNewAPIHadoopDataset(conf=conf,
    								  keyConverter=keyConv,
    								  valueConverter=valueConv)


lines = sc.textFile("hdfs://ec2-52-21-47-235.compute-1.amazonaws.com:9000/bitcoin/taoshi_tx_fee.txt")

dump_rdd = lines.map(lambda x: json.dumps(x))
#print dump_rdd.take(2)
load_rdd = dump_rdd.map(lambda x: json.loads(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#print load_rdd.take(2)


#tx = load_rdd.flatMap(lambda x: x.split(":")) #this also works but flatMap is not needed
split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#print split_blk_rdd.take(2)

tx_fee_rdd = split_blk_rdd.map(lambda x : (x[14][1:7],x[15][1:-15])) #this gets (blocknum,transaction fee) tuple
#print tx_fee_rdd.take(2)		#works
SaveRecord(tx_fee_rdd)		#function call to save to Hbase
