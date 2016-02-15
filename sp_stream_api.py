from pyspark import SparkContext
from jsonrpc.authproxy import AuthServiceProxy
from pyspark.streaming import StreamingContext
import json

#This is spark streaming application
#Producer is gen_sp_stream_api.py that makes http get requests
#to (https://bitcoin.toshi.io/). Result of this request is json
#of bitcoin block which is then relayed by gen_sp_stream_api.py
#to this application via Netcat relay.
#
#Producer is run like following
#python gen_sp_stream_api.py | netcat -l 8888


# Create a local StreamingContext with * working thread and batch interval of 1 second
sc = SparkContext("local[*]", "txcount")
ssc = StreamingContext(sc, 0.5) #0.001 did 9710 blocks in 12 minutes

#function SaveRecord: saves tx_fee for a block to hbase database
def SaveRecord(tx_fee_rdd):  
    host = 'localhost'  #sys.argv[1]  
    table = 'transaction_fee_table'	#needs to be created before hand in hbase shell  
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


lines = ssc.socketTextStream("localhost", 8888)
dump_rdd = lines.map(lambda x: json.dumps(x))
load_rdd = dump_rdd.map(lambda x: json.loads(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#load_rdd.pprint(2)

split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#split_blk_rdd.pprint()

tx_fee_rdd = split_blk_rdd.map(lambda x : (x[14][1:7],x[15][1:-15])) #this gets transaction fee
#tx_fee_rdd.pprint(200)		#works
tx_fee_rdd.foreachRDD(SaveRecord)		#function call




ssc.start()             # Start the computation
#ssc.awaitTermination()  # Wait for the computation to terminate
ssc.awaitTerminationOrTimeout(15000) #13000#time out in 3 hours
#ssc.stop()  # Wait for the computation to terminate
