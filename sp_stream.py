from pyspark import SparkContext
from jsonrpc.authproxy import AuthServiceProxy
from pyspark.streaming import StreamingContext
import json

#This is stream processing of bitcoind (locally run bitcoin daemon)
#using netcat relay. One netcat program acts as producer.
#One on terminal, run
#$time python ./batch_load_blocks_rpc.py | netcat -l 9999
#On a different terminal run
#time ./run_sp_stream.sh
#The output of this program is block_number and the corresponding
#transaction fee in units of Satoshi. This data is written to HBASE
#table.
#the program takes 177 minutes to run while the batch takes 69 minutes
#It is a Good illustration of time-space(memory) tradeoff

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[*]", "txcount")
ssc = StreamingContext(sc, 0.5)

rpcuser="bitcoinrpc"
rpcpassword="5C3Y6So6sCRPgBao8KyWV2bYpTHZt5RCVAiAg5JmTnHr"
rpcip = "127.0.0.1"
bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpassword+"@"+rpcip+":8332")

#function SaveRecord: saves tx_fee for a block to hbase database
def SaveRecord(tx_fee_rdd):  
    host = 'localhost'  #sys.argv[1]  
    table = 'tx_fee_table_sp_stream'	#needs to be created before hand in hbase shell  
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



def get_tx_fee(gen_tx):
	gen_tx_json = bitcoinrpc.decoderawtransaction(bitcoinrpc.getrawtransaction(gen_tx))
	#print gen_tx_json
	return gen_tx_json


#get lines RDD
lines = ssc.socketTextStream("localhost", 9999)
dump_rdd = lines.map(lambda x: json.dumps(x))
#print dump_rdd.take(2)
load_rdd = dump_rdd.map(lambda x: json.loads(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#print load_rdd.take(2)

#load_rdd.pprint(100)
#tx = load_rdd.flatMap(lambda x: x.split(":")) #this works
split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#split_blk_rdd.pprint()

gen_tx_rdd = split_blk_rdd.map(lambda x : (x[8][1:7],x[6][4:68]) ) #this gets generation transactions
#gen_tx_rdd.pprint()		#works

tx_json_rdd = gen_tx_rdd.map(lambda x: (x[0],get_tx_fee(x[1])) )	#function call			  
tx_fee_rdd = tx_json_rdd.map(lambda x : (x[0],x[1].items()
										[3][1][0]["value"]-25) )#.filter(lambda x : "value" in x)

tx_fee_rdd.foreachRDD(SaveRecord)		#function call


ssc.start()             # Start the computation
#ssc.awaitTermination()  # Wait for the computation to terminate
ssc.awaitTerminationOrTimeout(12000) #time out 3.33 hours
#ssc.stop()  # Wait for the computation to terminate
