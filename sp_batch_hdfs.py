from pyspark import SparkConf, SparkContext
from jsonrpc.authproxy import AuthServiceProxy
import json
import sys

#This is batch processing of bitcoind (locally run bitcoin daemon)
#RPC (Remote Procedure Call) block's json stored
#in HDFS. Currently 187,990 blocks' json representation is
#stored in HDFS. The HDFS file size is around 6.5GB
#The output of this program is block_number and the corresponding
#transaction fee in units of Satoshi. This data is written to HBASE
#table.
#The program takes only 69 minutes to run. While the streaming version
#of the program takes XXX minutes. 
#It is a Good illustration of time-space(memory) tradeoff

conf = SparkConf().setMaster("local").setAppName("bitcoin_TransactionFee_calcultor")
sc = SparkContext(conf=conf)

rpcuser="bitcoinrpc"
rpcpassword="5C3Y6So6sCRPgBao8KyWV2bYpTHZt5RCVAiAg5JmTnHr"
rpcip = "127.0.0.1"
bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpassword+"@"+rpcip+":8332")



#function SaveRecord: saves tx_fee for a block to hbase database
def SaveRecord(tx_fee_rdd):  
    host = 'localhost'  #sys.argv[1]  
    table = 'tx_fee_table_sp_batch'	#needs to be created before hand in hbase shell  
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
	return gen_tx_json
	


content_rdd = sc.textFile("hdfs://ec2-52-21-47-235.compute-1.amazonaws.com:9000/bitcoin/block_chain_full.txt")
#The file below is for testing purposes
#content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/bitcoin/2_blocks.txt")

dump_rdd = content_rdd.map(lambda x: json.dumps(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#print dump_rdd.take(2)
load_rdd = dump_rdd.map(lambda x: json.loads(x))
#print load_rdd.take(2)

split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#tx = load_rdd.filter(lambda x: "tx" in x)
#print split_blk_rdd.take(split_blk_rdd.count())

gen_tx_rdd = split_blk_rdd.map(lambda x : (x[8][1:7],x[6][4:68]) ) #this gets generation transactions
#print "*************HERE***************"
#print gen_tx_rdd.take(gen_tx_rdd.count())			  #from the blocks		

tx_json_rdd = gen_tx_rdd.map(lambda x : (x[0],get_tx_fee(x[1])) )	 #function call			  
#print tx_json_rdd.take(tx_json_rdd.count())

tx_fee_rdd = tx_json_rdd.map(lambda x : (x[0],x[1].items()
                                         [3][1][0]["value"]-25) )#.filter(lambda x : "value" in x)
#print tx_fee_rdd.take(tx_fee_rdd.count())

SaveRecord(tx_fee_rdd)  #function call

#just to display values for debugging
#val_lst = tx_fee_rdd.take(tx_fee_rdd.count())		#use [3][1]
#print val_lst
