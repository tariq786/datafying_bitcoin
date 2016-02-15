from pyspark import SparkConf, SparkContext
from jsonrpc.authproxy import AuthServiceProxy
import json
import sys


conf = SparkConf().setMaster("local").setAppName("bitcoin_TransactionFee_calcultor")
sc = SparkContext(conf=conf)

rpcuser="bitcoinrpc"
rpcpassword="5C3Y6So6sCRPgBao8KyWV2bYpTHZt5RCVAiAg5JmTnHr"
rpcip = "127.0.0.1"
bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpassword+"@"+rpcip+":8332")


#function SaveRecord: saves tx_fee for a block to hbase database
def SaveRecord(tx_fee_rdd):  
    host = 'localhost'  #sys.argv[1]  
    table = 'tx_fee_table'	#needs to be created before hand in hbase shell  
    conf = {"hbase.zookeeper.quorum": host,
            "hbase.mapred.outputtable": table,
            "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
            "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
    keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
   #row key id,id, cfamily=tx_fee_col,column_name = tx_fee, column_value=x 
    datamap = tx_fee_rdd.map(lambda x: (("1"),["tx_fee_col","tx_fee",x] ) )  
    datamap.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)  



def get_tx_fee(gen_tx):
	gen_tx_json = bitcoinrpc.decoderawtransaction(bitcoinrpc.getrawtransaction(gen_tx))
	return gen_tx_json
	

#content_rdd = sc.wholeTextFiles("file:///home/ubuntu/unix_practice/spark-example/block_json_395545.txt",use_unicode=False)
#content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/bitcoin/2_blocks.txt")

content_rdd = sc.textFile("hdfs://ec2-52-21-47-235.compute-1.amazonaws.com:9000/bitcoin/block_chain_full.txt")
dump_rdd = content_rdd.map(lambda x: json.dumps(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#print dump_rdd.take(2)
load_rdd = dump_rdd.map(lambda x: json.loads(x))
#print load_rdd.take(2)

split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#tx = load_rdd.filter(lambda x: "tx" in x)
#print tx.take(tx.count())
#print tx.count()

gen_tx_rdd = split_blk_rdd.map(lambda x : x[6][4:68]) #this gets generation transactions
print gen_tx_rdd.take(gen_tx_rdd.count())			  #from the blocks		

tx_json_rdd = gen_tx_rdd.map(get_tx_fee) #funcion call
#print tx_json_rdd.take(tx_json_rdd.count())

tx_fee_rdd = tx_json_rdd.map(lambda x : x.items()[3][1][0]["value"]-25)#.filter(lambda x : "value" in x)
val_lst = tx_fee_rdd.take(tx_fee_rdd.count())		#use [3][1]
print val_lst
