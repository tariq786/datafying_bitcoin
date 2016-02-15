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
	#print "inside get_tx_fee function"
	#print gen_tx
	#gen_tx_json = bitcoinrpc.gettxout(str(gen_tx),1)
	gen_tx_json = bitcoinrpc.decoderawtransaction(bitcoinrpc.getrawtransaction(gen_tx))
	#print gen_tx_json
	return gen_tx_json
	#gen_tx_value = int(gen_tx_out["value"])


#content_rdd = sc.wholeTextFiles("file:///home/ubuntu/unix_practice/spark-example/block_json_395545.txt",use_unicode=False)
#content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/spark-example/block_json_395545_rpc.txt")
#content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/bcrpc/bitcoin-inspector-webserver/bitcoin/block_384533.txt")
#content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/bcrpc/bitcoin-inspector-webserver/bitcoin/log.txt")
content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/bitcoin/2_blocks.txt")
#content_rdd = sc.textFile("hdfs://ec2-52-21-47-235.compute-1.amazonaws.com:9000/bitcoin/block_chain_full.txt")
#content_rdd = sc.textFile("file:///home/ubuntu/unix_practice/bitcoin/2_blocks.txt")
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
#print type(gen_tx_rdd)
tx_json_rdd = gen_tx_rdd.map(get_tx_fee)				  
#print tx_json_rdd.take(tx_json_rdd.count())
tx_fee_rdd = tx_json_rdd.map(lambda x : x.items()[3][1][0]["value"]-25)#.filter(lambda x : "value" in x)

#tx_fee_str_rdd = tx_fee_rdd.map(lambda x : json.dumps(str(x)) )
#print type(tx_fee_rdd)
#tx_fee_rdd.foreach(SaveRecord) #function call

#just to display values
val_lst = tx_fee_rdd.take(tx_fee_rdd.count())		#use [3][1]
print val_lst
#for val in val_lst:
#	print val
#print "here"
#print type(val_lst)
#print val_lst[3][0]
#print type(val_lst[3][0])

#print val_lst[3][0]["value"]
#print type(val_lst[3][0]["value"])
#print kv_tx_json_rdd.take(kv_tx_json_rdd.count())

							   
####works
#tx_it = [x for x in tx.toLocalIterator()]
#print tx_it[0][7][4:68]
#print tx_it[1][7][4:68]
###enf of work

#print lst_tx[8][0]
#print type(lst_tx[8])
#print "here"
#print str(lst_tx[8])[4:-4] #gives tx_id without the enclosing quotes

#print type(lst_tx)
