from pyspark import SparkContext
from jsonrpc.authproxy import AuthServiceProxy
from pyspark.streaming import StreamingContext
import json



# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "txcount")
ssc = StreamingContext(sc, 0.001)

rpcuser="bitcoinrpc"
rpcpassword="5C3Y6So6sCRPgBao8KyWV2bYpTHZt5RCVAiAg5JmTnHr"
rpcip = "127.0.0.1"
bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpassword+"@"+rpcip+":8332")


def get_tx_fee(gen_tx):
	#print "inside get_tx_fee function"
	#print gen_tx
	#gen_tx_json = bitcoinrpc.gettxout(str(gen_tx),1)
	gen_tx_json = bitcoinrpc.decoderawtransaction(bitcoinrpc.getrawtransaction(gen_tx))
	#print gen_tx_json
	return gen_tx_json





lines = ssc.socketTextStream("localhost", 9999)
dump_rdd = lines.map(lambda x: json.dumps(x))
#print dump_rdd.take(2)
load_rdd = dump_rdd.map(lambda x: json.loads(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#print load_rdd.take(2)

#load_rdd.pprint(100)
#tx = load_rdd.flatMap(lambda x: x.split(":")) #this works
split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#split_blk_rdd.pprint()

gen_tx_rdd = split_blk_rdd.map(lambda x : x[6][4:68]) #this gets generation transactions
#gen_tx_rdd.pprint()		#works

tx_json_rdd = gen_tx_rdd.map(get_tx_fee)	#function call			  
tx_fee_rdd = tx_json_rdd.map(lambda x : x.items()[3][1][0]["value"]-25)#.filter(lambda x : "value" in x)
tx_fee_rdd.pprint(1000)

#gen_tx = tx.map(lambda x: x[8])

#gen_tx.pprint(100)
#gen_tx = tx.map(parse_gen_tx)
#print type(tx)
#lst_tx = pprint.pprint(tx)
#print lst_tx
#print lst_tx[8][0]
#print type(lst_tx[8])
#print "here"
#print str(tx[8])[4:-4] #gives tx_id without the enclosing quotes

#print type(lst_tx)

ssc.start()             # Start the computation
#ssc.awaitTermination()  # Wait for the computation to terminate
ssc.awaitTerminationOrTimeout(10800) #time out in 3 hours
#ssc.stop()  # Wait for the computation to terminate
