#from __future__ import print_function
from pyspark import SparkContext
from jsonrpc.authproxy import AuthServiceProxy
from pyspark.streaming import StreamingContext
import json


#def get_output(rdd):
#    rdd_data = rdd.collect()
#    rdd_data.saveAsTextFile("stream_output.txt")


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[*]", "txcount")
ssc = StreamingContext(sc, 0.5) #0.001 did 9710 blocks in 12 minutes



lines = ssc.socketTextStream("localhost", 9999)
dump_rdd = lines.map(lambda x: json.dumps(x))
#print dump_rdd.take(2)
load_rdd = dump_rdd.map(lambda x: json.loads(x)).map(lambda x : x.decode('unicode_escape').encode('ascii','ignore'))
#load_rdd.pprint(2)

#load_rdd.pprint(100)
#tx = load_rdd.flatMap(lambda x: x.split(":")) #this works
split_blk_rdd = load_rdd.map(lambda x: x.split(":"))
#split_blk_rdd.pprint()

tx_fee_rdd = split_blk_rdd.map(lambda x : x[15][0:-15]) #this gets transaction fee
tx_fee_rdd.pprint(200)		#works
#tx_fee_rdd.saveAsTextFiles("hdfs://ec2-52-21-47-235.compute-1.amazonaws.com:9000/bitcoin/","txt")

######tx_fee_rdd.pprint(1000)

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
ssc.awaitTerminationOrTimeout(13000) #13000#time out in 3 hours
#ssc.stop()  # Wait for the computation to terminate
