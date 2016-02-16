from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime
import happybase
import pprint
import pickle, base64
import socket
import time
import json




#### Bitcoin stuff
bitcoinrpc = None
def bitcoin_init():
	global bitcoinrpc
	global bitcoinrpc
	rpcuser="bitcoinrpc"
	rpcpassword="5C3Y6So6sCRPgBao8KyWV2bYpTHZt5RCVAiAg5JmTnHr"
	rpcip = "127.0.0.1"
	bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpassword+"@"+rpcip+":8332")


#### Hbase stuff
hbase = None
hbase_settings_table = None
blockchain_height = 0
def hbase_init():
	global hbase, hbase_settings_table
	hbase = None
	hbase_blocks_table = None
	hbase = happybase.Connection('ec2-52-72-36-43.compute-1.amazonaws.com')
	hbase_settings_table = hbase.table('settings') #connect to settings table
	#print "Table setting created"




#### Kafka stuff
#kafka = None
#kafka_producer = None
#kafka_topic = "bitcoin_blocks"
#def kafka_init():
#	global kafka, kafka_producer, kafka_topic
#	kafka = KafkaClient("ec2-52-72-36-43.compute-1.amazonaws.com")
#	kafka_producer = SimpleProducer(kafka)#,batch_send=True,batch_send_every_n=20,batch_send_every_t=60)




known_blocks = None
known_block = None

def known_blocks_init():
	global known_blocks, known_block
	try:
		known_blocks = hbase_settings_table.row('blocks_processed')#, columns=['data'])
		known_block = int(known_blocks["data:last_block_processed"])
		#print "Resuming " + str(known_block)
	except:
		print "No data from Hbase"
		######sys.exit(1) ###commented for now
#		hbase_settings_table.put('blocks_processed', {'data:last_block_processed' : str(0)})
#		known_block = -1
	#print "Last block processed: " + str(known_block)

def is_known_block(value):
	return int(value) <= int(known_block)
	#return value < known_blocks

#def mark_block_done(value):
#	global known_block
#	known_block = value
#	if int(known_block) % 100 == 0:
#		known_blocks_save(known_block)

#def known_blocks_save(known_block):
	#kafka_producer.stop()
	#print "inside known_blocks_save"
	#print str(known_block)
	#hbase_settings_table.put('blocks_processed', {'data:last_block_processed' : str(known_block)})


def log_block(block_id, block_data):
	data = base64.b64encode(str(block_data))
	#print data
	kafka_producer.send_messages(kafka_topic, data)
	
def log_transaction(transaction_id, transaction_data):
	print str(transaction_data)




 #given transaction hash, pull up transacion information
def get_transaction(transaction_hash):
	print "fetch_transaction " + transaction_hash
	try:
		transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
		log_transaction(transaction_hash, transaction_json)
	except KeyboardInterrupt:
		raise
	except:
		print "Error: Transaction# " + transaction_hash + " - Could not get transaction data"
		#hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})

#given block id, pull up block information
def get_block_json(block_id):
	#hbase_blocks_table.delete(str(block_id))
	try:
		#print "Getting JSON for block " + str(block_id)
		block_hash = bitcoinrpc.getblockhash(int(block_id))
		#call("bitcoin-cli","getblockhash",str(block_id))
		#print block_hash
		#print "Block #" + str(block_id) + " - hash: " + block_hash
		block_json = bitcoinrpc.getblock(block_hash)
		print block_json
		#pprint.pprint(block_json)
		#gen_tx = bitcoinrpc.decoderawtransaction(bitcoinrpc.getrawtransaction("cea5beb84b701f10b93742ad7967e5e771551ce30e34434a6a7b381ee62cecc8"))
		#gen_tx = call("bitcoin-cli","gettxout,cea5beb84b701f10b93742ad7967e5e771551ce30e34434a6a7b381ee62cecc8,0")
		#print gen_tx
		#pprint.pprint(gen_tx)
		#print type(block_json)
		#if block_json:
		#	block_data = bitcoin_pb2.Block()
		#	block_data.hash = block_json["hash"]
		#	block_data.confirmations = block_json["confirmations"]
		#	block_data.height = block_json["height"]
		#	block_data.version = block_json["version"]
		#	block_data.time = block_json["time"]
		#	block_data.nonce = block_json["nonce"]
		#	block_data.bits = block_json["bits"]
		#	block_data.difficulty = float(block_json["difficulty"])
		#	block_data.chainwork = block_json["chainwork"]
		#	if "previousblockhash" in block_json:
		#		block_data.previousblockhash = block_json["previousblockhash"]
		#	else:
		#		block_data.previousblockhash = ""
		#	block_data.nextblockhash = block_json["nextblockhash"]
		#	for transaction_hash in block_json["tx"]:
		#		transaction = block_data.tx.add()
		#		transaction.hash = transaction_hash
			#log_block(block_id, block_data.SerializeToString())

			#print block_data
	except Exception as e:
		print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
		raise



def main():
	global blockchain_height
	#hbase_init()
	known_blocks_init()
	bitcoin_init()
	###kafka_init() #temporarily disabled to test the rest of the stuff
	#print "Inside main()"
	blockchain_height = bitcoinrpc.getblockcount()
	#print "Current blockchain height: " + str(blockchain_height)
	#print "Bitcoin client # of connections: " + str(bitcoinrpc.getconnectioncount())
	#try:
	#print "Max blockchain: " + str(blockchain_height)
	#for block_id in range(210000, blockchain_height+1): #388521
	for block_id in range(210000, 396844): 

	#for block_id in range(0, 1):
	#for block_id in range(0, blockchain_height+1):
		#if not is_known_block(block_id):
		#print "Loading next block #%s " % str(block_id)  # + "  #" + str(i) + "/" + str(len(difference))
		get_block_json(block_id)
		#mark_block_done(block_id)
#except KeyboardInterrupt:
	#	known_blocks_save()
	#	sys.exit() 
	
	###except Exception as e:
	###	print(e)
	###	known_blocks_save()
	###	sys.exit()
	###known_blocks_save()



main()
