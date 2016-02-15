from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime
import happybase
import pprint
import hyperloglog, pickle, base64
#from bitstring import BitArray

import bitcoin_pb2, lzo, base64

from kafka import KafkaClient, SimpleProducer


#handle kill signal
# import signal
# def signal_handler(signal, frame):
# 	known_blocks_save()
# 	sys.exit(0)
# signal.signal(signal.SIGINT, signal_handler)



import socket
import time


def get_lock(process_name):
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print 'Lock acquired'
    except socket.error:
        print 'Process already running. Exiting..'
        sys.exit()

get_lock('bitcoin_batch_load_blocks')




import logging
logging.basicConfig()


#### Bitcoin stuff
bitcoinrpc = None
def bitcoin_init():
	global bitcoinrpc
	rpcuser = "bitcoinrpc"
	rpcpass = "AD3gBQJ3z1oaZqQ8sdoNhJeH47BFH888n3tKaPJji4B2"
	rpcip = "127.0.0.1"
	if rpcpass == "":
		bitcoinrpc = AuthServiceProxy("http://"+rpcip+":8332")
	else:
		bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpass+"@"+rpcip+":8332")


#### Hbase stuff
hbase = None
hbase_settings_table = None
blockchain_height = 0
def hbase_init():
	global hbase, hbase_settings_table
	hbase = None
	hbase_blocks_table = None
	hbase = happybase.Connection('cloud.soumet.com')
	hbase_settings_table = hbase.table('settings')



#### Kafka stuff
kafka = None
kafka_producer = None
kafka_topic = "bitcoin_blocks"
def kafka_init():
	global kafka, kafka_producer, kafka_topic
	kafka = KafkaClient("cloud.soumet.com:9092")
	kafka_producer = SimpleProducer(kafka)#,batch_send=True,batch_send_every_n=20,batch_send_every_t=60)



#### Hyperloglog stuff
blocks_log = None
def hyperloglog_init():
	global blocks_log
	try:
		print("Loading HLL from file")
		blocks_log_f = open("blocks_log.hll", "r")
		blocks_log = pickle.load(blocks_log_f)
	except (IOError, EOFError):
		print("Creating HLL")
		blocks_log_f = open("blocks_log.hll", "w")
		blocks_log = hyperloglog.HyperLogLog(0.01)
	blocks_log_f.close()

def hyperloglog_check(value):
	global blocks_log
	before = len(blocks_log)
	blocks_log.add(str(value))
	after = len(blocks_log)
	return before < after
def hyperloglog_save():
	blocks_log_f = open("blocks_log.hll", "w")
	pickle.dump(blocks_log, blocks_log_f)









known_blocks = None
known_block = None
def known_blocks_init():
	global known_blocks, known_block
	try:
		known_blocks = hbase_settings_table.row('blocks_processed')#, columns=['data'])
		known_block = int(known_blocks["data:last_block_processed"])
		print "Resuming " + str(known_block)
	except:
		print "No data from Hbase"
		sys.exit(1)
#		hbase_settings_table.put('blocks_processed', {'data:last_block_processed' : str(0)})
#		known_block = -1
	print "Last block processed: " + str(known_block)

def is_known_block(value):
	return int(value) <= int(known_block)
	#return value < known_blocks
def mark_block_done(value):
	global known_block
	known_block = value
	if int(known_block) % 100 == 0:
		known_blocks_save()
def known_blocks_save():
	#kafka_producer.stop()
	hbase_settings_table.put('blocks_processed', {'data:last_block_processed' : str(known_block)})









def log_block(block_id, block_data):
	#data = base64.b64encode(lzo.compress(str(block_data),1))
	data = base64.b64encode(str(block_data))
	kafka_producer.send_messages(kafka_topic, data)
	#fh.write(str(lzo.compress(str(block_data),1)))
	#hbase_settings_table.put('blocks_processed', {'data:' + str(block_id) : 'processed'})

def log_transaction(transaction_id, transaction_data):
	print str(transaction_data)





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

def get_block_json(block_id):
	#hbase_blocks_table.delete(str(block_id))
	try:
		#print "Getting JSON for block " + str(block_id)
		block_hash = bitcoinrpc.getblockhash(int(block_id))
		print "Block #" + str(block_id) + " - hash: " + block_hash
		block_json = bitcoinrpc.getblock(block_hash)
		if block_json:
			block_data = bitcoin_pb2.Block()
			block_data.hash = block_json["hash"]
			block_data.confirmations = block_json["confirmations"]
			block_data.size = block_json["size"]
			block_data.height = block_json["height"]
			block_data.version = block_json["version"]
			block_data.time = block_json["time"]
			block_data.nonce = block_json["nonce"]
			block_data.bits = block_json["bits"]
			block_data.difficulty = float(block_json["difficulty"])
			block_data.chainwork = block_json["chainwork"]
			if "previousblockhash" in block_json:
				block_data.previousblockhash = block_json["previousblockhash"]
			else:
				block_data.previousblockhash = ""
			block_data.nextblockhash = block_json["nextblockhash"]
			for transaction_hash in block_json["tx"]:
				transaction = block_data.tx.add()
				transaction.hash = transaction_hash
			log_block(block_id, block_data.SerializeToString())

	except Exception as e:
		print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
		raise



def main():
	global blockchain_height
	hbase_init()
	known_blocks_init()
	bitcoin_init()
	kafka_init()
	
	blockchain_height = bitcoinrpc.getblockcount()
	print "Current blockchain height: " + str(blockchain_height)
	print "Bitcoin client # of connections: " + str(bitcoinrpc.getconnectioncount())
	try:
		print "Max blockchain: " + str(blockchain_height)
		for block_id in range(0, blockchain_height):
			if not is_known_block(block_id):
				print "Loading next block #%s " % str(block_id)  # + "  #" + str(i) + "/" + str(len(difference))
				get_block_json(block_id)
				mark_block_done(block_id)
	#except KeyboardInterrupt:
	#	known_blocks_save()
	#	sys.exit() 
	except Exception as e:
		print(e)
		known_blocks_save()
		sys.exit()
	known_blocks_save()



main()
