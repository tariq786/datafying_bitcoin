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
	rpcuser="bitcoinrpc"
	rpcpassword="5C3Y6So6sCRPgBao8KyWV2bYpTHZt5RCVAiAg5JmTnHr"
	rpcip = "127.0.0.1"
	bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpassword+"@"+rpcip+":8332")




 #given transaction hash, pull up transacion information
#def get_transaction(transaction_hash):
#	print "fetch_transaction " + transaction_hash
#	try:
#		transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
#		log_transaction(transaction_hash, transaction_json)
#	except KeyboardInterrupt:
#		raise
#	except:
#		print "Error: Transaction# " + transaction_hash + " - Could not get transaction data"
#		#hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})

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
		#print block_json
		gen_tx = block_json['tx'][0]
		print gen_tx
		print type(gen_tx)
		try:
			get_tx_fee = bitcoinrpc.decoderawtransaction(bitcoinrpc.getrawtransaction(gen_tx,1))
			print get_tx_fee
		except JSONRPCException, e:
  			print repr(e.error)
			#print block_data
	except Exception as e:
		print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
		raise



def main():
	global blockchain_height
	known_blocks_init()
	bitcoin_init()
	blockchain_height = bitcoinrpc.getblockcount()
	for block_id in range(211000, 211011): #388521
		get_block_json(block_id)

main()
