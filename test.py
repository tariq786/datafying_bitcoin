from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime
import pprint
import  pickle, base64
#from bitstring import BitArray
import bitcoin_pb2,  base64



#handle kill signal
# import signal
# def signal_handler(signal, frame):
# 	known_blocks_save()
# 	sys.exit(0)
# signal.signal(signal.SIGINT, signal_handler)



import socket
import time







def get_block_json(block_id):

    #hbase_blocks_table.delete(str(block_id))

    print "Getting JSON for block " + str(block_id)
    block_hash = bitcoinrpc.getblockhash(int(block_id))
    print "Block #" + str(block_id) + " - hash: " + block_hash
    block_json = bitcoinrpc.getblock(block_hash)
    #print block_json #enable for debugging
    #Now translate to protocol buffer
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
        print "here"
        print block_data.tx.add()
        print "after"
        transaction = block_data.tx.add()
        transaction.hash = transaction_hash
			#log_block(block_id, block_data.SerializeToString())
    print block_data

#### Bitcoin stuff
#bitcoinrpc = None
#def bitcoin_init():
global bitcoinrpc
rpcuser = "bitcoinrpc"
rpcpass = "9s2YTvKEW7Wh5RRDeWe4EKo7V5v2i2VvHHAsK48VqtZi"
rpcip = "127.0.0.1"
bitcoinrpc = AuthServiceProxy("http://"+rpcuser+":"+rpcpass+"@"+rpcip+":8332")
#for i in range(0,10):
get_block_json(0)

