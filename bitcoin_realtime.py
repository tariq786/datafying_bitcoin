from jsonrpc.authproxy import AuthServiceProxy
import sys, string, getpass, time, datetime, traceback, time
import pprint
import hyperloglog, pickle, base64
from bitstring import BitArray

import bitcoin_pb2, lzo, base64

from kafka import KafkaClient, SimpleProducer


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
get_lock('bitcoin_realtime')



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


#### Kafka stuff
kafka = None
kafka_producer = None
kafka_topic = "realtime"
def kafka_init():
    global kafka, kafka_producer, kafka_topic
    kafka = KafkaClient("cloud.soumet.com:9092")
    kafka_producer = SimpleProducer(kafka)#,batch_send=True,batch_send_every_n=1,batch_send_every_t=5)




def log_transaction(transaction_id, transaction_data):

    p2b_transaction = bitcoin_pb2.TransactionFull()
    p2b_transaction.txid = transaction_data["txid"]
    p2b_transaction.version = transaction_data["version"]
    p2b_transaction.locktime = transaction_data["locktime"]
    for vin in transaction_data["vin"]:
        record = p2b_transaction.vin.add()
        record.sequence = vin["sequence"]
        if "txid" in vin: #peer to peer transaction
            (out_address, amount) = get_vout_address(vin["txid"], vin["vout"])
            record.txid = vin["txid"]
            record.vout = vin["vout"]
            record.address = out_address
            record.amount = float(amount)
            #print "Send: %s (%s BTC)" % (out_address, amount)
        elif "coinbase" in vin: #block reward
            new_coin_value = sum([vout["value"] for vout in transaction_data["vout"]])
            record.address = "coinbase"
            record.amount = float(new_coin_value)
            #print "coinbase: %s generated %s BTC" % (vin["coinbase"], new_coin_value)
    for vout in transaction_data["vout"]:
        #print "Receive: %s (%s BTC) " % (vout["scriptPubKey"]["addresses"][0], vout["value"])
        record = p2b_transaction.vout.add()
        record.amount = float(vout["value"])
        record.n = vout["n"]
        if "addresses" in vout["scriptPubKey"] and len(vout["scriptPubKey"]["addresses"]) > 0:
            record.address = vout["scriptPubKey"]["addresses"][0]
        else:
            record.address = "non-standard"
    p2b_transaction.blockhash = "NA"
    p2b_transaction.confirmations = 0
    p2b_transaction.txtime = int(time.time())
    p2b_transaction.blocktime = int(time.time())

    ##p2b_transaction.blockhash = transaction_data["blockhash"]
    ##p2b_transaction.confirmations = transaction_data["confirmations"]
    ##p2b_transaction.txtime = transaction_data["time"]
    #p2b_transaction.blocktime = transaction_data["blocktime"]
    #print p2b_transaction
    data = base64.b64encode(p2b_transaction.SerializeToString())
    kafka_producer.send_messages(kafka_topic, data)


def get_vout_address(transaction_hash, index):
    try:
        transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
        return (transaction_json["vout"][index]["scriptPubKey"]["addresses"][0], transaction_json["vout"][index]["value"])
    except Exception, e:
        with open("undecodable.txt", "a") as text_file:
            text_file.write("Error decoding address #%s from tx: %s. %s" % (index, transaction_hash, e))
        return ("Non-decodable address", 0)




def get_transaction(transaction_hash):
    print "fetch_transaction " + transaction_hash
    try:
        transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
        return transaction_json
        #log_transaction(transaction_hash, transaction_json)
    except KeyboardInterrupt:
        raise
    except:
        print "Error: Transaction# " + transaction_hash + " - Could not get transaction data"
        #hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})





def main():
    bitcoin_init()
    kafka_init()

    print "# of connections: " + str(bitcoinrpc.getconnectioncount())
    current_blockcount = bitcoinrpc.getblockcount()
    current_mempool = bitcoinrpc.getrawmempool()
    
    try:
        while 1:
            time.sleep(1.41)
            #print "Checking for transactions"
            new_blockcount = bitcoinrpc.getblockcount()
            new_mempool = bitcoinrpc.getrawmempool()
            diff_mempool = list(set(new_mempool) - set(current_mempool))
            if (len(diff_mempool) > 0):
                print "Processing " + str(len(diff_mempool)) + " new transaction(s)"
                for transaction_hash in diff_mempool:
                    data = get_transaction(transaction_hash)
                    log_transaction(transaction_hash, data)
                current_mempool = new_mempool
            if new_blockcount > current_blockcount:
                print "New block to process: " + str(new_blockcount)
                #get_block_json(new_blockcount)
                current_blockcount = new_blockcount
    except KeyboardInterrupt:
        sys.exit()
 
   







main()

















# def get_transaction(transaction_hash):
#     ###hbase_transactions_table.delete(transaction_hash)
#     print "Processing transaction " + transaction_hash
#     try:
#         transaction_json = bitcoinrpc.getrawtransaction(transaction_hash, 1)
#         pprint.pprint(transaction_json)
#         return 0
#         #pprint.pprint(transaction_json)
#         load_dict = {'metadata:timestamp':str(time.time())}
#         for key in transaction_json:
#             if key == 'vin':
#                 for i, vin in enumerate(transaction_json[key]):
#                     #load_dict['in:vin' + str(i) + '_scriptSigasm'] = str(vin['scriptSig']['asm'])
#                     #load_dict['in:vin' + str(i) + '_scriptSighex'] = str(vin['scriptSig']['hex'])
#                     if 'coinbase' in vin:
#                         load_dict['in:vin' + str(i) + '_coinbase'] = str(vin['coinbase'])
#                         load_dict['in:vin' + str(i) + '_sequence'] = str(vin['sequence'])
#                     else:
#                         load_dict['in:vin' + str(i) + '_sequence'] = str(vin['sequence'])
#                         load_dict['in:vin' + str(i) + '_txid'] = str(vin['txid'])
#                         load_dict['in:vin' + str(i) + '_txvout'] = str(vin['vout'])
#             elif key == 'vout':
#                 for i, vout in enumerate(transaction_json[key]):
#                     load_dict['out:vout' + str(vout['n']) + '_value'] = str(vout['value'])
#                     for j, address in enumerate(vout['scriptPubKey']['addresses']):
#                         load_dict['out:vout' + str(vout['n']) + '_address' + str(j)] = str(address)
#             else:
#                 load_dict['metadata:' + key] = str(transaction_json[key])
#         ###hbase_transactions_table.put(str(transaction_hash), load_dict)
#     except KeyError as e:
#         print "Error: Transaction# " + transaction_hash + " - keyerror: " + str(e)
#         ###hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})
#     except KeyboardInterrupt:
#         raise
#         #hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})
#     #pprint.pprint(load_dict)
#     except:
#         print "Error: Transaction# " + transaction_hash + " - Could not get transaction data"
#         ###hbase_transactions_table.put(transaction_hash, {'metadata:status' : 'Error loading transaction'})

# def get_block_json(block_id):
#     ###hbase_blocks_table.delete(str(block_id))
#     try:
#         print "Saving new block: #" + str(block_id)
#         block_hash = bitcoinrpc.getblockhash(int(block_id))
#         print "Block #" + str(block_id) + " - hash=" + block_hash
#         block_json = bitcoinrpc.getblock(block_hash)
#         if block_json:
#             load_dict = {}
#             for key in block_json:
#                 if key == 'tx':
#                     for transaction_hash in block_json['tx']:
#                         load_dict['transactions:' + transaction_hash] = transaction_hash
#                         get_transaction(transaction_hash)
#                 else:
#                     load_dict['metadata:' + key] = str(block_json[key])
#             #pprint.pprint(load_dict)
#             #del load_dict['metadata:tx']
#             #####hbase_blocks_table.put(str(block_id), load_dict)
#     except Exception as e:
#         #hbase_blocks_table.put(str(block_id), {'metadata:status' : 'Error loading block'})
#         print "****************Error saving block #" + str(block_id) + " " + str(e) + " ****************************"
#         raise
