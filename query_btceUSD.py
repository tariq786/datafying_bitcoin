import sys, string, getpass, datetime
import happybase
import urllib, json, os

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
get_lock('bitcoin_btc_usd_query')



import logging
logging.basicConfig()



hbase = happybase.Connection('cloud.soumet.com')
hbase_settings_table = hbase.table('settings')

known_transactions = None
known_transaction = None
def known_transactions_init():
	global known_transactions, known_transaction
	try:
		known_transactions = hbase_settings_table.row('BTCUSD_processed')#, columns=['data'])
		known_transaction = int(known_transactions["data:last_transaction_processed"]) + 1
		print "Resuming"
	except:
		print "No data from Hbase"
		hbase_settings_table.put('BTCUSD_processed', {'data:last_transaction_processed' : str(0)})
		known_transaction = -1
	print "Last transaction processed: " + str(known_transaction)

def is_known_transaction(value):
	return int(value) <= int(known_transaction)
def mark_transaction_done(value):
	global known_transaction
	known_transaction = value
	if int(known_transaction) % 100 == 0:
		known_transactions_save()
def known_transactions_save():
	#kafka_producer.stop() #flush kafka
	hbase_settings_table.put('BTCUSD_processed', {'data:last_transaction_processed' : str(known_transaction)})



#### Kafka stuff
kafka = None
kafka_producer = None
kafka_topic = "bitcoin_exchange"
def kafka_init():
	global kafka, kafka_producer, kafka_topic
	kafka = KafkaClient("cloud.soumet.com:9092")
	kafka_producer = SimpleProducer(kafka) #,batch_send=True,batch_send_every_n=5,batch_send_every_t=60)




def main():
	kafka_init()
	known_transactions_init()
	global known_transaction
	print "Last known transaction: %s" % known_transaction
	try:
		transactions_count = 0
		while True:
			btc_usd_data = json.loads(urllib.urlopen("https://btc-e.com/api/3/trades/btc_usd-btc_rur-btc_eur-btc_cnh-btc_gbp?ignore_invalid=1").read())
			master_data = []
			for l in btc_usd_data:
				for elem in btc_usd_data[l]:
					elem["currency"] = l.split('_')[1].upper()
				master_data += btc_usd_data[l]
			master_data = sorted(master_data, key=lambda k: int(k['tid']))
			for transaction in master_data:
				if not is_known_transaction(transaction['tid']):
					#print transaction
					#print "%s,%s,%s,%f,%s" % (transaction['tid'],transaction["timestamp"], transaction["price"], float(transaction["amount"]), transaction["currency"])
					message = str("%s,%s,%s,%f,%s" % (transaction['tid'],transaction["timestamp"], transaction["price"], float(transaction["amount"]), transaction["currency"]))
					print message
					kafka_producer.send_messages(kafka_topic, message)
					mark_transaction_done(transaction['tid'])
					transactions_count += 1
				#known_transactions_save() #flush
			time.sleep(2)
		print "Total transactions processed: %s" % transactions_count
	except KeyboardInterrupt:
		#known_transactions_save()
		sys.exit() 
	except Exception as e:
		print(e)
		#known_transactions_save()
		sys.exit() 

main()
#known_transactions_save()

