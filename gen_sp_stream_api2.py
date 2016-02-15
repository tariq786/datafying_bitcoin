import requests


#this retrieves block's json via http get request. Upper bound is set
#to 397990. You can adjust it to getblkcount() by looking at
#http://blockchain.info or using RPC call getblockcount() in bitcoind

for i in range(210000,397990):  #
	url = 'https://bitcoin.toshi.io/api/v0/blocks/'+str(i)
	r = requests.get(url)
	#tx_fee = r.json()["fees"]
	try:
		print r.json()
	except IOError:
		pass
