import requests


for i in range(210000,397990):  #
	url = 'https://bitcoin.toshi.io/api/v0/blocks/'+str(i)
	r = requests.get(url)
	#tx_fee = r.json()["fees"]
	try:
		print r.json()
	except IOError:
		pass
