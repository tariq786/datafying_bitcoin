# datafying_bitcoin

The idea of this breadth first project is to calculate the evolution of transaction fee in bitcoin blockchain using BigData tools. For this bitcoin node was locally run on AWS (Amazon webservices) EC2 nodes (1 master and 3 slave configuration). This means i had the entire block chain data (over 70 GB) stored locally. Note Blockchain is fast growing and it has grown from 47 GB in December 2015 to 70 GB in February 2016. Running bitcoin node locally gives access to blockchain via Python RPC (Remote procedure call). Additionally, I query Toshi online API (https://bitcoin.toshi.io) to retrieve blockchain.

I am comparing following three modes for latency, cost and throughput.
1) Batch processing mode where whole or partial data is stored in HDFS
2) Local Streaming mode where data is live streamed via Python RPC 
3) Remote Streaming via API mode where relevant data is retrieved by HTTP get request

This is the presentation. It is worth to go through this first to get an idea about the scope of the work
http://bit.ly/slideshare_tariq

