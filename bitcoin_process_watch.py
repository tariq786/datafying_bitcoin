import sys, string, getpass, time, datetime


#handle kill signal
import signal
def signal_handler(signal, frame):
	known_blocks_save()
	sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)



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
get_lock('bitcoin_client')


from subprocess import call
call(["bitcoind"])

