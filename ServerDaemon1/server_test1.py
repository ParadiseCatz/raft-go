#!/usr/bin/python

#import library 

import socket

UDP_IP_ADDR = "127.0.0.1"
UDP_PORT = 12121

# Menggunakan AF_INET dan UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind((UDP_IP_ADDR, UDP_PORT))
    
while True:
	data, addr = sock.recvfrom(10240) # buffer size is 1024 bytes
	print "received message:", data