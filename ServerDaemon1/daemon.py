#!/usr/bin/python

#import library 
import socket, os, psutil, sys, json, time

#cek argumen program harus 3 
def checkArgs():
	if len(sys.argv) != 3:
		print "Masukan IP dan PORT server pada argumen program!"
		sys.exit(2)

def readFileExternal():
	#set variable untuk UDP IP, Port dari argument
	global UDP_IP_ADDR
	global UDP_PORT
	UDP_IP_ADDR = str(sys.argv[1])
	UDP_PORT = int(sys.argv[2])
	print "IP Address Daemon: %s Daemon dengan Port: %d" %(UDP_IP_ADDR, UDP_PORT)	
	#membaca file external loadbalancer.txt untuk mengambil ip dan port
	file = open("loadbalancer.txt", "r")
	#mengisi array ip dan port
	temp = file.read().split('\n')
	global ip_addrs
	global ports
	ip_addrs = []
	ports = []
	for i in temp:
		ip = str(i.split(':')[0])
		#print "ip address node:", ip
		ip_addrs.append(ip)
		port = int(i.split(':')[1])
		#print "port node:", port
		ports.append(port)
	file.close()

def readCPUUsage():
	#mengambil cpu usage (blocking dengan interval 1)
	cpuUsage = psutil.cpu_percent(interval=1)
	#mengubah float menjadi string 
	print "CPU Usage: %f %%" % cpuUsage
	return cpuUsage

def sendCPUUsage():
	while True:
		cpuUsage = readCPUUsage()
		for i in range (len(ip_addrs)):
			try:
				# Menggunakan AF_INET dan UDP
				sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
				print "Socket berhasil dibuat"
				sock.connect((ip_addrs[i], ports[i]))
				print "Berhasil melakukan koneksi ke IP %s dengan port %d" %(ip_addrs[i], ports[i])
				data = {}
				data['IPDAEMON'] = UDP_IP_ADDR
				data['PORTDAEMON'] = UDP_PORT
				data['CPUUSAGE'] = cpuUsage
				data['TYPE'] = 'DAEMON'
				message_json = json.dumps(data)
				sock.sendall(message_json)
				print "Berhasil Mengirim JSON"
				sock.close()
			except:
				print "Gagal Mengirim"
				sock.close()			
		time.sleep(9)

#main program
checkArgs()
readFileExternal()
sendCPUUsage()
