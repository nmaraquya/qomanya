#!/usr/bin/python3

import sys			# This module provides access to some variables used or maintained by the interpreter and to functions that interact strongly with the interpreter.
import time 		# This module provides various time-related functions. 
import socket 		# This module provides access to the BSD socket interface.
import struct 		# This module performs conversions between Python values and C structs represented as Python strings. 
import random 		# This module contains a number of random number generators.
import signal 		# This module provides mechanisms to use signal handlers in Python.
import logging 		# This module defines functions and classes which implement a flexible event logging system for applications and libraries.
import argparse 	# That module makes it easy to write user-friendly command-line interfaces.

	
# parser=createParser() function makes (parser - object ) useful arguments in user-friendly way.

def createParser():
	parser = argparse.ArgumentParser()
	parser.add_argument ('-s','--server', action="store_true",help="Make a server. As a default you are just a client.")
	parser.add_argument ('-d','--destination', nargs='?', metavar='IPADRESS:PORT', default='127.0.0.1:9090', help="define the server ip address, default ip is 127.0.0.1:9090.")	
	parser.add_argument ('-t','--timeout', nargs='?', metavar='TIMEOUT',type=int, default=5,help="define the server waiting timeout, default is 15 sec.")
	parser.add_argument ('-g','--group', nargs='?',metavar='PACKETGROUP',type=int, default=3000,help="Number of packets that sending in one group. Default is 3000.")
	parser.add_argument ('-b','--buffsize', nargs='?',metavar='BUFFSIZE',type=int, default=1468,help="define the buffsize, default is 1468.")
	parser.add_argument ('-l','--logfile',nargs='?', metavar='LOGFILE',type=str, default=None,help="logging to the LOGNAME file. Otherwise, by default, there is no logging.")
	return parser

# logger = get_logger(name,logfile): that function makes a logger object, to write logs
def get_logger(name,logfile):
	formatter = logging.Formatter('[%(asctime)s,%(msecs)d] %(name)s %(levelname)s  >> %(message)s',datefmt='%H:%M:%S')
	logger = logging.getLogger(name) 		# change name of logger
	fh = logging.FileHandler(logfile,'w') 	# create log file
	fh.setFormatter(formatter) 				# make a format for line in logs
	logger.addHandler(fh) 					# add a file to logger
	logger.setLevel(logging.DEBUG) 			# change log level to debug
	logger.propagate = False 				# do not write to stderr
	return logger

# signal_handler(signal,frame): that function allow us to use CTRL+C as a good way to exit from the programm
def signal_handler(signal, frame):       
	exit(sock, ' Ctrl+C DETECTED. ')

# mean=meanval(m): that function calculates the sample mean value of list of numbers in m
def meanval(m): 
	return sum(m)/len(m)

# std=stdval(m): that function calculates the sample standart deviation of list of numbers in m
def stdval(m): 
	return (sum(i*i-meanval(m)*meanval(m) for i in m)/len(m))**(1/2)

# exit(sock, *args): close the socket and exit the programm with some messages in args*
def exit(sock, what='</ Seporator />',*args):
	for i in args: what=what+str(i)
	stderrwrite(what)
	try:
		sock.close()
		stderrwrite('	Socket was closed.\n\n')
	except:
		stderrwrite(' Unable to close a socket. My bad.\n\n')
	sys.exit('	... Goodbye.\n') 

# stderrlogwrite(*args): function that writes a messages from *args to std.err and log
def stderrlogwrite(what='</ Seporator />',*args):
	for i in args: what=what+str(i) # making an one big string from almost any kind of arguments
	sys.stderr.write(what)
	logging.info(what)

# stderrwrite(*args): function that writes a messages from *args to std.err
def stderrwrite(what='</ Seporator />',*args):
	for i in args: what=what+str(i) # making an one big string from almost any kind of arguments
	sys.stderr.write(what)

# logwrite(*args): function that writes a messages from *args to log
def logwrite(what='</ Seporator />',*args):
	for i in args: what=what+str(i) # making an one big string from almost any kind of arguments
	logging.info(what)

# sock=makesocket(proto, destination): making a UDP socket on some destination
def makesocket(proto, destination):
	try:
		logwrite(' Creating a socket on ',destination,'...')
		sock = socket.socket(socket.AF_INET,proto) # make a socket
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
		if namespace.server==True:		
			sock.bind((destination[0],int(destination[1]))) 
		logwrite('Done.')
	except Exception as err:
		stderrlogwrite(' Unable to create a socket.')
		exit(sock, err.strerror)
	return(sock)

# nPACKlist,wlist,gnACK = server_servant(PACKlist, wdiscr)
# Big function for server, by which server can generate new gnACKs
# 	make lists of packets that have to be written
#	and filtered main list of packets that contains all recieved messages.
def server_servant(PACKlist, wdiscr):
	#variables:
	#PACKlist - main packets list that is saved on the server
	#wdiscr - write descriptor - sequence of last written peace of file
	#wseqs - list of packet sequences that have to be written
	#gnACK - list that contains sequences of all missed packets and write descriptor
	#wlist - list of packets that is ready to be written
	#seqslist - list of sequences of packets from PACKlist
	#nPACKlist - filtered PACKlist
	wseqs,gnACK,wlist,missed,seqslist,nPACKlist=[],[],[],[],[],[]
	memory=0
	for PACK in sorted(PACKlist): #filtering PACKLIST and make packet sequences lists in packlist
		if memory==PACK[0]: continue
		if PACK[0]>wdiscr:
			nPACKlist.append(PACK)
			seqslist.append(PACK[0])
		memory=PACK[0]
	if seqslist: missed=sorted(set(range(wdiscr+1, max(seqslist)+1)).difference(seqslist)) # search for lost packets
	if seqslist:  # make a list of packet sequences that have to be written
		if wdiscr+1==min(seqslist):
			if missed:
				for i in range(wdiscr+1,min(missed)): wseqs.append(i)
			else: 
				wseqs=seqslist
	ndiscr=max(wseqs) if wseqs else wdiscr # make new write descriptor
	if len(missed)<368: # make gnACK or gnACKs if number of lost packets is too huge
		gnACK=[[ndiscr,*missed]]
	else:
		gm=[ndiscr]
		for i in missed:
			if len(gm)<368:
				gm.append(i)
			else:
				gnACK.append(gm)
				gm=[ndiscr]
				gm.append(i)
		gnACK.append(gm)
	for term in wseqs: # make a list of packets that must be written and also filtering main list of recieved packets
		for PACK in nPACKlist:
			if term==PACK[0]:
				wlist.append(PACK)
				nPACKlist.remove(PACK)
				break
	return nPACKlist,wlist,gnACK

"""+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

# an UDP client, 4 - mean number of our attempts to create a fast and reliable programm

# Client's tasks:
# - read from std in;
# - send peaces of file in packets to server
# - wait for gnACK's from the server
# - resend missed packets during the transmission
# - logs writing.

def UDP4client(sock,destination):
	PACKbucket_p=list() 		# Main list of packets. Read peaces of file from stdin and put them into it (filtering by gnACKs)
	RESENDbucket_p=list()		# List of packets for resending for each recieved gnACK
	gnACK=list() 				# A list for gnACK
	RTTl=list() 				# list for Round Trip Time statistics
	RTT=namespace.timeout 		# A variable for actual RTT
	TS=[0,0,0]					# variable for timing
	INPUT='something'		 	# variable for reading stdin
	seq=1 						# variable for sequence of packets counting 
	wrdiscr=0 					# variable for server write descriptor, gaining from gnACKs
	barLength=10  				# for progress bar - length of the bar
	status = "" 				# for progress bar - status 
	block=0 					# for progress bar - variable for counting sycles
	safety=3000 				# variable for overload detecting

	# while the PACKbucket_p and INPUT are not empty, do:
	while PACKbucket_p or INPUT: # CONDITION for CYCLE CLOSING

		TS[0]=time.time() # remember the time of begin of the transmission

		logwrite("LEN OF PACKbucket_p IS: ",len(PACKbucket_p))
		if TS[2]==0: TS[2]=time.time()
#		if PACKbucket_p: logwrite("PACKbucket_p contain min:  ",min([i[0] for i in PACKbucket_p]))
		# if there is something to read from stdin
		# if there is not too much packets (< 3000)
		# and if writedescriptor of the server not stack in the beginnig, do:
		if INPUT and len(PACKbucket_p)+len(RESENDbucket_p)<safety and abs(seq-wrdiscr)<safety :
			logwrite('Read and send from STDIN from ', seq,' to ',seq+namespace.group-1)
			for i in range(0,namespace.group): # read a serial of peaces of stdin
				INPUT = sys.stdin.buffer.read(namespace.buffsize)
				if not INPUT: # detecting an end of the stdin
					INPUT=''
					logwrite('END OF INPUT DETECTED ON ', 0 if seq==0 else seq-1)
					break
				LIL=len(INPUT) # for remember length of the last packet
				PACKbucket_p.append([seq,INPUT]) # add new peace of file to PACKbucket_p
				formatofstruct='!I'+str(len(INPUT))+'s' #make a format for packet structure
				try: # try to pack the peace of std in
					spacket=struct.pack(formatofstruct,seq,INPUT)
				except Exception as err:
					stderrlogwrite("Can not make a packet ",err.strerror)
				try: # try to send packet to server
					sock.sendto(spacket,(destination[0],destination[1]))
				except Exception as err:
					stderrlogwrite("Can not send a packet ",err.strerror)
				seq=seq+1 # sequence of packet counter

		logwrite('RESENDbucket_p length: ', len(RESENDbucket_p))

		if RESENDbucket_p: # if there something to resend, do:
			for PACK in sorted(RESENDbucket_p): 
				formatofstruct='!I'+str(len(PACK[1]))+'s'	
				try: # try to pack packet for resend
					spacket=struct.pack(formatofstruct,PACK[0],PACK[1])
				except Exception as err:
					stderrlogwrite("Can not make a packet ",err.strerror)
				try: # try to send it
					sock.sendto(spacket,(destination[0],destination[1]))
				except Exception as err:
					stderrlogwrite("Can not send a packet ",err.strerror)
		RESENDbucket_p.clear()

	#	logwrite('WAIT FOR gnACK : ',RTT,' SECONDS.')

		sock.settimeout(None)
		sock.settimeout(RTT) # Set the timeout to the socket

		while True: # recieving the gnACKs from the server
			try:
				[rpacket, addr]=sock.recvfrom(namespace.buffsize+4)
				if rpacket:
					formatofstruct='!'+str(int(len(rpacket)/4))+'I'
					try: # try to unpack the gnACK
						r_unpacket=list(struct.unpack(formatofstruct,rpacket))
					except Exception as err:
						stderrlogwrite("Can not unpack ",err.strerror)					
					gnACK.append(r_unpacket)
					sock.settimeout(0.01) # set a small timeout to recieve next gnACK
			except socket.timeout:
				break

		if PACKbucket_p and not INPUT and not gnACK:
			for PACK in sorted(PACKbucket_p): 
				formatofstruct='!I'+str(len(PACK[1]))+'s'	
				try: # try to pack packet for resend
					spacket=struct.pack(formatofstruct,PACK[0],PACK[1])
				except Exception as err:
					stderrlogwrite("Can not make a packet ",err.strerror)
				try: # try to send it
					sock.sendto(spacket,(destination[0],destination[1]))
				except Exception as err:
					stderrlogwrite("Can not send a packet ",err.strerror)

		if gnACK: # if there is gnACK - work with it
			PACKbucket_tmp=[] # make clear list for filtered packbucket
			wrdiscr=gnACK[0][0] # remember write descriptor from the server
			gnacklist=sum([i[1:] for i in gnACK], []) # make one huge gnack from all gnacks
			logwrite(" gnACK DETECTED:[lines,wdiscr,length]: [",len(gnACK),' , ',gnACK[0][0],' , ',len(gnacklist),']')
			mark = True if gnacklist or INPUT else False
			for PACK in PACKbucket_p:
				if mark==True:
					if PACK[0]>wrdiscr: # filter by write descriptor
						PACKbucket_tmp.append(PACK)
					if PACK[0] in gnacklist: # make list for resend by gnacklist
						RESENDbucket_p.append(PACK)
						gnacklist.remove(PACK[0])
				else:
					if PACK[0]>wrdiscr: # filter by write descriptor
						PACKbucket_tmp.append(PACK)
						RESENDbucket_p.append(PACK)
			PACKbucket_p=list(PACKbucket_tmp) # refresh packbucket
			gnACK.clear() # clear gnack list

		TS[1]=time.time()
		
		# round trip time calculating
		if RTT==namespace.timeout: # first changing RTT by timer
			RTT=TS[1]-TS[0]
			RTTl.append(RTT)
		else: # next - calculate it from statistics
			RTTl.append(TS[1]-TS[0]) 
			RTT=meanval(RTTl)+2*stdval(RTTl)
		
		logwrite('RTT NOW IS ',RTT)
		sock.settimeout(None)

		#PROGRESS BAR: jsut write progress bar, progress of sending in Kb and show the timer
		totalsize=(((wrdiscr-1)*namespace.buffsize)+LIL)/(1000)
		timer=time.time()-TS[2]		
		block=(block+1)%barLength
		if not PACKbucket_p and not INPUT: 
			status="Done.     "
			block=barLength
		text = "\r 		SENDING: [{0}] {1:,.2f} KiB. Timer: {2} sec ... {3}".format( "-"*block + "*"*(barLength-block), round(totalsize,5),round(timer,3), status)

		sys.stderr.write(text) # write
		sys.stderr.flush() # flush


	# CONDITION TO CLOSE CYCLE WAS ACHIEVED, say goodbye from the client programm
	stderrlogwrite(
				'\n',
				'\n	AVERAGE RTT IS: ',round(meanval(RTTl),3)*1000,' miliseconds.',
				'\n	TOTAL NUMBER OF PACKETS IS: ',wrdiscr,
				'\n	SPEED WAS: ',round((totalsize/timer)/1000,3),' MB per second.\n')
	exit(sock,'	PACKETLIST IS EMPTY, STDIN IS EMPTY, file was transmitted fully.\n')

"""+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

# Server's tasks:
# - receiving packets from client
# - sending a gnACKs to client
# - writing down data to stdout

def UDP4server(sock):
	PACKbucket_p=list() 		# Main list of packets. All recieved packets are there. 
	WRITEbucket=list()			# List of packets that ready for write to file
	gnACK=list() 				# List for generating gnACKs
	RTTl=list() 				# List for RTT statistic
	RTT=namespace.timeout 		# first RTT from argument
	TS=[0,0,0]					# timestamps
	EOF=0 						# END of file flag
	wrdiscr=0 					# write descriptor
	addr=[]	 					# wariable for client adress
	firsttry=True 				# first and last gnacks sending more than once 
	barLength=10 				# for progress bar, length of it
	status = "" 				# for progress bar, status
	block=barLength 			# for progress bar

	while PACKbucket_p or EOF==0: # CONDITION TO CLOSE the CYCLE

		sock.settimeout(RTT) # set a timeout to the socket

		logwrite('Wait for data : ',RTT,' seconds.')

		while True: # wait a packets from the client
			try: # try to receive a packet from socket
				[rpacket, addr]=sock.recvfrom(namespace.buffsize+4) 
				if rpacket: # if packet is received - unpacking it
					formatofstruct='!I'+str(len(rpacket)-4)+'s'
					try: # try to unpack the packets
						PACK=struct.unpack(formatofstruct,rpacket)
					except Exception as err:
						stderrlogwrite("Can not unpack ",err.strerror)	
					if len(rpacket)-4<namespace.buffsize: # detect the end of file
						logwrite(' EOF DETECTED : ',PACK[0],' Size of PACK is : ',len(rpacket))
						EOF=len(PACK[1])
					if PACK[0]>wrdiscr: PACKbucket_p.append(PACK)
					sock.settimeout(0.01) # make a small timeout to recieve next packet
			except socket.timeout: # if timeout had been detected
				if not addr: # if there are no any packet from a client - exit the programm
					exit(sock,'ERROR WHILE WAITING A CLIENT.')
				logwrite(" TIMOUT ON PACKET ",PACK[0])
				PACK=[0,0]
				break

		if TS[2]==0: TS[2]=time.time()
		if TS[0]!=0: TS[1]=time.time()

		logwrite("LEN OF PACKbucket_p IS: ",len(PACKbucket_p))
		sock.settimeout(None)

		if PACKbucket_p: # if there are any packet in PACKbucket_p - than work with it by server_servant
			PACKbucket_p,WRITEbucket,gnACK=server_servant(PACKbucket_p, wrdiscr)
			logwrite("GENERATING gnACK.")
		else:
			logwrite("THERE IS NO ANY PACKETS IN PACKbucket_p, sending wrdiscr gnack.")
			try: # try to pack it
				gnACKsp=struct.pack('!1I',wrdiscr)
			except:
				stderrlogwrite("Can not make a gnACK pack ")	
			try: # try to send it
				sock.sendto(gnACKsp,addr)
			except:
				stderrlogwrite("Can not send a packet ")			
			gnACK.clear()

		if WRITEbucket: # if there are any packet to write - write it!
#			log_WRITEbucket=[item[0] for item in WRITEbucket]
#			logwrite("WRITING PACKETS form ",min(log_WRITEbucket),' to ',max(log_WRITEbucket))
			logwrite("Write ",len(WRITEbucket), " packets")
			for PACKw in sorted(WRITEbucket):
				sys.stdout.buffer.write(PACKw[1])
				wrdiscr=PACKw[0]
			WRITEbucket.clear()
		
		if TS[0]!=0: # calculating the RTT 
			if RTT==namespace.timeout:
				RTT=(TS[1]-TS[0])
				RTTl.append(RTT)
			elif len(RTTl)>100:
				RTTl=RTTl[50:]
				RTTl.append(TS[1]-TS[0])
				RTT=meanval(RTTl)+2*stdval(RTTl)
			else:
				RTTl.append(TS[1]-TS[0])
				RTT=meanval(RTTl)+2*stdval(RTTl)
			logwrite('RTT NOW IS ',RTT)

		TS[0]=time.time()

		if gnACK: # if there are any gnACK - send it to client
			for sample in gnACK:
				formatofstruct='!'+str(len(sample))+'I'
				try: # try to pack it
					gnACKsp=struct.pack(formatofstruct,*sample)
				except Exception as err:
					stderrlogwrite("Can not make a gnACK pack ",err.strerror)	
				try: # try to send it
					sock.sendto(gnACKsp,addr)
				except Exception as err:
					stderrlogwrite("Can not send a packet ",err.strerror)
			if firsttry:
				firsttry=False
				for i in range(0,2):
					try: # try to pack itmd5sum file1 file2
						gnACKsp=struct.pack('!1I',wrdiscr)
					except:
						stderrlogwrite("Can not make a gnACK pack ")	
					try: # try to send it
						sock.sendto(gnACKsp,addr)
					except:
						stderrlogwrite("Can not send a packet ")					

		#PROGRESS BAR:
		block=(block-1)%barLength
		if not PACKbucket_p and EOF!=0: 
			status="Done.    "
			block=barLength
		totalsize=(((wrdiscr-1)*namespace.buffsize)+EOF)/(1000)
		timer=time.time()-TS[2]
		text = "\r 		RECEIVING: [{0}] {1:,.2f} KiB. Timer: {2} sec ... {3}".format( "*"*block + "-"*(barLength-block), round(totalsize,3),round(timer,3), status)
		sys.stderr.write(text)
		sys.stderr.flush()

	# Final step of server work - send multiple gnACKs to be shure that client will be know
	# about that the server complete writing.
	if gnACK: 
		for i in range(0,3):
			for sample in gnACK:
				formatofstruct='!'+str(len(sample))+'I'
				try: 
					gnACKsp=struct.pack(formatofstruct,*sample)
				except Exception as err:
					stderrlogwrite("Can not make a gnACK pack ",err.strerror)
				try: 
					sock.sendto(gnACKsp,addr)
				except Exception as err:
					stderrlogwrite("Can not send a packet ", err.strerror)

	if not RTTl: RTTl.append(0.001)
	# Say goodbye from the server programm.
	stderrlogwrite(
				'\n',
				'\n	AVERAGE RTT IS: ',round(meanval(RTTl),3)*1000,' miliseconds.',
				'\n	TOTAL NUMBER OF PACKETS IS: ',wrdiscr,
				'\n	SPEED WAS: ',round((totalsize/timer)/1000,3),' MB per second.\n')	
	exit(sock,'	PACKETLIST IS EMPTY, EOF IS GAINED, file was recieved fully.\n')

"""+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"""

if __name__ == "__main__": # that means execute code below lonly if it was asked by a __main__ 
	parser = createParser() # make a parser object
	namespace = parser.parse_args() # create namespace for parsing arguments
	signal.signal(signal.SIGINT, signal_handler) # make a CTRL+C handling
	stderrlogwrite('\n	Hello!...\n')
	dest=namespace.destination.split(':') # splitting (formatting) the distanation argument
	dest=(dest[0],int(dest[1]))
	if namespace.server:
		namespace.group='UNKNOWN'
		logname='SERVER'
	else: logname='CLIENT'
	
	if namespace.logfile: logging=get_logger(logname,namespace.logfile) # make a logger
	
	# "Hello" from the programm
	stderrlogwrite('\n 	Programm started at ',time.ctime(),' with params:\n'
				'	Server mode is: ',namespace.server,'\n'
				'	Destination is: ',dest,'\n'
				'	Number packets in group is: ',namespace.group,'\n'
				'	Buffer size is: ',namespace.buffsize,' bytes\n'
				'	Timeout RTT[0] is: ',namespace.timeout,' seconds\n'
				'	Making a log to : ',namespace.logfile,'\n\n')
	sock=makesocket(socket.SOCK_DGRAM,dest) # make a socket
	if namespace.server==True: # run a server or a client by -s argument
		UDP4server(sock)
	else:
		UDP4client(sock,dest)
	exit(sock,'\n EXIT: Last line of code: ') # exit from programm if you are somehow there.