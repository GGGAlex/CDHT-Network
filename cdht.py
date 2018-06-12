import socket
import pickle
from threading import Thread
import time
import sys

#---------------------------------------------------------------------------------------------------------------------#
#                                                CDHT Peer Class                                                      #
#                                                                                                                     #
#  Start new peer by entering the following command at the Terminal or xterm:                         		      #
#                                                                                                                     #
#            >>> python3 cdht.py [X] [Y] [Z]                                                                          #
#                                                                                                                     #
#            (integer)   [X] is the peer's identity                                                                   #
#            (integer)   [Y] is the peer's 1st successor                                                              #
#            (integer)   [Z] is the peer's 2nd successor                                                              #
#                                                                                                                     #
#  Peer will bind to port [X + 50000], connect to both successors and maintain a connection state until shutdown.     #
#---------------------------------------------------------------------------------------------------------------------#

class CDHTPeer:
	def __init__(self, IDENTITY, SUCCESSORS, IP_ADDRESS, PORT_NO):
		self.PORT_DISTANCE = 50000
		self.UDP_SOCKET_OPEN = 0
		self.TCP_SOCKET_OPEN = 0

		self.IP_ADDRESS = IP_ADDRESS
		self.PORT_NO = PORT_NO

		self.IDENTITY = IDENTITY
		self.SUCCESSORS = SUCCESSORS
		self.SUCCESSORS_PORTS = [self.SUCCESSORS[0] + self.PORT_DISTANCE,self.SUCCESSORS[1] + self.PORT_DISTANCE]
		self.SUCCESSOR_STATUS = [0,0]
		self.SUCCESSOR_SEQUENCE = [0,0]
		
		self.PREDECESSORS = [0,0]
		self.PREDECESSORS_PORTS = [0,0]
		self.PREDECESSORS_STATUS = [0,0]

		self.filename_range = range(0,10000)
		self.peer_file_range = [0,0]
		self.hash_range = 256
		self.file_readiness = [0,0]
		self.file_system_status = 0


	#---------------------------------------------------------------------------------#
	#                              Peer Start and Shutdown                            #
	#                                                                                 #
	# These functions are used to start and shutdown the peer's core loop routine     #
	#---------------------------------------------------------------------------------#

	def start_peer(self):
		UDP_socket = self.UDP_start_socket(self.PORT_NO)
		print('Started Peer {} on port {}'.format(self.IDENTITY,self.PORT_NO))
		
		try:
			find_maintain_successors_thread = Thread(target = self.find_successors, args = [UDP_socket])
			UDP_listen_thread = Thread(target = self.UDP_listen, args = [UDP_socket])
			TCP_listen_thread = Thread(target = self.TCP_listen, args = [] )


			find_maintain_successors_thread.start()
			UDP_listen_thread.start()
			TCP_listen_thread.start()

			while True:
				try:
					self.__updatePorts()				
					command = input("").split(' ')  
					if command[0] == "request":
						if int(command[1]) in self.filename_range and len(command[1]) == 4:
							start_request_file_thread = Thread(target = self.start_request_file, args = [command[1], UDP_socket])
							start_request_file_thread.start()
						else:
							print("Invalid syntax used: request ####")
							print("Please try again")
					elif command[0] == "quit":
						start_peer_departure_thread = Thread(target = self.shutdown_peer, args = [UDP_socket])
						start_peer_departure_thread.start()
					
				except ValueError:
					print("Command Invalid. Please try again.")
					continue

		except KeyboardInterrupt:
			print('Peer {} shut down unexpectedly'.format(IDENTITY))
			self.TCP_SOCKET_OPEN = 0
			self.UDP_close_socket(UDP_socket)
			sys.exit(0)
	

	def shutdown_peer(self,peer_socket):
		self.__updatePorts()
		self.TCP_SOCKET_OPEN = 0
		for predecessor_number in range(2):
			departure_message = pickle.dumps(["peer_shutdown", predecessor_number, self.SUCCESSORS])
			peer_socket.sendto(departure_message, (self.IP_ADDRESS, self.PREDECESSORS_PORTS[predecessor_number]))
		self.UDP_close_socket(peer_socket)

		sys.exit(0)


	#---------------------------------------------------------------------------------#
	#                       Stage 1: Find Successor Connections                       #
 	#                                                                                 #
	#          Peer uses UDP to search for the 2 succssor's upon activation.          #
	#             Peer pings both successors until a response is received             #
	#---------------------------------------------------------------------------------#

	def find_successors(self, peer_socket):
		max_ping = 5
		wait_time = 4
		while not all(self.SUCCESSOR_STATUS) and self.UDP_check_socket(peer_socket):
			ping_count = max_ping
			while not all(self.SUCCESSOR_STATUS) and ping_count > 0 and self.UDP_check_socket(peer_socket):
				ping_count -= 1

				#  Display message to user  #
				if ping_count == (max_ping - 1):
					for sucessor_number in range(2):
						if not self.SUCCESSOR_STATUS[sucessor_number]: 
							print('Pinging Peer {}'.format(self.SUCCESSORS[sucessor_number]))					

				#  Ping successor if SUCCESSOR_STATUS = 0  #
				for sucessor_number in range(2):
					if not self.SUCCESSOR_STATUS[sucessor_number]:
						ping_message = pickle.dumps(["PING 1",sucessor_number])
						peer_socket.sendto(ping_message, (self.IP_ADDRESS, self.SUCCESSORS_PORTS[sucessor_number]))

				peer_socket.settimeout(wait_time)

				#  Listen for reply  #
				try:
					recv_message, addr = peer_socket.recvfrom(1024)
					sender_IP, sender_socket, sender_identity = self.__extract_sender_info(addr)
					recv_message = pickle.loads(recv_message)
					command = recv_message[0]
					successor_number_reply = recv_message[1]
					if command == "ALIVE 1":
						if sender_identity == self.SUCCESSORS[successor_number_reply] and self.SUCCESSOR_STATUS[successor_number_reply] == 0:
							self.update_successors(successor_number_reply, sender_identity, 1)
							print('A ping response message was received from Peer {}'.format(sender_identity))
				except socket.timeout:
					continue

			if not all(self.SUCCESSOR_STATUS):
				for i in range(2):
					if self.SUCCESSOR_STATUS[i] == 0:
						print('Successor {} (Peer {}) was not found.'.format(i+1, self.SUCCESSORS[i]))
				print('Resending pings...')
				time.sleep(3)

			else: 
				print('All successors found and are active.')
				self.maintain_successors(peer_socket)
				break
	#---------------------------------------------------------------------------------#
	#                   Stage 2: Maintain Successor Connections                       #
  	#                                                                                 #
	# The peer will maintain the connection between the 2 successors once they        #
	# have been found.                                                                #
  	#                                                                                 #	
	# The peer sends numbered packets to successors to identify the last packet       #
	# received from each successor.                                                   #
  	#                                                                                 #
	# If the peer doesn't receive 5 consecutive packets (alive_threshold) then the    #
	# successor is presumed dead and the search for a new succesor begins.            #
	#---------------------------------------------------------------------------------#

	def maintain_successors(self,peer_socket):
		max_packet_sequence = 30
		alive_threshold = 5
		timeout_wait = 4

		
		while self.UDP_check_socket(peer_socket):
			time.sleep(timeout_wait)
			current_sequence = 0
			if self.UDP_check_socket(peer_socket):
				print("\nPeer {}: Pinging Peers {} and {} to maintain connection to successors...".format(self.IDENTITY, self.SUCCESSORS[0], self.SUCCESSORS[1]))

			
			self.SUCCESSOR_SEQUENCE = [0,0]
			while (self.UDP_check_socket(peer_socket) 
				and (current_sequence < max_packet_sequence)
				and ((current_sequence - self.SUCCESSOR_SEQUENCE[0]) <= alive_threshold) 
				and ((current_sequence - self.SUCCESSOR_SEQUENCE[1]) <= alive_threshold)):

				
				
				if ((current_sequence - self.SUCCESSOR_SEQUENCE[0]) > 0) or ((current_sequence - self.SUCCESSOR_SEQUENCE[1]) > 0):
					print("\nWarning: Connection Lost")
					print("Last Packet Sequence Sent: {}".format(current_sequence))
					for i in range(2):
						if (current_sequence - self.SUCCESSOR_SEQUENCE[i]) > 0:
							print("Successor {} (Peer {}) Last Packet Sequence Received: {}".format(i+1,self.SUCCESSORS[i], self.SUCCESSOR_SEQUENCE[i]))

				current_sequence += 1

				for sucessor_number in range(2):
					ping_message = pickle.dumps(["PING 2",sucessor_number, current_sequence])
					peer_socket.sendto(ping_message, (self.IP_ADDRESS, self.SUCCESSORS_PORTS[sucessor_number]))

				time.sleep(timeout_wait)

			for successor_number in range(2):
				if (current_sequence - self.SUCCESSOR_SEQUENCE[successor_number]) > alive_threshold:
					print('\nPeer {} is no longer alive.'.format(SUCCESSORS[successor_number]))
					print("Please wait file system restarts...")
					self.file_system_status = 0
					self.change_file_readiness("s",0)
					self.manage_dead_successor(successor_number)
			

	def manage_dead_successor(self, successor_number):

		if successor_number:
			time.sleep(2) #allow for successor 1 to update their list first
		else:
			self.update_successors(0, self.SUCCESSORS[1], 1)
		self.SUCCESSOR_STATUS[1] = 0

		message = ["request_successors"]

		print("Opening TCP connection with Peer {} to find new successor".format(self.SUCCESSORS[0]))
		TCP_Socket = TCPConnection(self.IDENTITY, '', self.SUCCESSORS_PORTS[0])
		TCP_Socket.send_message(message)
		successor_list_message = TCP_Socket.await_message()

		self.update_successors(1, successor_list_message[0], 1)
		self.__declare_successors()

	#----------------------------------------------------------------#
	#                       UDP Socket Control                       #
    	#                                                                #
	#    Allows peer to open, close and check (if open) UDP sockets  #
	#----------------------------------------------------------------#

	def UDP_start_socket(self, port_no):
		if self.UDP_SOCKET_OPEN == 0:
			peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			peer_socket.bind(('', port_no))
			self.UDP_SOCKET_OPEN = 1
		return peer_socket

	def UDP_close_socket(self, peer_socket):
		if self.UDP_SOCKET_OPEN == 1:
			peer_socket.close()
			self.UDP_SOCKET_OPEN = 0
		return

	def UDP_check_socket(self, peer_socket):
		if self.UDP_SOCKET_OPEN == 1:
			return 1
		else:
			return 0

	#----------------------------------------------------------------#
	#                  UDP Socket Response Management                #
    	#                                                                #
	#     Allows peer to respond to UDP messages based on commands   #
	#     from other peers in the network                            #
	#----------------------------------------------------------------#	


	def UDP_listen(self, peer_socket):
		while self.UDP_check_socket(peer_socket):
			
			try:
				self.__updatePorts()
				recv_message, addr = peer_socket.recvfrom(1024)
				sender_IP, sender_socket, sender_identity = self.__extract_sender_info(addr)
				recv_message = pickle.loads(recv_message)
				command = recv_message[0]
				if command == "PING 1":
					self.ping_reply(peer_socket, sender_identity, sender_IP, sender_socket, recv_message[1])
				elif command == "PING 2":
					self.ping_2_reply(peer_socket, sender_identity,sender_IP, sender_socket,recv_message[1],recv_message[2])
				elif command == "ALIVE 2":
					self.update_successor_sequence(sender_identity,recv_message[1],recv_message[2])
				elif command == "peer_shutdown":
					self.manage_successor_shutdown(sender_identity,recv_message[1],recv_message[2])

			except socket.timeout:
				continue
			except OSError:
				sys.exit(0)

	def ping_reply(self,peer_socket,sender_identity, sender_ip, sender_socket, pred_order):
		print('A ping request message was received from Peer {}'.format(sender_identity))
		self.update_predecessors(pred_order, sender_identity, 1)
		send_message = pickle.dumps(["ALIVE 1",pred_order])
		peer_socket.sendto(send_message, (sender_ip, sender_socket))

	def ping_2_reply(self,peer_socket,sender_identity, sender_ip, sender_socket, pred_order, packet_sequence):
		self.update_predecessors(pred_order, sender_identity, 1)
		send_message = pickle.dumps(["ALIVE 2",pred_order,packet_sequence])
		peer_socket.sendto(send_message, (sender_ip, sender_socket))

	def update_successor_sequence(self, sender_identity, successor_number_reply, current_sequence_reply):
		if sender_identity == self.SUCCESSORS[successor_number_reply] and current_sequence_reply > self.SUCCESSOR_SEQUENCE[successor_number_reply]:
			self.SUCCESSOR_SEQUENCE[successor_number_reply] = current_sequence_reply		
		
	def manage_successor_shutdown(self,DEPARTED_PEER,DEPARTED_PEER_ORDER, DEPARTED_PEER_SUCCESSORS):
		self.__updatePorts()
		if DEPARTED_PEER_ORDER:
			self.SUCCESSORS[1] = DEPARTED_PEER_SUCCESSORS[0]
		else:
			self.SUCCESSORS[0] = self.SUCCESSORS[1]
			self.SUCCESSORS[1] = DEPARTED_PEER_SUCCESSORS[1]
		
		print("\nPeer {} will depart from the network.".format(DEPARTED_PEER))
		print("Please wait file system restarts...")
		self.__declare_successors()
		self.begin_file_partition()

	#----------------------------------------------------------------#
	#                       TCP Socket Control                       #
    	#                                                                #
	# Allows peer to listen for TCP connection requests and respond  #
	#----------------------------------------------------------------#


	def TCP_listen(self, receive_from = None):
		self.TCP_SOCKET_OPEN = 1
		TCP_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		TCP_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
		TCP_socket.bind(('', self.PORT_NO))
		TCP_socket.listen(5)
		TCP_socket.settimeout(15)
		
		while self.TCP_SOCKET_OPEN:
			try:
				sock, addr = TCP_socket.accept()
				sock.settimeout(None)
				t = Thread( target = self.TCP_respond,
						      args = [ sock, addr, receive_from] )
				t.start()

			except KeyboardInterrupt:
				TCP_socket.close()
				self.TCP_SOCKET_OPEN = 0
				break
			except socket.timeout:
				continue
			if self.TCP_SOCKET_OPEN == 0:
				print("TCP socket was closed. Loop stopped")
		


	def TCP_respond(self, TCP_Socket, addr, receive_from = None):
		sender_IP, sender_socket, sender_identity = self.__extract_sender_info(addr)
		try: 
			recv_message_encoded = TCP_Socket.recv(1024)
			recv_message = pickle.loads(recv_message_encoded)
			command = recv_message[0]

			if command == "file_found":
				print('Received a response message from peer {}, which has the file {}.\n'.format(recv_message[3], recv_message[1]))
			elif command == "request_successors":
				message = ["successor_list", self.SUCCESSORS]
				new_message = pickle.dumps(message)
				TCP_Socket.send(new_message)	
			elif command == "request":
				self.manage_request_file(recv_message[1],recv_message[2])		
		except:
			raise

	#-----------------------------------------------------------------#
	#               Successor/Predecessor Management		  #
	#                                                                 #
	#     Allows peer to update and change successor/predecessors     #
	#    Peer will also check if the file range needs to be updated   #
	#-----------------------------------------------------------------#

	def update_successors(self, order, peer_id, status = None):
		self.SUCCESSORS[order] = peer_id
		self.SUCCESSORS_PORTS[order] = peer_id + self.PORT_DISTANCE

		if status != None and status in [0,1]:
			self.SUCCESSOR_STATUS[order] = status

		if order == 0:
			self.file_system_status = 0


		if all(self.SUCCESSOR_STATUS):
			self.change_file_readiness("s",1)
		else:
			self.change_file_readiness("s",0)

	def update_predecessors(self, order, peer_id, status = None):
		if self.PREDECESSORS[order] != peer_id:
			self.PREDECESSORS[order] = peer_id
			self.PREDECESSORS_PORTS[order] = peer_id + self.PORT_DISTANCE

			if status != None and status in [0,1]:
				self.PREDECESSORS_STATUS[order] = status

			if order == 0:
				self.file_system_status = 0

			if all(self.PREDECESSORS_STATUS):
				self.change_file_readiness("p",1)
			else:
				self.change_file_readiness("p",0)

	def __updatePorts(self):
		self.SUCCESSORS_PORTS = [self.SUCCESSORS[0]+self.PORT_DISTANCE,self.SUCCESSORS[1]+self.PORT_DISTANCE]
		self.PREDECESSORS_PORTS = [self.PREDECESSORS[0]+self.PORT_DISTANCE,self.PREDECESSORS[1]+self.PORT_DISTANCE]
		return


	#--------------------------------------------------------------#
	#                  File System Management	 	       #
    	#                                                              #
	# Allows peer to manage file system by:			       #
	#    Activate/deactivate file system		               #
   	#    Partition file ranges among the closest peers in network  #
	#    Update file range based on changes to peers in network    #
	#    Request files from other peers in the network             #
	#    Respond to file request and searches from peers           #
	#--------------------------------------------------------------#


	def change_file_readiness(self,type,status):
		if type == "p":
			self.file_readiness[0] = status
		elif type == "s":
			self.file_readiness[1] = status

		if all(self.file_readiness) and self.file_system_status == 0:
			self.begin_file_partition()

	def begin_file_partition(self): #modulo?
		lower_distance = ((self.IDENTITY - self.PREDECESSORS[0]) % self.hash_range) // 2
		lower_bound = (self.IDENTITY - lower_distance) % self.hash_range
		upper_distance = ((self.SUCCESSORS[0] - self.IDENTITY) % self.hash_range) //2
		upper_bound = (self.IDENTITY + upper_distance) % self.hash_range
		self.peer_file_range = [lower_bound,upper_bound]
		self.file_system_status = 1
		print("\nPeer {}: File Systems Online.".format(self.IDENTITY))
		print("Peer {} holds filenames with the following hash range: {}\n".format(self.IDENTITY,self.peer_file_range))


	def file_search(self, filename):
	    filename_hash = self.__hashfn(filename)

	    if not all(self.peer_file_range):
	    	print("\nFile Systems Offline. Network setup not complete.")
	    	return 0

	    lower = self.peer_file_range[0]
	    upper = self.peer_file_range[1]
	   
	    if lower == filename_hash or upper == filename_hash:
	        return 1
	    if lower < upper:
	        if lower < filename_hash and filename_hash < upper:
	            return 1
	    elif upper < lower:
	    	if filename_hash >= 0 and filename_hash <= upper:
	    		return 1
	    	elif filename_hash <= self.hash_range and filename_hash >= lower:
	    		return 1
	    	else:
	    		return 0
	    return 0

	def __hashfn(self, filename):
		filename_hash = int(filename) % self.hash_range
		return filename_hash

	def start_request_file(self, filename, UDP_Socket):
		self.__updatePorts()
		file_request = ["request", filename, self.IDENTITY]
		TCP_socket = TCPConnection(self.IDENTITY, self.IP_ADDRESS, self.SUCCESSORS_PORTS[0])
		TCP_socket.send_message(file_request)
		print("\nFile request message for {} has been sent to my successor.".format(filename))
		TCP_socket.close()

	def manage_request_file(self, filename, request_identity):
		self.__updatePorts()
		FILE_FOUND = self.file_search(filename)

		if FILE_FOUND:
			file_found_message = ["file_found", filename, request_identity, self.IDENTITY]
			request_identity_port = request_identity + self.PORT_DISTANCE

			TCP_socket = TCPConnection(self.IDENTITY, self.IP_ADDRESS, request_identity_port)
			TCP_socket.send_message(file_found_message)
			TCP_socket.close()
	
			print("\nFile {} is here.".format(filename))
			print("A response message, destined for peer {}, has been sent.".format(request_identity))

		else:
			file_request = ["request", filename, request_identity]

			TCP_socket = TCPConnection(self.IDENTITY, self.IP_ADDRESS, self.SUCCESSORS_PORTS[0])
			TCP_socket.send_message(file_request)
			TCP_socket.close()

			print("\nFile {} is not stored here.".format(filename))
			print("File request message has been forwarded to my successor.".format(filename))



	#----------------------------------------------------------------#
	#                   Other Helper Functions                       #
	#----------------------------------------------------------------#

	def __declare_successors(self):
		words = ['first','second']
		print('')
		for successor_number in range(2):
			print("My {} successor is now peer {}".format(words[successor_number], self.SUCCESSORS[successor_number]))

	def __extract_sender_info(self, addr):
		sender_ip = addr[0]
		sender_socket = addr[1]
		sender_identity = int(sender_socket) - self.PORT_DISTANCE
		return sender_ip, sender_socket, sender_identity


#----------------------------------------------------------------#
#                TCP Socket Connection Class                     #
#                                                                #
#         Allows peer to connect to other peers in network       #
#----------------------------------------------------------------#

class TCPConnection:
	def __init__(self, peer_identity, ip_address, port_no, sock=None):
		self.peer_identity = peer_identity
		self.ip_address = ip_address
		self.port_no = port_no

		if sock is None:
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.sock.connect((ip_address,port_no))
		else:
			self.sock = sock

	def close(self):
		self.sock.close()
		self.sock = None

	def send_message(self, message):
		new_message = pickle.dumps(message)
		self.sock.send(new_message)	

	def await_message(self):

		new_message = self.sock.recv(1024)
		recv_message = pickle.loads(new_message)
		command = recv_message[0]

		if command == "successor_list":
			recv_successor_list = recv_message[1]
			return recv_successor_list
		


IDENTITY = int(sys.argv[1])
SUCCESSORS = [int(sys.argv[2]),int(sys.argv[3])]
IP_ADDRESS = "127.0.0.1"
PORT_NO = 50000 + IDENTITY

new_peer = CDHTPeer(IDENTITY, SUCCESSORS, IP_ADDRESS, PORT_NO)
new_peer.start_peer()
