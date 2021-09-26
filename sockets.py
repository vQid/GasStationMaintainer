import socket
import config as cfg
import struct


class ServerSockets:

    def __init__(self):
        # Local host information

        # Create a UDP socket
        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        # -------------------
        # BROADCAST
        self.BROADCAST_PORT = cfg.config["BROADCAST_PORT"]
        self.UDP_PORT = cfg.config["UDP_SOCKET_PORT"]
        # Bind socket to address and port
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Set the socket to broadcast and enable reusing addresses
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.broadcast_socket.bind((self.MY_IP, self.BROADCAST_PORT))

        # -------------------
        # MULTICAST
        self.MCAST_GRP = cfg.config["MULTICAST_GROUP"]
        self.MCAST_PORT = cfg.config["MCAST_PORT"]
        self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.multicast_socket.bind(('', self.MCAST_PORT))
        self.mreq = struct.pack("4sl", socket.inet_aton(self.MCAST_GRP), socket.INADDR_ANY)
        self.multicast_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self.mreq)
