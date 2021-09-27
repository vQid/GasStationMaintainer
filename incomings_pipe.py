import multiprocessing
import threading
import time
from queue import PriorityQueue
from sockets import ServerSockets
import pipe_filter
import socket
import config as cfg


class MultiCastChannel(threading.Thread):

    def __init__(self):
        super(MultiCastChannel, self).__init__()
        self.incomings_pipe = PriorityQueue()
        self.multicast_listener = ServerSockets()
        self.mcl = self.multicast_listener.multicast_socket

    def run(self):
        print("Listening to network multicasts...")
        try:
            while True:
                print("inside incomings pipe thread")
                data, addr = self.mcl.recvfrom(1024)

                if data:
                    self.incomings_pipe.put(pipe_filter.incoming_frame_filter(data.decode(), str(addr[0])), block=False)
                    print("Received broadcast message:", data.decode())
        except Exception as e:
            print(e)


class UdpSocketChannel(threading.Thread):

    def __init__(self):
        super(UdpSocketChannel, self).__init__()
        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        self.incomings_pipe = PriorityQueue()
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.udp_sock.bind(("", cfg.config["UDP_SOCKET_PORT"]))
        print("my udp socket port is " + str(cfg.config["UDP_SOCKET_PORT"]))
        print(self.MY_IP)

    def run(self):
        print("Listening to network udp_unicasts...")
        try:
            while True:
                print("inside incomings udp pipe thread")
                data, addr = self.udp_sock.recvfrom(1024)
                if data:
                    self.incomings_pipe.put(pipe_filter.incoming_frame_filter(data.decode(), str(addr[0])), block=False)
                    print("Received broadcast message:", data.decode())
        except Exception as e:
            print(e)
