import multiprocessing
import threading
import time
from queue import PriorityQueue
from sockets import ServerSockets
import pipe_filter
import socket
import config as cfg


class MultiCastChannel(threading.Thread):

    def __init__(self, process_id):
        super(MultiCastChannel, self).__init__()
        self.incomings_pipe = PriorityQueue()
        self.multicast_listener = ServerSockets()
        self.mcl = self.multicast_listener.multicast_socket
        self.process_id = process_id

    def run(self):
        print("Listening to network multicasts...")
        try:
            while True:
                data, addr = self.mcl.recvfrom(1024)
                if data:
                    data_list = pipe_filter.incoming_frame_filter(data.decode("utf-8"), str(addr[0]))
                    if data_list[2] != self.process_id:
                        self.incomings_pipe.put(data_list, block=False)
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

    def run(self):
        print("Listening to network udp_unicasts...")
        try:
            while True:
                data, addr = self.udp_sock.recvfrom(1024)
                if data:
                    self.incomings_pipe.put(pipe_filter.incoming_frame_filter(data.decode("utf-8"), str(addr[0])), block=False)
        except Exception as e:
            print(e)
