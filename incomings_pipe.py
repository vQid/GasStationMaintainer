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
                time.sleep(2)

                data, addr = self.mcl.recvfrom(1024)

                if data:
                    self.incomings_pipe.put(pipe_filter.incoming_frame_filter(data.decode(), str(addr[0])), block=True)
                    print("Received broadcast message:", data.decode())

        except:
            pass

class UdpSocketChannel(threading.Thread):

    def __init__(self):
        super(UdpSocketChannel, self).__init__()
        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        self.incomings_pipe = PriorityQueue()
        self.udp_single_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.udp_single_sock.bind((self.MY_IP, cfg.config["UDP_SOCKET_PORT"]))

    def run(self):
        print("Listening to network multicasts...")
        try:
            while True:
                print("inside incomings pipe thread")
                time.sleep(2)
                data, addr = self.udp_single_sock.recvfrom(1024)
                if data:
                    self.incomings_pipe.put(pipe_filter.incoming_frame_filter(data.decode(), str(addr[0])), block=True)
                    print("Received broadcast message:", data.decode())

        except:
            pass
