import multiprocessing

import threading
import time
from queue import PriorityQueue
from sockets import ServerSockets
import config as cfg
import pipe_filter

import socket


class SocketChannels(threading.Thread):

    def __init__(self):
        super(SocketChannels, self).__init__()
        self.incomings_pipe = PriorityQueue()
        self.multicast_listener = ServerSockets()
        self.mcl = self.multicast_listener.multicast_socket

    def run(self):
        print("Listening to network...")
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
