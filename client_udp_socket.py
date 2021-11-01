import threading
import socket
import time
import uuid
from queue import PriorityQueue
import config as cfg
import pipe_filter


class ClientSocket(threading.Thread):

    def __init__(self, station_id, incomings_pipe):
        super(ClientSocket, self).__init__()
        self.primary_IP = ""
        self.stationID = station_id

        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.udp_sock.bind(("", cfg.config["STATION_CLIENT_PORT"]))

        self.incomings_pipe = incomings_pipe


    def run(self):
        print("Client Socket started...")

        try:
            while True:
                data, addr = self.udp_sock.recvfrom(1024)
                print("after receive")
                if data:
                    print(data)
                    self.incomings_pipe.put(pipe_filter.incoming_frame_filter(data.decode("utf-8"), str(addr[0])), block=False)
        except Exception as e:
            print(e)





