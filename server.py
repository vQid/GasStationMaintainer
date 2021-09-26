import multiprocessing
import uuid
import socket
from threading import Thread
import time
from message_builder import MessageBuilder
from incomings_pipe import SocketChannels
import pipe_filter

import config as cfg


class Server:
    def __init__(self):
        # for updates -> sequentiell numbering of updates over all gas stations
        self.clock_block_number = 0
        self.physical_time = time.time()
        self.ProcessUUID = uuid.uuid4()
        self.DynamicDiscovery_timestamp = time.time()
        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        self.updates = {}
        self.BOARD_OF_SERVERS = {
            "ServerNodes": [],
            "NodeIP": [],
            "LastActivity": [],
            "HigherPID": [],
            "PRIMARY": []
        }
        # self.discovery_uuid =

        self.primary = False
        self.election = False

        self.messenger = MessageBuilder(self.ProcessUUID)
        print(self.ProcessUUID)
        print(self.messenger.UUID)

        self.amk = []

    def run_threads(self):
        self.incoming_msgs_thread = SocketChannels()
        self.incoming_msgs_thread.start()
        # initial discovery broadcast
        self._dynamic_discovery(server_start=True)

        while True:
            time.sleep(2)
            print(self.BOARD_OF_SERVERS)
            # try discovering if no server nodes running in 10 seconds intervals
            self._dynamic_discovery(server_start=False)
            if not self.incoming_msgs_thread.incomings_pipe.empty():
                data_list = self.incoming_msgs_thread.incomings_pipe.get()
                if (data_list[0] == "DISCOVERY" or data_list[0] == "HEARTBEAT") and \
                        data_list[1] == "SERVER" and \
                        data_list[2] != str(self.ProcessUUID) and \
                        data_list[7] != self.MY_IP:
                    if data_list[7] not in self.BOARD_OF_SERVERS["NodeIP"]:
                        self._addNode(data_list)
                    else:
                        self._updateServerBoard(data_list)
                    if data_list[0] == "DISCOVERY":
                        # ack to DISCOVERY!
                        self._ackDiscovery()

                # self.amk.append(self.incoming_msgs_thread.incomings_pipe.get())

    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------

    def _dynamic_discovery(self, server_start):
        if len(self.BOARD_OF_SERVERS["ServerNodes"]) == 0 and server_start == True:
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message()
        self._discoveryIntervall()

    def _discoveryIntervall(self):
        if (float(time.time()) - float(self.DynamicDiscovery_timestamp)) > 10 and len(self.BOARD_OF_SERVERS["ServerNodes"]) == 0:
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message()

    def _addNode(self, frame_list):
        self.BOARD_OF_SERVERS["ServerNodes"].append(frame_list[2])
        self.BOARD_OF_SERVERS["NodeIP"].append(frame_list[7])
        self.BOARD_OF_SERVERS["LastActivity"].append(float(time.time()))
        self.BOARD_OF_SERVERS["PRIMARY"].append(False)
        if str(self.ProcessUUID) < str(frame_list[2]):
            self.BOARD_OF_SERVERS["HigherPID"].append(True)
        else:
            self.BOARD_OF_SERVERS["HigherPID"].append(False)

    def _updateServerBoard(self, frame_list):
        index = self.BOARD_OF_SERVERS["NodeIP"].index(frame_list[7])
        self.BOARD_OF_SERVERS["ServerNodes"][index] = frame_list[2]
        self.BOARD_OF_SERVERS["LastActivity"][index] = float(time.time())
        if str(self.ProcessUUID) < str(frame_list[2]):
            self.BOARD_OF_SERVERS["HigherPID"][index] = True
        else:
            self.BOARD_OF_SERVERS["HigherPID"][index] = False

    def _ackDiscovery(self):
        pass

    # kill server from board when last activity greater then 30 seconds!
    def _killNodeFromServerBoard(self):
        pass


    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------

if __name__ == "__main__":
    server = Server()
    server.run_threads()
