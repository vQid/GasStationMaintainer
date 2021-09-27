import multiprocessing
import uuid
import socket
from threading import Thread
import time
from message_builder import MessageBuilder
from incomings_pipe import MultiCastChannel, UdpSocketChannel
import pipe_filter

import config as cfg


class Server:
    def __init__(self):
        # for updates -> sequentiell numbering of updates over all gas stations
        self.incoming_msgs_thread = MultiCastChannel()
        self.incoming_mssgs_udp_socket_thread = UdpSocketChannel()
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
            "HigherPID": [], # depends on "ServerNodes" keys...
            "PRIMARY": []
        }
        self._discovery_uuids_of_server = {}

        self.primary = False
        self.election = False

        self.messenger = MessageBuilder(self.ProcessUUID)
        print(self.ProcessUUID)
        print(self.messenger.UUID)



    def run_threads(self):
        self.incoming_msgs_thread.daemon = True
        self.incoming_msgs_thread.start()
        self.incoming_mssgs_udp_socket_thread.daemon = True
        self.incoming_mssgs_udp_socket_thread.start()
        # initial discovery broadcast
        self._dynamic_discovery(server_start=True)
        try:
            while True:
                time.sleep(2)
                print(self.BOARD_OF_SERVERS)
                #print(self._discovery_uuids_of_server)
                # try discovering if no server nodes running in 10 seconds intervals
                self._dynamic_discovery(server_start=False)
                if not self.incoming_msgs_thread.incomings_pipe.empty():
                    data_list = self.incoming_msgs_thread.incomings_pipe.get()
                    print("new frame")
                    print(data_list)
                    if (data_list[0] == "DISCOVERY" or data_list[0] == "HEARTBEAT") and \
                            data_list[1] == "SERVER" and \
                            data_list[2] != str(self.ProcessUUID) and \
                            data_list[7] != self.MY_IP:
                        if data_list[7] not in self.BOARD_OF_SERVERS["NodeIP"]:
                            self._addNode(data_list)
                        else:
                            self._updateServerBoard(data_list)
                        # ack to DISCOVERY message...
                    if data_list[0] == "DISCOVERY" and \
                            data_list[1] == "SERVER" and \
                            data_list[2] != str(self.ProcessUUID):
                        self._ackDiscovery(data_list[3], data_list[7])

                if not self.incoming_mssgs_udp_socket_thread.incomings_pipe.empty():
                    data_list = self.incoming_mssgs_udp_socket_thread.incomings_pipe.get()
                    print(data_list)
                    print("something from udp socket!")
                    print("something from udp socket!")
                    print("something from udp socket!")
                    print("something from udp socket!")
                    print("something from udp socket!")
                    print("something from udp socket!")
                    print("something from udp socket!")

                    print(data_list)

                    if data_list[0] == "ACK" and \
                            data_list[1] == "SERVER" and \
                            data_list[2] != str(self.ProcessUUID):
                        if data_list[7] not in self.BOARD_OF_SERVERS["NodeIP"]:
                            self._addNode(data_list)
                        else:
                            self._updateServerBoard(data_list)
        except Exception as e:
            print(e)

        finally:
            self.incoming_msgs_thread.join()
            self.incoming_mssgs_udp_socket_thread.join()


    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------

    def _dynamic_discovery(self, server_start):
        if len(self.BOARD_OF_SERVERS["ServerNodes"]) == 0 and server_start == True:
            message_uuid = self._create_DiscoveryUUID()
            print("inside dynamic discovery! - initial message")
            print(message_uuid)
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid)
        self._discoveryIntervall()

    def _discoveryIntervall(self):
        if (float(time.time()) - float(self.DynamicDiscovery_timestamp)) > 10 and len(self.BOARD_OF_SERVERS["ServerNodes"]) == 0:
            message_uuid = self._create_DiscoveryUUID()
            print("inside dynamic discovery! - interval")
            print(message_uuid)
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid)

    def _create_DiscoveryUUID(self):
        message_uuid = uuid.uuid4()
        print("spawned uuid!")
        print(message_uuid)
        self._discovery_uuids_of_server[str(message_uuid)] = False
        return message_uuid

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

    def _ackDiscovery(self, discovery_mssg_uuid, receiver):
        print("ack check!")
        print(receiver)
        print(discovery_mssg_uuid)
        self.messenger.ack_dynamic_discovery_message(discovery_mssg_uuid, receiver)

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
