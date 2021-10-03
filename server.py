import uuid
import socket
import time
from message_builder import MessageBuilder
from incomings_pipe import MultiCastChannel, UdpSocketChannel
from election import BullyAlgorithm
from PrintBoard import Cons


class Server:
    def __init__(self):
        # for updates -> sequentiell numbering of updates over all gas stations

        # clock block number is only to iterate if there is updates!
        self.clock_block_number = 0
        self.physical_time = time.time()
        self.ProcessUUID = uuid.uuid4()
        self.server_starttime = time.time()
        self.DynamicDiscovery_timestamp = time.time()
        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        self.updates = {}
        self.BOARD_OF_SERVERS = {
            "ServerNodes": [],
            "NodeIP": [],
            "LastActivity": [],
            "HigherPID": [],  # depends on "ServerNodes" keys...
            "PRIMARY": []
        }

        self.primary = False
        self.election = False

        self.incoming_msgs_thread = MultiCastChannel(process_id=str(self.ProcessUUID))
        self.incoming_mssgs_udp_socket_thread = UdpSocketChannel()
        self.election_thread = BullyAlgorithm(self.BOARD_OF_SERVERS, self.ProcessUUID, self.server_starttime, self.primary)

        self.console = Cons(self.BOARD_OF_SERVERS, self.primary)

        self._discovery_mssg_uuids_of_server = {}

        self.messenger = MessageBuilder(self.ProcessUUID)
        print(self.ProcessUUID)
        print(self.messenger.UUID)

    def run_threads(self):
        self.incoming_msgs_thread.daemon = True
        self.incoming_msgs_thread.start()
        self.incoming_mssgs_udp_socket_thread.daemon = True
        self.incoming_mssgs_udp_socket_thread.start()
        self.election_thread.daemon = True
        self.election_thread.start()
        self.console.daemon = True
        self.console.start()
        # initial discovery broadcast
        self._dynamic_discovery(server_start=True)
        try:
            while True:

                if str(self.ProcessUUID) == str(self.election_thread.primaryPID):
                    self.console.primary = True
                if str(self.ProcessUUID) != str(self.election_thread.primaryPID):
                    self.console.primary = False
                self._dynamic_discovery(server_start=False)
                # print(self._discovery_mssg_uuids_of_server)
                # try discovering if no server nodes running in 10 seconds intervals
                # multicast
                if not self.incoming_msgs_thread.incomings_pipe.empty():
                    data_list = self.incoming_msgs_thread.incomings_pipe.get()
                    if data_list[0] == "DISCOVERY" and \
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
                    if data_list[0] == "HEARTBEAT" and \
                            data_list[2] != str(self.ProcessUUID) and \
                            data_list[2] in self.BOARD_OF_SERVERS["ServerNodes"] and \
                            data_list[1] == "SERVER" and \
                            data_list[7] in self.BOARD_OF_SERVERS["NodeIP"]:
                        print("GOT AN HEARTBEAT!")
                        self.election_thread.incoming_mssgs.put(data_list)

                # process outgoing messages from election thread
                if not self.election_thread.outgoing_mssgs.empty():
                    data_list = self.election_thread.outgoing_mssgs.get()
                    if data_list[0] == "HEARTBEAT":
                        self.messenger.multicast_hearbeat(data_list[0:7]) # multicast
                    if data_list[0] == "ELECTION":
                        self.messenger.election_mssg(data_list[0:7], data_list[7]) # unicast
                    if data_list[0] == "ACK": # to do
                        self.messenger.ack_election_mssg(data_list[0:7], data_list[7]) # unicast
                    if data_list[0] == "COORDINATOR":
                        self.messenger.coordinator_mssg(data_list[0:7]) # multicast

                # udp socket thread
                if not self.incoming_mssgs_udp_socket_thread.incomings_pipe.empty():
                    data_list = self.incoming_mssgs_udp_socket_thread.incomings_pipe.get()
                    if data_list[0] == "ACK" and \
                            data_list[1] == "SERVER" and \
                            data_list[2] != str(self.ProcessUUID) and \
                            data_list[3] != self.election_thread.ELECTION_BOARD["electionID"]:
                        if data_list[7] not in self.BOARD_OF_SERVERS["NodeIP"]:

                            self._addNode(data_list)
                            self._discovery_mssg_uuids_of_server[data_list[7]] = True
                        else:
                            self._updateServerBoard(data_list)
                    if data_list[0] == "ACK" and data_list[3] == self.election_thread.ELECTION_BOARD["electionID"]:
                        self.election_thread.incoming_mssgs.put(data_list)

                    if data_list[0] == "ELECTION":
                        self.election_thread.incoming_mssgs.put(data_list)


        except Exception as e:
            print(e)

        finally:
            self.incoming_msgs_thread.join()
            self.incoming_mssgs_udp_socket_thread.join()
            self.election_thread.join()
            self.console.join()

    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------
    # --------------------------------------------------------

    def _dynamic_discovery(self, server_start):
        if len(self.BOARD_OF_SERVERS["ServerNodes"]) == 0 and server_start == True:
            message_uuid = self._create_DiscoveryUUID()
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid)
        self._discoveryIntervall()

    def _discoveryIntervall(self):
        # for @starttime
        if (float(time.time()) - float(self.DynamicDiscovery_timestamp)) > 10 and len(
                self.BOARD_OF_SERVERS["ServerNodes"]) == 0:
            message_uuid = self._create_DiscoveryUUID()
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid)
        # for @futher discoveries
        if (float(time.time()) - float(self.DynamicDiscovery_timestamp)) > 30 and len(self.BOARD_OF_SERVERS["ServerNodes"]) > 0:
            message_uuid = self._create_DiscoveryUUID()
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid)


    def _create_DiscoveryUUID(self):
        message_uuid = uuid.uuid4()
        self._discovery_mssg_uuids_of_server[str(message_uuid)] = False
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
