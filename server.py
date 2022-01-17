import uuid
import socket
import time
from message_builder import MessageBuilder
from incomings_pipe import MultiCastChannel, UdpSocketChannel
from election import BullyAlgorithm
from PrintBoard import Cons
from persistence import PersistenceMessaging


class Server:
    def __init__(self):
        self.physical_time = time.time()
        self.ProcessUUID = uuid.uuid4()
        self.server_starttime = time.time()
        self.DynamicDiscovery_timestamp = time.time()
        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)

        self.message_type = dict()

        self.BOARD_OF_SERVERS = {
            "ServerNodes": [],
            "NodeIP": [],
            "LastActivity": [],
            "HigherPID": [],  # depends on "ServerNodes" PIDs(uuid)...
        }

        self.primary = False
        self.election = False

        self.incoming_msgs_thread = MultiCastChannel(process_id=str(self.ProcessUUID))
        self.incoming_mssgs_udp_socket_thread = UdpSocketChannel()
        self.election_thread = BullyAlgorithm(self.BOARD_OF_SERVERS, self.ProcessUUID, self.server_starttime,
                                              self.primary)
        self.persistence_thread = PersistenceMessaging("TEST", self.BOARD_OF_SERVERS, self.ProcessUUID)
        self.console = Cons(self.BOARD_OF_SERVERS, self.primary, self.persistence_thread.state_clock_relation,
                            self.persistence_thread.state_clock)

        self._discovery_mssg_uuids_of_server = {}

        self.messenger = MessageBuilder(self.ProcessUUID)
        print(self.ProcessUUID)
        print(self.messenger.UUID)

        # --------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------

    def run_threads(self):
        # self.incoming_msgs_thread.daemon = True
        self.incoming_msgs_thread.start()
        # self.incoming_mssgs_udp_socket_thread.daemon = True
        self.incoming_mssgs_udp_socket_thread.start()
        # self.persistence_thread.daemon = True
        self.persistence_thread.start()
        # self.election_thread.daemon = True
        self.election_thread.start()
        # self.console.daemon = True
        self.console.start()

        # initial discovery broadcast
        self._dynamic_discovery(server_start=True)
        try:

            while True:
                self.console.clock = self.persistence_thread.state_clock
                self._updateLead()
                self.console.primaryppid = self.election_thread.primaryPID
                self.console.election_pending = self.election_thread.election_pending

                self._dynamic_discovery(server_start=False)
                # print(self._discovery_mssg_uuids_of_server)
                # try discovering if no server nodes running in 10 seconds intervals
                # multicast
                if not self.incoming_msgs_thread.incomings_pipe.empty():
                    self.handle_incoming_multicasts()

                # udp socket thread
                if not self.incoming_mssgs_udp_socket_thread.incomings_pipe.empty():
                    self.handle_incoming_udp_socket_channel_frames()

                # process outgoing messages from election thread
                if not self.election_thread.outgoing_mssgs.empty():
                    self.create_outgoing_frame()

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

    def _registerNewMessage(self, frame):
        self.message_type[frame[3]] = frame[0]

    def _messageAlreadyRegistred(self, mssg_uuid):
        if mssg_uuid in self.message_type:
            return True
        else:
            return False

    def handle_incoming_multicasts(self):
        data_list = self.incoming_msgs_thread.incomings_pipe.get()

        if data_list[0] == "DISCOVERY" and \
                data_list[1] == "CLIENT" and \
                self.election_thread.iAmLead():
            print("<----- GOT A CLIENT DISCOVERY REQUEST! ---->")
            print("<----- GOT A CLIENT DISCOVERY REQUEST! ---->")
            self._ackClientDiscovery(data_list[3], data_list[7])

        if data_list[0] == "DISCOVERY" and \
                data_list[1] == "SERVER" and \
                data_list[2] != str(self.ProcessUUID) and \
                data_list[7] != self.MY_IP:
            if data_list[7] not in self.BOARD_OF_SERVERS["NodeIP"]:
                print("<----- GOT A SERVER DISCOVERY REQUEST! ---->")
                print("<----- GOT A SERVER DISCOVERY REQUEST! ---->")
                self._addNode(data_list)
            else:
                self._updateServerBoard(data_list)

        # ack to DISCOVERY message...
        if data_list[0] == "DISCOVERY" and \
                data_list[1] == "SERVER" and \
                data_list[2] != str(self.ProcessUUID):
            self.election_thread.updateLastActivity(data_list)
            self._ackDiscovery(data_list[3], data_list[7])
            dm_clock = int(data_list[4])
            print(dm_clock)
            print(self.persistence_thread.state_clock)
            if (int(self.persistence_thread.state_clock) - dm_clock) >= 1:
                print("<---- A LOWER CLOCK DETECTED! ---->")
                print("<---- A LOWER CLOCK DETECTED! ---->")
                self.persistence_thread.init_recovery(data_list)

        if data_list[0] == "HEARTBEAT" and \
                data_list[2] != str(self.ProcessUUID) and \
                data_list[2] in self.BOARD_OF_SERVERS["ServerNodes"] and \
                data_list[1] == "SERVER" and \
                data_list[7] in self.BOARD_OF_SERVERS["NodeIP"]:
            print("GOT A HEARTBEAT!")
            self.election_thread.incoming_mssgs.put(data_list)
        if data_list[0] == "VICTORY" and \
                data_list[2] != str(self.ProcessUUID) and \
                data_list[1] == "SERVER":
            print("GOT AN VICTORY MESSAGE o.O!")
            self.election_thread.incoming_mssgs.put(data_list)
        if data_list[0] == "REPLICATION" and data_list[2] != str(self.ProcessUUID) and \
                not self._messageAlreadyRegistred(data_list[3]):
            print("<- GOT A REPLICATION MESSAGE FROM SOMEONE! ->")
            print("<- GOT A REPLICATION MESSAGE FROM SOMEONE! ->")
            self._registerNewMessage(data_list)
            self.persistence_thread.incomings_pipe.put(data_list)
        if data_list[0] == "QUERY" and data_list[1] == "QUERYING_CLIENT" and not self._messageAlreadyRegistred(
                data_list[3]):
            print("<- GOT A QUERY REQUEST FROM A QUERYING CLIENT! ->")
            print("<- GOT A QUERY REQUEST FROM A QUERYING CLIENT! ->")
            self._registerNewMessage(data_list)
            self.persistence_thread.incomings_pipe.put(data_list)

    def create_outgoing_frame(self):
        data_list = self.election_thread.outgoing_mssgs.get()
        if data_list[0] == "HEARTBEAT":
            self.messenger.multicast_hearbeat(data_list[0:7])  # multicast
        if data_list[0] == "ELECTION":
            self.messenger.election_mssg(data_list[0:7], data_list[7])  # unicast
        if data_list[0] == "ACK":  # to do
            self.messenger.ack_election_mssg(data_list[0:7], data_list[7])  # unicast
        if data_list[0] == "VICTORY":
            self.messenger.coordinator_mssg(data_list[0:7])  # multicast

    def handle_incoming_udp_socket_channel_frames(self):
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

        if data_list[0] == "UPDATE" and \
                data_list[3] not in self.persistence_thread.clock_mssguuid_relation and \
                not self._messageAlreadyRegistred(data_list[3]):
            print("<---- WE GOT AN UPDATE REQUEST FROM A GAS STATION!!!! ---->")
            print("<---- WE GOT AN UPDATE REQUEST FROM A GAS STATION!!!! ---->")
            if self.election_thread.iAmLead():
                self._registerNewMessage(data_list)
                self.messenger.client_transmission_ack_message(data_list[3], data_list[7])
                self.persistence_thread.incomings_pipe.put(data_list)

    def _dynamic_discovery(self, server_start):
        if len(self.BOARD_OF_SERVERS["ServerNodes"]) == 0 and server_start == True:
            message_uuid = self._create_DiscoveryUUID()
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid, str(self.persistence_thread.state_clock))
        self._discoveryIntervall()

    def _discoveryIntervall(self):
        # for @starttime
        if (float(time.time()) - float(self.DynamicDiscovery_timestamp)) > 5 and len(
                self.BOARD_OF_SERVERS["ServerNodes"]) == 0:
            message_uuid = self._create_DiscoveryUUID()
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid, str(self.persistence_thread.state_clock))
        # for @futher discoveries
        if (float(time.time()) - float(self.DynamicDiscovery_timestamp)) > 10 and len(
                self.BOARD_OF_SERVERS["ServerNodes"]) > 0:
            message_uuid = self._create_DiscoveryUUID()
            self.DynamicDiscovery_timestamp = time.time()
            self.messenger.dynamic_discovery_message(message_uuid, str(self.persistence_thread.state_clock))

    def _create_DiscoveryUUID(self):
        message_uuid = uuid.uuid4()
        self._discovery_mssg_uuids_of_server[str(message_uuid)] = False
        return message_uuid

    def _addNode(self, frame_list):
        self.BOARD_OF_SERVERS["ServerNodes"].append(frame_list[2])
        self.BOARD_OF_SERVERS["NodeIP"].append(frame_list[7])
        self.BOARD_OF_SERVERS["LastActivity"].append(float(time.time()))

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

    def _ackClientDiscovery(self, discovery_mssg_uuid, receiver):
        self.messenger.ack_client_dynamic_discovery_message(discovery_mssg_uuid, receiver)

    def _updateLead(self):
        if str(self.ProcessUUID) == str(self.election_thread.primaryPID):
            self.console.primary = True
            self.console.primaryppid = self.election_thread.primaryPID
        if str(self.ProcessUUID) != str(self.election_thread.primaryPID):
            self.console.primary = False
        # self.BOARD_OF_SERVERS = self.election_thread.BOARD_OF_SERVERS

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
