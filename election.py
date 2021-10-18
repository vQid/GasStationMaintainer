import threading
import time
import uuid
from queue import PriorityQueue
import config as cfg
from elections_temps import ElectionTemplate


class BullyAlgorithm(threading.Thread):

    def __init__(self, board_of_nodes, process_uuid, starttime_server, primary):
        super(BullyAlgorithm, self).__init__()
        self.primaryPID = ""
        self.bool_primary = primary
        self.incoming_mssgs = PriorityQueue()
        self.outgoing_mssgs = PriorityQueue()
        self.last_heartbeat_timestamp = time.time()

        self.server_start_time = float(starttime_server)
        self.BOARD_OF_SERVERS = board_of_nodes
        self.PROCESS_UUID = str(process_uuid)
        self.temps = ElectionTemplate(self.PROCESS_UUID)

        # set true if election is pending => to block further server joining thenetwork during elections
        self.election_pending = False

        self.election_timeout_timestamp = time.time()
        self.ELECTION_BOARD = {
            "electionHighestPID": str(self.PROCESS_UUID),
            "electionID": "",
        }

    def run(self):
        time.sleep(3)  # for w8ing until server discovers other servers!
        print("starting election thread...")
        try:
            while True:
                self._initateElection()
                self._heartbeat()
                self._monitorTimeout()
                self._detectCrash()
                self._refreshBoardOfServers()

                if not self.incoming_mssgs.empty():
                    data_list = self.incoming_mssgs.get()
                    self.handleMessages(data_list)

        except Exception as e:
            print(e)

    def _initateElection(self):
        if self.primaryPID == "" and self.election_pending != True:
            print("starting an election")
            self.election_pending = True
            election_uuid = str(uuid.uuid4())
            self.ELECTION_BOARD["electionID"] = election_uuid
            if True in self.BOARD_OF_SERVERS["HigherPID"] and len(self.BOARD_OF_SERVERS["ServerNodes"]) > 0:
                self._sendMessageToHigherPPIDs(election_uuid)
                self._setTimeout()
            if not True in self.BOARD_OF_SERVERS["HigherPID"]:
                self._broadcastVictory()
                self._releaseElection()



    def _sendMessageToHigherPPIDs(self, election_uuid):
        for idx, val in enumerate(self.BOARD_OF_SERVERS["NodeIP"]):
            if self.BOARD_OF_SERVERS["HigherPID"][idx]:
                self.outgoing_mssgs.put(self.temps.getElectionTemp(election_uuid, val))

    def _detectCrash(self):
        if self.primaryPID != str(self.PROCESS_UUID) and self.election_pending != True:
            primaryboardindex = self.BOARD_OF_SERVERS["ServerNodes"].index(self.primaryPID)
            if (float(time.time() - float(self.BOARD_OF_SERVERS["LastActivity"][primaryboardindex]))> cfg.config["ELECTION_TIMEOUT"]):
                print("PRIMARY CRASHED!")
                print("PRIMARY CRASHED!")
                print("PRIMARY CRASHED!")
                del self.BOARD_OF_SERVERS["ServerNodes"][primaryboardindex]
                del self.BOARD_OF_SERVERS["NodeIP"][primaryboardindex]
                del self.BOARD_OF_SERVERS["LastActivity"][primaryboardindex]
                del self.BOARD_OF_SERVERS["HigherPID"][primaryboardindex]
                self.primaryPID = ""
                self._initateElection()

    def handleMessages(self, data_frame):
        if data_frame[0] == "HEARTBEAT":
            self._updateLastActivity(data_frame)
        if data_frame[0] == "ACK":
            print("got ack to my mssg!!!!")
            if data_frame[2] > str(self.ELECTION_BOARD["electionHighestPID"]):
                print("processing ack of my election")
                self.ELECTION_BOARD["electionHighestPID"] = data_frame[2]
                self._updateLastActivity(data_frame)
                self._setTimeout()

        if data_frame[0] == "ELECTION":
            print("got an election Message from : !")
            print(data_frame)
            self.outgoing_mssgs.put(self.temps.getAckToElectionTemp(data_frame[3], data_frame[7]))
            if data_frame[2] > str(self.PROCESS_UUID):
                print("is higher!")
                self._setTimeout()
            if data_frame[2] < str(self.PROCESS_UUID):
                if not True in self.BOARD_OF_SERVERS["HigherPID"]:
                    self._broadcastVictory()


        if data_frame[0] == "VICTORY":
            if data_frame[2] > str(self.PROCESS_UUID):
                print("GOT AN VICTORY MESSAGE!!!")
                print("GOT AN VICTORY MESSAGE!!!")
                print("GOT AN VICTORY MESSAGE!!!")
                print("GOT AN VICTORY MESSAGE!!!")
                print("GOT AN VICTORY MESSAGE!!!")
                print("GOT AN VICTORY MESSAGE!!!")
                print(data_frame)
                self.primaryPID = data_frame[2]
                self._releaseElection()
            else:
                self._initateElection()

    def _broadcastVictory(self):
        print("in broadcast victory mssg")
        if not True in self.BOARD_OF_SERVERS["HigherPID"]:
            self.outgoing_mssgs.put(self.temps.getCoordinatorTemp())
            self.primaryPID = self.PROCESS_UUID
            print("sended VICTORY MESSAGE AND REALEASING ELECTION STATUS")
            self._releaseElection()


    def _releaseElection(self):
        self.ELECTION_BOARD["electionHighestPID"] = str(self.PROCESS_UUID)
        self.ELECTION_BOARD["electionID"] = ""
        self.election_pending = False
        print("ELECTION IS OVER")

    def _setTimeout(self):
        self.election_timeout_timestamp = time.time()

    def _monitorTimeout(self):
        if self.election_pending == True:
            if (float(time.time() - float(self.election_timeout_timestamp)) > cfg.config["ELECTION_TIMEOUT"]) and \
                    self.ELECTION_BOARD["electionHighestPID"] != "":
                # resend election messages...
                self._sendMessageToHigherPPIDs(self.ELECTION_BOARD["electionID"])
            else:
                self.primaryPID = self.ELECTION_BOARD["electionHighestPID"]


    def _refreshBoardOfServers(self):
        # if higher PID Nodes not responding then delete!
        for idx, val in enumerate(self.BOARD_OF_SERVERS["NodeIP"]):
            if self.BOARD_OF_SERVERS["HigherPID"][idx] == True and \
                    self.primaryPID == "" and \
                    (float(time.time() - float(self.BOARD_OF_SERVERS["LastActivity"][idx]))) > 5:
                del self.BOARD_OF_SERVERS["ServerNodes"][idx]
                del self.BOARD_OF_SERVERS["NodeIP"][idx]
                del self.BOARD_OF_SERVERS["LastActivity"][idx]
                del self.BOARD_OF_SERVERS["HigherPID"][idx]

    def _heartbeat(self):
        if (float(time.time() - float(self.last_heartbeat_timestamp))) > float(cfg.config["HEARTBEAT_INTERVAL"]):
            # send heartbeats to lower pids...
            if len(self.BOARD_OF_SERVERS["ServerNodes"]) > 0 and self._iAmLead():
                self.outgoing_mssgs.put(self.temps.getHeartbeatTemp())
                self.last_heartbeat_timestamp = time.time()

    def _updateLastActivity(self, frame_list):
        print("inside heartbeat function")
        if frame_list[2] in str(self.BOARD_OF_SERVERS["ServerNodes"]):
            index = self.BOARD_OF_SERVERS["ServerNodes"].index(frame_list[2])
            self.BOARD_OF_SERVERS["LastActivity"][index] = float(time.time())

    def updateElectionBoard(self, data_list):
        if data_list[2] > self.ELECTION_BOARD["electionHighestPID"]:
            self.ELECTION_BOARD["electionHighestPID"] = data_list[2]

    def _iAmLead(self):
        if str(self.PROCESS_UUID) == self.primaryPID and self.election_pending != True:
            return True
        else:
            return False
