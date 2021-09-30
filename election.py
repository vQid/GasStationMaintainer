import multiprocessing
import threading
import time
from queue import PriorityQueue
import config as cfg
import uuid


class BullyAlgorithm(threading.Thread):

    def __init__(self, board_of_nodes, process_uuid):
        super(BullyAlgorithm, self).__init__()
        self.primary = ""
        self.incoming_mssgs = PriorityQueue()
        self.outgoing_mssgs = PriorityQueue()
        self.last_heartbeat_timestamp = time.time()
        self.election_pending = False
        self.vote_timestamp = time.time()
        self.coordinator_message_timestamp = None
        self.BOARD_OF_SERVERS = board_of_nodes
        self.PROCESS_UUID = str(process_uuid)
        self.ELECTION_BOARD = {
            "election": {
                "voteID": [],
                "PID": [],
                "voteTimestamp": [],
            },
            "electionID": []

        }

    def run(self):
        try:
            while True:
                self._heartbeat()

                if not self.incoming_mssgs.empty():
                    data_list = self.incoming_mssgs.get()
                    if data_list[0] == "HEARTBEAT":
                        self._updateLastActivity(data_list)

                if self.primary == "" and self.election_pending != True:
                    #self.election_pending = True
                    #self._coordinator_message()
                    pass

                #self._detectCrash()




        except Exception as e:
            print(e)

    def _coordinator_message(self):
        # send multicast coordinator message
        self.coordinator_message_timestamp = time.time()
        self.election_pending = True



    def _detectCrash(self):
        for iteration in self.BOARD_OF_SERVERS["LastActivity"]:
            if float(time.time() - float(iteration.index())) > 5:
                print("A server crashed!")
                if self.BOARD_OF_SERVERS["ServerNodes"][self.BOARD_OF_SERVERS["LastActivity"][iteration.index()]]:
                    print("PRIMARY CRASH DETECTED!")
                    print("PRIMARY " + self.BOARD_OF_SERVERS["ServerNodes"][iteration.index()] + " " + self.BOARD_OF_SERVERS["NodeIP"][iteration.index()] + " crashed!")
                    self.primary = ""
                    del self.BOARD_OF_SERVERS["ServerNodes"][iteration.index()]
                    del self.BOARD_OF_SERVERS["NodeIP"][iteration.index()]
                    del self.BOARD_OF_SERVERS["LastActivity"][iteration.index()]
                    del self.BOARD_OF_SERVERS["HigherPID"][iteration.index()]
                    del self.BOARD_OF_SERVERS["PRIMARY"][iteration.index()]
                else:
                    del self.BOARD_OF_SERVERS["ServerNodes"][iteration.index()]
                    del self.BOARD_OF_SERVERS["NodeIP"][iteration.index()]
                    del self.BOARD_OF_SERVERS["LastActivity"][iteration.index()]
                    del self.BOARD_OF_SERVERS["HigherPID"][iteration.index()]
                    del self.BOARD_OF_SERVERS["PRIMARY"][iteration.index()]


    def _initiateElection(self):
        # broadcastElection to slaves!

        pass

    def _heartbeat(self):
        if (float(time.time() - float(self.last_heartbeat_timestamp))) > float(cfg.config["HEARTBEAT_INTERVAL"]):
            #send heartbeats to lower pids...
            if len(self.BOARD_OF_SERVERS["HigherPID"]) > 0:
                self.outgoing_mssgs.put(
                    [
                        "HEARTBEAT",
                        "SERVER",
                        str(self.PROCESS_UUID),
                        "",
                        "",
                        "",
                        "",
                    ]
                )
                self.last_heartbeat_timestamp = time.time()

    def _updateLastActivity(self, frame_list):
        if frame_list[2] in self.BOARD_OF_SERVERS["ServerNodes"]:
            index = self.BOARD_OF_SERVERS["ServerNodes"].index(frame_list[2])
            self.BOARD_OF_SERVERS["LastActivity"][index] = float(time.time())


