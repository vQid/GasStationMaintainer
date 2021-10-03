
class ElectionTemplate():

    def __init__(self, process_uuid):
        self.PROCESS_UUID = process_uuid

        self.election_template = [
            "ELECTION",
            "SERVER",
            str(self.PROCESS_UUID),
            "",
            "",
            "",
            "ARE YOU ALIVE?",
            ""
        ]

        self.heartbeat_template = [
            "HEARTBEAT",
            "SERVER",
            str(self.PROCESS_UUID),
            "",
            "",
            "",
            "I AM ALIVE",
        ]

        self.coordinator_template = [
            "VICTORY",
            "SERVER",
            str(self.PROCESS_UUID),
            "",
            "",
            "",
            "I AM LEAD",
        ]

        self.ackElection_template = [
            "ACK",
            "SERVER",
            str(self.PROCESS_UUID),
            "",
            "",
            "",
            "I AM ALIVE",
            ""
        ]

    def getElectionTemp(self, newElectionUUID, receiver):
        self.election_template[3] = newElectionUUID
        self.election_template[7] = receiver
        return self.election_template

    def getHeartbeatTemp(self):
        return self.heartbeat_template

    def getCoordinatorTemp(self):
        return self.coordinator_template

    def getAckToElectionTemp(self, election_uuid, receiver):
        self.ackElection_template[3] = election_uuid
        self.ackElection_template[7] = receiver
        return self.ackElection_template

