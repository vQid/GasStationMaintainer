import threading


class BullyAlgorithm(threading.Thread):

    def __init__(self):
        super(BullyAlgorithm, self).__init__()
        self.primary = False
        self.election_pending = False

    def run(self):
        pass