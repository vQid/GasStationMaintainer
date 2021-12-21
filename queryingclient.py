import threading
import time
from client_udp_socket import ClientSocket
from queue import PriorityQueue


class QueryingClient(threading.Thread):

    def __init__(self):
        super(QueryingClient, self).__init__()
        # Gas-Station-ID | DATE OF ENTRY | GTIN | PRODUCT DESCRIPTION | QUANTITY |
        self.datasets = []
        self.incomings_pipe = PriorityQueue()
        self.client_udp_socket = ClientSocket(None, self.incomings_pipe)

    def run(self):
        # start upd thread here
        while True:
            try:
                # get primary host
                # input query parameter...
                time.sleep(5)
                # clear buffer
                # generate report
                #
                #

            except Exception as e:
                print(e)


if __name__ == "__main__":
    pass
