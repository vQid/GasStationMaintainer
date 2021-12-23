import threading
import uuid
import socket
import pipe_filter
import config as cfg
from client_udp_socket import ClientSocket
from queue import PriorityQueue
from tkinter import *
from QueryingClientWorker import QCWorker

group = cfg.config["MULTICAST_GROUP"]
port_mc = cfg.config["MCAST_PORT"]
port_udp_unicast = cfg.config["UDP_SOCKET_PORT"]
# 2-hop restriction in network
ttl = 2


class QueryingClient(threading.Thread):

    def __init__(self):
        super(QueryingClient, self).__init__()
        # Gas-Station-ID | DATE OF ENTRY | GTIN | PRODUCT DESCRIPTION | QUANTITY |
        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP,
                             socket.IP_MULTICAST_TTL,
                             ttl)
        self.datasets = []
        self.process_queue = PriorityQueue()
        self.clientsocket_thread = ClientSocket("TEST", self.process_queue, cfg.config["QUERYING_CLIENT_PORT"])
        self.querying_client_worker_thread = QCWorker(self.process_queue)

        window_name = "<-- QUERYING_APPLICATION -->"
        self.root = Tk()
        self.root.title(window_name)
        Label(text="QueryRequest = <RequestName>; Target = <specific Gas Station> or <ALL>").grid(row=0, columnspan=2)
        Label(text="QueryRequest", width=20).grid(row=1, column=0)
        self.QueryRequest = Entry(self.root)
        self.QueryRequest.grid(row=1, column=1)
        Label(text="Target:", width=20).grid(row=2, column=0)
        self.Target = Entry(self.root)
        self.Target.grid(row=2, column=1)

        Button(self.root, text="Request Entries!", command=self._queryRequest).grid(row=4, column=0, columnspan=3)

    def run(self):
        self.clientsocket_thread.daemon = True
        self.querying_client_worker_thread.daemon = True
        self.clientsocket_thread.start()
        self.querying_client_worker_thread.start()

        while True:
            try:
                self.root.mainloop()
            except Exception as e:
                print(e)

    def _clearEntrys(self):
        self.QueryRequest.delete(0, "end")
        self.Target.delete(0, "end")

    def _queryRequest(self):
        qc = str(self.QueryRequest.get())
        target = str(self.Target.get())

        if str(self.QueryRequest.get()) == "" or str(self.Target.get()) == "":
            print("ALL FIELDS ARE REQUIRED")
        if target.lower() == "all" and str(self.QueryRequest.get()) != "" and str(self.Target.get()) != "":
            print("ALL GAS STATIONS ARE SPECIFIED")
            print("QueryRequest ID:\t" + qc)
            print("Target:\t\t\t\t" + target)
            target = "ALL"
            self.querying_client_worker_thread.addNewQueryProcess(qc)
            self._mc_req(qc, target)
            self._clearEntrys()
        else:
            if str(self.QueryRequest.get()) != "" and str(self.Target.get()) != "":
                print("A TARGET GAS STATION SPECFIED")
                print("QueryRequest ID:\t" + qc)
                print("Target:\t\t\t" + target)
                self.querying_client_worker_thread.addNewQueryProcess(qc)
                self._mc_req(qc, target)
                self._clearEntrys()

    def _mc_req(self, queryID, targetID):
        statement = queryID + "," + targetID
        template = {
            "MESSAGE_TYPE": "QUERY",
            "NODE_TYPE": "QUERYING_CLIENT",
            "PROCESS_UUID64": "",
            "MSSG_UUID64": str(uuid.uuid4()),
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": statement
        }
        print(list(template.values()))
        self.sock.sendto(str.encode(
            pipe_filter.outgoing_frame_creater(list(template.values()))),
            (group, port_mc))


if __name__ == "__main__":
    client = QueryingClient()
    client.run()
