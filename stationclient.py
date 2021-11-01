import datetime
import threading
import socket
import time
import uuid
from queue import PriorityQueue, Queue

import config
import config as cfg
import pipe_filter
import sys
from client_udp_socket import ClientSocket
from tkinter import *



class GuiClass(threading.Thread):

    def __init__(self, stationID):
        super(GuiClass, self).__init__()
        self.stationID = stationID
        self.outgoings_pipe = Queue()
        self.incomings_pipe = Queue()
        self.primaryIP = ""

        self.clientsocket_thread = ClientSocket(self.stationID, self.incomings_pipe)
        self.worker_thread = WorkerClass(self.stationID, self.incomings_pipe, self.outgoings_pipe, self.primaryIP)

        self.transmission_template = {
            "MESSAGE_TYPE": "UPDATE",
            "NODE_TYPE": "CLIENT",
            "PROCESS_UUID64": "",
            "MSSG_UUID64": "",
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": "",
        }

        window_name = "Gas Station ID <" + stationID + ">"
        self.root = Tk()
        self.root.title(window_name)

        Label(text=("The Primary is: " + self.primaryIP)).grid(row=0, column=0)
        self.connection_label = Label(text=("-" + self.primaryIP)).grid(row=0, column=1)
        Label(text="Item GTIN", width=20).grid(row=1, column=0)
        self.gtin = Entry(self.root)
        self.gtin.grid(row=1, column=1)
        Label(text="Product Description", width=20).grid(row=2, column=0)
        self.product_description = Entry(self.root)
        self.product_description.grid(row=2, column=1)
        Label(text="Quantity", width=20).grid(row=3, column=0)
        self.quantity = Entry(self.root)
        self.quantity.grid(row=3, column=1)

        Button(self.root, text="Transmit dataset...", command=self._transmitData).grid(row=4, column=0, columnspan=3)



    def run(self):
        self.clientsocket_thread.start()
        self.worker_thread.start()

        try:
            self.root.mainloop()
        except Exception as e:
            print(e)



    #bool
    def _checkInputValidity(self, input):
        #regex possible
        print("method" + str(self._checkInputValidity))
        return True

    def _clearEntrys(self):
        self.gtin.delete(0, "end")
        self.product_description.delete(0, "end")
        self.quantity.delete(0, "end")

    def _transmitData(self):
        print("DATA TRANSMISSION...")
        self.connection_label = Label(text=self.worker_thread.primaryIP, width=20).grid(row=0, column=1, sticky="w")
        try:
            if self.gtin.get() == "" or self.product_description.get() == "" or self.quantity.get() == "":
                print("ALL FIELDS ARE REQUIRED")
            else:
                message_uuid = str(uuid.uuid4())
                statement = []
                statement.append(self.gtin.get())
                statement.append(self.product_description.get())
                statement.append(self.quantity.get())
                joined_statement = ",".join(statement)
                self.transmission_template["MSSG_UUID64"] = message_uuid
                self.transmission_template["PROCESS_UUID64"] = self.stationID
                self.transmission_template["STATEMENT"] = joined_statement
                self.transmission_template["PHYSICAL_CLOCK"] = str(datetime.datetime.now())
                print(";".join(self.transmission_template.values()))
                self.outgoings_pipe.put(";".join(self.transmission_template.values()))

                self.worker_thread.datasets[message_uuid] = ";".join(self.transmission_template.values())
                self.worker_thread.unacked[message_uuid] = False
                self._clearEntrys()

        except Exception as e:
            print(e)


    def dosomething(self):
        print("do something command amk!")




class WorkerClass(threading.Thread):

    def __init__(self, stationID, incomings_referece, outgoings_reference, primaryIP):
        super(WorkerClass, self).__init__()
        self.stationID = stationID
        self.primaryIP = primaryIP

        self.incomings_pipe = incomings_referece
        self.outgoings_pipe = outgoings_reference

        self.MY_HOST = socket.gethostname()
        self.MY_IP = socket.gethostbyname(self.MY_HOST)
        self.ttl = 2

        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self.udp_sock.bind(("", cfg.config["UDP_SOCKET_PORT"]))



        self.mc_sock = socket.socket(socket.AF_INET,
                                     socket.SOCK_DGRAM,
                                     socket.IPPROTO_UDP)
        self.mc_sock.setsockopt(socket.IPPROTO_IP,
                                socket.IP_MULTICAST_TTL,
                                self.ttl)

        # key= mssg_uuid ; value = dataset_list
        self.datasets = {
        }
        # key= mssg_uuid ; value = TRUE
        self.acked = {
        }
        # key= mssg_uuid ; value = FALSE
        self.unacked = {
        }

        self.lastsend_timestamp = time.time()
        self.retry_timeout = 8

        self.discovery_timestamp = time.time()
        self.reDiscovery_timeout = 10  # let it be valid as a heartbeat

        self.input_gate = Queue()

        self.discovery_message_uuid = str(uuid.uuid4())

        self.dynamic_discovery_template = {
            "MESSAGE_TYPE": "DISCOVERY",
            "NODE_TYPE": "CLIENT",
            "PROCESS_UUID64": "",
            "MSSG_UUID64": "",
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": "I NEED A LEAD SERVER TO TALK TO",
        }
        self.raw_data_structure = {
            "GTIN": 0,
            "PRODUCT_DESCRIPTION": 1,
            "QUANTITY": 2,
        }



    def run(self):
        self._dynamic_discovery(client_start=True)
        try:
            while True:
                time.sleep(2)
                print("PRINTING ACKED MESSAGES: ")
                print(self.acked)
                print("The Primary IP is: " + self.primaryIP)
                print("worker is working")
                self._dynamic_discovery(client_start=False)
                self._retryTimer()
                if not self.incomings_pipe.empty():
                    data_list = self.incomings_pipe.get(block=False)
                    self._handleMessageFrame(data_list)
                    print("GOT SOMETHING !!!!!!!!!")
                    print(data_list)
                if not self.outgoings_pipe.empty():
                    self._sendMessagesInQueue()

        except Exception as e:
            print(e)

    def _sendMessagesInQueue(self):
        self.udp_sock.sendto(str.encode(self.outgoings_pipe.get()), (self.primaryIP, config.config["UDP_SOCKET_PORT"]))

    def _handleMessageFrame(self, data_list):
        if data_list[3] == self.discovery_message_uuid:
            self.primaryIP = data_list[7]
        if data_list[0] == "ACK" and data_list[3] in self.unacked:
            print("GOT AN ACK MESSAGE FROM PRIMARY!")
            print(data_list)
            self.unacked.pop(data_list[3])
            self.acked[data_list[3]] = True
            print("The Message " + data_list[3] + " with dataset " + self.datasets[data_list[3]] + " has ben acked now!")




    def _addToUnacked(self, uuid):
        self.unacked[uuid] = False

    def _setPrimaryIP(self, primaryIP):
        self.primaryIP = primaryIP



    def _addNewData(self, dataframe):
        new_mssg_uuid = str(uuid.uuid4())
        self._addToUnacked(new_mssg_uuid)
        self.datasets[new_mssg_uuid] = [
            self.stationID,
            dataframe[self.raw_data_structure["GTIN"]],
            dataframe[self.raw_data_structure["PRODUCT_DESCRIPTION"]],
            dataframe[self.raw_data_structure["QUANTITY"]],
            dataframe["TEST"]
        ]


    def _dynamic_discovery(self, client_start):
        if client_start == True:
            print("discovery method at starttime")
            self.discovery_timestamp = time.time()
            self.dynamic_discovery_message()
        self._discoveryIntervall()


    def _discoveryIntervall(self):
        if (float(time.time()) - float(self.discovery_timestamp)) > self.reDiscovery_timeout:
            print("re Discovery method....")
            self.discovery_timestamp = time.time()
            self.dynamic_discovery_message()


    def dynamic_discovery_message(self):
        self.dynamic_discovery_template["MSSG_UUID64"] = self.discovery_message_uuid
        self.mc_sock.sendto(str.encode(
            pipe_filter.outgoing_frame_creater(list(self.dynamic_discovery_template.values()))),
            (cfg.config["MULTICAST_GROUP"], cfg.config["MCAST_PORT"]))

    def _retryTimer(self):
        if (float(time.time()) - float(self.lastsend_timestamp)) > self.retry_timeout:
            self._reSendUnacked()



    def _reSendUnacked(self):
        if len(self.unacked) > 0:
            for mssg_uuid in self.unacked:
                if not self.unacked[mssg_uuid]:
                    self.outgoings_pipe.put(self.datasets[mssg_uuid])
                    print("SENDING UNACKED AGAIN!")
                    print(mssg_uuid)
                    print(self.datasets)
            self._setTimerRetryTimer()


    def _setTimerRetryTimer(self):
        self.lastsend_timestamp = time.time()



if __name__ == "__main__":
    if len(sys.argv) > 1:
        client = GuiClass(sys.argv[1])
        client.run()
    else:
        print("No Client defined... => define a user ID!")
        pass

