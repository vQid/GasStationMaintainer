import datetime
import threading
import uuid
from queue import Queue
from worker import WorkerClass
import config as cfg
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

        self.clientsocket_thread = ClientSocket(self.stationID, self.incomings_pipe, cfg.config["STATION_CLIENT_PORT"])
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

    # bool
    def _checkInputValidity(self, input):
        # regex possible
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





if __name__ == "__main__":
    if len(sys.argv) > 1:
        client = GuiClass(sys.argv[1])
        client.run()
    else:
        print("No Client defined... => define a user ID!")
        pass
