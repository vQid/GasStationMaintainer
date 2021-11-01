import threading
from queue import PriorityQueue
import socket
import pipe_filter
import config as cfg


class PersistenceMessaging(threading.Thread):

    def __init__(self, node_name, bos, p_uuid):
        super(PersistenceMessaging, self).__init__()
        self.ProcessUUID = p_uuid
        # for reading the correct csv-file (local data)
        self.node_naming = node_name
        # amount of malicious processes tolerated:
        self.faulty = 1
        self.BOARD_OF_SERVERS = bos
        self.ttl = 2
        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP,
                             socket.IP_MULTICAST_TTL,
                             self.ttl)

        self.state_clock = 0

        # clock = key ||| VALUE = List of Lists(datasets)
        self.state_clock_relation = {

        }
        # message_uuid = key || Value == clock-nr
        self.clock_mssguuid_relation = {

        }


        self.incomings_pipe = PriorityQueue()
        self.outgoings_pipe = PriorityQueue()

    def run(self):
        try:
            while True:
                self._sendMCMessage()
                if not self.incomings_pipe.empty():
                    data_list = list(self.incomings_pipe.get())
                    self._handle_message(data_list)
        except Exception as e:
            print(e)
        finally:
            pass

    def _sendMCMessage(self):
        if not self.outgoings_pipe.empty():
            data_list = self.outgoings_pipe.get()
            print("SENDING THIS NOW!")
            print(list(data_list))
            self.sock.sendto(str.encode(pipe_filter.outgoing_frame_creater(data_list)), (cfg.config["MULTICAST_GROUP"], cfg.config["MCAST_PORT"]))



    def _handle_message(self, data_list):
        if data_list[0] == "UPDATE":
            # initiate replication
            self._leadOM(data_list)
        if data_list[0] == "REPLICATION":
            self._oralMessage(data_list)




    def _oralMessage(self, data_list):
        # data_list[6] = oralMessage(vi, Dest, List, f) joined by a logical "&"
        unwrapped_statement = data_list[6].split("&")
        # |0 = GTIN|1 = Product Description| 2 = Quantitiy | 3 = Gas Station ID |
        # |4 = message uuid | 5 = Logical Clock | 6 = Physical Time transmission made on gas station application|
        vi = unwrapped_statement[0].split(",")
        DEST = unwrapped_statement[1].split("/")
        LIST = unwrapped_statement[2].split(".")
        gas_station_id = vi[3]
        message_uuid = vi[4]
        logical_clock = vi[5]
        physical_clock = vi[6]


        LIST.insert(0, str(self.ProcessUUID))
        faulty_int = int(unwrapped_statement[3])
        f = faulty_int - 1

        statement_tuple = tuple(vi)

        if logical_clock not in self.state_clock_relation:
            self.state_clock_relation[logical_clock] = []
            self.clock_mssguuid_relation[data_list[3]] = tuple(logical_clock)

        if logical_clock in self.state_clock_relation:
            match_found = False
            for value_source in self.state_clock_relation[logical_clock]:
                if value_source[1] == statement_tuple:
                    match_found = True
                    value_source[0] = value_source[0] + 1
            if not match_found:
                # new source
                self.state_clock_relation[logical_clock].append([1, statement_tuple])
            # sort new sources by first initial [[0, (...)],[2, (...)]] after adding one as
            self.state_clock_relation[logical_clock] = sorted(self.state_clock_relation[logical_clock], reverse=True)


        if faulty_int > 0:
            vi_out = ",".join(vi)
            DESTS_out = "/".join(self.BOARD_OF_SERVERS["ServerNodes"])  # see above
            List_out = ".".join(LIST)
            oral_message_string = "&".join([vi_out, DESTS_out, List_out, str(f)])
            template = {
                "MESSAGE_TYPE": "REPLICATION",
                "NODE_TYPE": "SERVER",
                "PROCESS_UUID64": str(self.ProcessUUID),
                "MSSG_UUID64": message_uuid,
                "LOGICAL_CLOCK": logical_clock,
                "PHYSICAL_CLOCK": physical_clock,
                "STATEMENT": oral_message_string
            }
            # wrap and put into outgoings queue...
            self.outgoings_pipe.put(template.values())
        if self.state_clock < int(logical_clock):
            self.state_clock = int(logical_clock)


    def _leadOM(self, data_list):
        print("INSIDE LEAD OM METHOD NOW!")
        self.state_clock = self.state_clock + 1
        st_cl = str(self.state_clock)
        self.state_clock_relation[st_cl] = []
        self.clock_mssguuid_relation[data_list[3]] = tuple(st_cl)
        statement_tuple = data_list[6].split(",")
        statement_tuple.append(data_list[2])  # Process ID => Gas Station ID <TXXXX>
        statement_tuple.append(data_list[3]) # message_uuid
        statement_tuple.append(st_cl)  # Logical Clock
        statement_tuple.append(data_list[5])  # Physical Clock
        statement_tuple = tuple(statement_tuple)

        # new source
        self.state_clock_relation[st_cl].append([1, statement_tuple])
        vi = ",".join(statement_tuple)
        Dests = "/".join(self.BOARD_OF_SERVERS["ServerNodes"])
        List = str(self.ProcessUUID)
        oral_message_string = "&".join([vi, Dests, List, str(self.faulty)])
        print("PRINTING ORAL MESSAGE STRING!")
        print("PRINTING ORAL MESSAGE STRING!")
        print("PRINTING ORAL MESSAGE STRING!")
        print("PRINTING ORAL MESSAGE STRING!")
        print(oral_message_string)
        template = {
            "MESSAGE_TYPE": "REPLICATION",
            "NODE_TYPE": "SERVER",
            "PROCESS_UUID64": str(self.ProcessUUID),
            "MSSG_UUID64": data_list[3],
            "LOGICAL_CLOCK": st_cl,
            "PHYSICAL_CLOCK": data_list[5],
            "STATEMENT": oral_message_string
        }

        # wrap and put into outgoings queue...
        print("REPLICATION MESSAGE IS OUT!")
        print("REPLICATION MESSAGE IS OUT!")
        print("REPLICATION MESSAGE IS OUT!")
        print("REPLICATION MESSAGE IS OUT!")
        print("REPLICATION MESSAGE IS OUT!")
        print(template.values())
        self.outgoings_pipe.put(template.values())



    def read_local_data(self):
        pass
