import socket
import config as cfg
import pipe_filter as pipe

group = cfg.config["MULTICAST_GROUP"]
port = cfg.config["MCAST_PORT"]
# 2-hop restriction in network
ttl = 2





class MessageBuilder:

    def __init__(self, process_uuid4):
        self.UUID = process_uuid4
        # dynamic discovery message socket
        self.sock = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP,
                             socket.IP_MULTICAST_TTL,
                             ttl)
        # single shot udp
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP



        self.dynamic_discovery_template = {

            "MESSAGE_TYPE": "DISCOVERY",
            "NODE_TYPE": "SERVER",
            "PROCESS_UUID64": str(self.UUID),
            "MSSG_UUID64": "",
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": "WHO IS THERE?",
        }

        self.dynamic_discovery_ack_template = {

            "MESSAGE_TYPE": "ACK",
            "NODE_TYPE": "SERVER",
            "PROCESS_UUID64": str(self.UUID),
            "MSSG_UUID64": "",
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": "I AM HERE",
        }
    # sock.sendto(b"hello world", (group, port))

    def dynamic_discovery_message(self, message_uuid):
        self.dynamic_discovery_template["MSSG_UUID64"] = str(message_uuid)
        self.sock.sendto(str.encode(pipe.outgoing_frame_creater(frame_list=list(self.dynamic_discovery_template.values()), receiver_ip=False)), (group, port))

    def ack_dynamic_discovery_message(self, ack_to_mssg, receiver):
        self.dynamic_discovery_ack_template["MSSG_UUID64"] = ack_to_mssg
        self.udp_sock.sendto(str.encode(pipe.outgoing_frame_creater(frame_list=list(self.dynamic_discovery_template.values()), receiver_ip=False)),(receiver, port))