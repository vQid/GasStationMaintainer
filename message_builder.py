import socket
import pipe_filter as pipe
import config as cfg

group = cfg.config["MULTICAST_GROUP"]
port_mc = cfg.config["MCAST_PORT"]
port_udp_unicast = cfg.config["UDP_SOCKET_PORT"]
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

        self.dynamic_client_discovery_ack_template = {
            "MESSAGE_TYPE": "ACK",
            "NODE_TYPE": "SERVER",
            "PROCESS_UUID64": "",
            "MSSG_UUID64": "",
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": "I AM THE PRIMARY SERVER",
        }

        self.client_transmission_ack_template = {
            "MESSAGE_TYPE": "ACK",
            "NODE_TYPE": "SERVER",
            "PROCESS_UUID64": "",
            "MSSG_UUID64": "",
            "LOGICAL_CLOCK": "",
            "PHYSICAL_CLOCK": "",
            "STATEMENT": "MESSAGE CONFIRMATION",
        }

    def dynamic_discovery_message(self, message_uuid, current_clock):
        self.dynamic_discovery_template["MSSG_UUID64"] = str(message_uuid)
        self.dynamic_discovery_template["LOGICAL_CLOCK"] = current_clock
        self.sock.sendto(str.encode(
            pipe.outgoing_frame_creater(list(self.dynamic_discovery_template.values()))),
            (group, port_mc))

    def multicast_hearbeat(self, list_frame):
        self.sock.sendto(str.encode(
            pipe.outgoing_frame_creater(list_frame)), (group, port_mc))

    def ack_dynamic_discovery_message(self, ack_to_mssg, receiver):
        self.dynamic_discovery_ack_template["MSSG_UUID64"] = ack_to_mssg
        self.udp_sock.sendto(str.encode(
            pipe.outgoing_frame_creater(list(self.dynamic_discovery_ack_template.values()), )),
            (receiver, port_udp_unicast))

    def ack_client_dynamic_discovery_message(self, ack_to_mssg, receiver):
        self.dynamic_client_discovery_ack_template["MSSG_UUID64"] = ack_to_mssg
        self.udp_sock.sendto(str.encode(
            pipe.outgoing_frame_creater(list(self.dynamic_client_discovery_ack_template.values()), )),
            (receiver, cfg.config["STATION_CLIENT_PORT"]))

    def client_transmission_ack_message(self, ack_to_mssg, receiver):
        self.client_transmission_ack_template["MSSG_UUID64"] = ack_to_mssg
        self.udp_sock.sendto(str.encode(
            pipe.outgoing_frame_creater(list(self.client_transmission_ack_template.values()), )),
            (receiver, cfg.config["STATION_CLIENT_PORT"]))

    def election_mssg(self, frame_list, receiver):  # unicast
        self.udp_sock.sendto(str.encode(
            pipe.outgoing_frame_creater(frame_list)), (receiver, port_udp_unicast))

    def ack_election_mssg(self, frame_list, receiver):  # unicast
        self.udp_sock.sendto(str.encode(
            pipe.outgoing_frame_creater(frame_list)), (receiver, port_udp_unicast))

    def coordinator_mssg(self, frame_list):  # multicast
        self.sock.sendto(str.encode(
            pipe.outgoing_frame_creater(frame_list)), (group, port_mc))
