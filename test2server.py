import socket
import config as cfg

MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)

udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
udp_sock.bind(("", cfg.config["UDP_SOCKET_PORT"]))
print("my udp socket port is " + str(cfg.config["UDP_SOCKET_PORT"]))
print(MY_IP)

while True:
    print("inside incomings udp pipe thread")
    data, addr = udp_sock.recvfrom(1024)
    if data:
        print("Received broadcast message:", data.decode())


