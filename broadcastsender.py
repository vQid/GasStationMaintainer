import socket
import config as cfg

def broadcast(ip, port, broadcast_message):

    # Create a UDP socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Send message on broadcast address
    broadcast_socket.sendto(str.encode(broadcast_message), (ip, port))
    broadcast_socket.close()


if __name__ == '__main__':
    # Broadcast address and port
    BROADCAST_IP = "192.168.0.255"
    BROADCAST_PORT = 5975

    # Local host information
    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)
    print("Sending mssg from " + str(MY_IP) + ":"+str(BROADCAST_PORT))
    # Send broadcast message
    message = MY_IP + ' sent a broadcast'
    broadcast(BROADCAST_IP, BROADCAST_PORT, message)

