# MESSAGE FRAME BUILT
# MESSAGE_TYPE | NODE_TYPE | PROCESS_UUID64 | MSSG_UUID64 | LOGICAL_CLOCK | PHYSICAL_CLOCK| STATEMENT | SENDER |
#       0      |      1    |        2       |       3     |         4     |       5       |     6     |    7   |
#_______________________________________________________________________________________________________________

def incoming_frame_filter(frame, sender_ip):
    framepositions = frame.split(";")
    framepositions.append(sender_ip)
    return framepositions

def outgoing_frame_creater(frame_list):
    frame = ";".join(frame_list)
    return frame
