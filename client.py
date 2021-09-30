import threading


class GasStationClient(threading.Thread):

    def __init__(self):
        super(GasStationClient, self).__init__()
        self.primary_IP = ""


    def run(self):
        while True:
            print("Enter any command or data input source...")
            print("Enter <\"SOURCE\";C:\\example...> to define the source of datas...")
            command_or_raw_data = input()
            if command_or_raw_data == command_or_raw_data.split(";")[0] == "SOURCE":
                pass
            else:
                if self._checkInputValidity(command_or_raw_data):
                    pass

    #bool
    def _checkInputValidity(self, input):
        #regex possible
        print("method" + str(self._checkInputValidity))
        pass

if __name__ == "__main__":
    client = GasStationClient()
    client.run()
