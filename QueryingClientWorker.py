import threading
import time
import csv


class QCWorker(threading.Thread):

    def __init__(self, process_queue):
        super(QCWorker, self).__init__()
        self.process_queue = process_queue
        self._registred_mssgs = []

        # key = QueryClient Process ID |||||| Value = [timestamp, {"clock": "[0,(input)]"}]
        self.query_pool = {

        }

    def run(self):

        while True:
            self._monitorProcessTime()
            if not self.process_queue.empty():
                self._handleFrame()

    def _handleFrame(self):
        data_list = self.process_queue.get()
        print(data_list)
        if self._newMessage(data_list[3]):
            print("A NEW MESSAGE!")
            statement = data_list[6].split("&")
            placeFrame = {
                "CLOCK": statement[5],
                "GAS_STATION": statement[3],
                "GTIN": statement[0],
                "DESCRIPTION": statement[1],
                "QUANTITY": statement[2],
                "EVENT_TIMESTAMP": statement[6]
            }
            self._insertFrameIntoQueryProces(data_list=data_list, placeFrame=list(placeFrame.values()))

    def _insertFrameIntoQueryProces(self, data_list, placeFrame):
        processID = data_list[2]
        if processID in self.query_pool:
            clock = placeFrame[0]
            if clock in self.query_pool[processID][1]:
                match_found = False
                for frame in self.query_pool[processID][1][clock]:
                    new_source = tuple(placeFrame)
                    if new_source == frame[1]:
                        frame[0] = frame[0] + 1
                        match_found = True
                        self.query_pool[processID][1][clock] = sorted(self.query_pool[processID][1][clock], reverse=True)

                if not match_found:
                    self.query_pool[data_list[2]][1][clock].append([1, tuple(placeFrame)])
            else:
                self.query_pool[processID][1][clock] = []
                self.query_pool[processID][1][clock].append([1, tuple(placeFrame)])
            print(self.query_pool)
        else:
            pass

    def addNewQueryProcess(self, processID):
        timestamp = float(time.time())
        if processID not in self.query_pool:
            # key = QueryClient Process ID |||||| Value = [timestamp, {"clock": "[[0,(input)], ...]}]
            self.query_pool[processID] = [timestamp, {}]

    def _monitorProcessTime(self):
        if len(self.query_pool) > 0:
            k = self.query_pool.copy()
            for processindex in k:
                if (float(time.time()) - float(self.query_pool[processindex][0])) > 6:
                    print("A PROCESS IS DONE!")
                    print(processindex)
                    self._completeQueryProcess(processindex)


    def _completeQueryProcess(self, processID):
        path = "./queries/" + processID + ".csv"
        header = ["VOTES", "CLOCK", "GAS_STATION", "GAS_STATION", "DESCRIPTION", "QUANTITY", "EVENT_TIMESTAMP"]
        data = []
        if len(self.query_pool[processID][1]) > 0:
            for index in self.query_pool[processID][1]:
                frame = self.query_pool[processID][1][str(index)][0]
                votes = [frame[0]]
                full_frame = votes + list(frame[1])
                data.append(full_frame)

            with open(path, 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                # write the header
                writer.writerow(header)
                # write multiple rows
                writer.writerows(data)
        del self.query_pool[processID]



    def _newMessage(self, uuid):
        if uuid in self._registred_mssgs:
            return False
        else:
            return True

