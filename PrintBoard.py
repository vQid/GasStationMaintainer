import threading
import time


class Cons(threading.Thread):

    def __init__(self, board, primary):
        super(Cons, self).__init__()
        self.board = board
        self.primary = primary
        self.primaryppid = ""
        self.election_pending = False



    def run(self):

        while True:
            time.sleep(2.0)
            print("I AM PRIMARY: ")
            print(self.primary)
            print("The Primary PID is: " + self.primaryppid)
            print(self.board)
            if self.election_pending:
                print("election is pending")

