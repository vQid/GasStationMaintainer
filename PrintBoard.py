import threading
import time


class Cons(threading.Thread):

    def __init__(self, board, primary, datasets, clock):
        super(Cons, self).__init__()
        self.board = board
        self.primary = primary
        self.primaryppid = ""
        self.election_pending = False
        self.clock = clock
        self.dataset = datasets

    def run(self):

        while True:
            time.sleep(4.0)
            print("I AM PRIMARY: ")
            print(self.primary)
            print("The Primary PID is: " + self.primaryppid)
            print(self.board)
            if len(self.dataset) > 0:
                print("MY CLOCK IS:")
                print(self.clock)
                print(self.dataset)

            if self.election_pending:
                print("election is pending")
