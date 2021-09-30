import threading
import time


class Cons(threading.Thread):

    def __init__(self, board):
        super(Cons, self).__init__()

        self.board = board



    def run(self):

        while True:
            time.sleep(2.5)
            print(self.board)
