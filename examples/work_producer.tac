import sys
import random

from twisted.application import service
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.internet import task
from twisted.protocols import basic
from twisted.python import log


from magnet.protocol import ClientCreator


log.startLogging(sys.stdout)

class WorkClient(basic.LineReceiver):
    def __init__(self):
        self.count = 0

    def do_work(self, t):
        to_send = str(t)
        self.sendLine(to_send)

    def lineReceived(self, line):
        print 'Received: ', line


def sleep_time(client):
    t = random.randint(1, 5)
    client.do_work(t)


@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, preactor, WorkClient)
    d = client_creator.connectWorkProducer('work')
    work_client = yield d
    
    l = task.LoopingCall(sleep_time, work_client)
    l.start(1)

    preactor.run()



application = service.Application('workproducer')
main()
