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

class FactorClient(basic.LineReceiver):
    def __init__(self):
        self.count = 0

    def factor(self, n):
        to_send = str(n)
        self.sendLine(to_send)

    def sleep(self, t):
        to_send = str(t)
        self.sendLine(to_send)

    def lineReceived(self, line):
        print 'Received: ', line


def factor_int(client, order=155):
    n = random.randint(2**order, 2**(order+2))
    client.factor(n)

def sleep_time(client):
    t = random.randint(1, 5)
    client.sleep(t)


@inlineCallbacks
def main():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, preactor, FactorClient)
    d = client_creator.connectWorkProducer('work')
    factor_client = yield d
    

    # l = task.LoopingCall(factor_int, factor_client)
    l = task.LoopingCall(sleep_time, factor_client)
    l.start(1)

    preactor.run()



application = service.Application('workproducer')
main()
