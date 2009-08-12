import sys
import random

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.internet import task
from twisted.protocols import basic
from twisted.python import log


from misted.amqp import AMQPClientCreator
from misted.core import PocketReactor
from misted.protocol import ClientCreator

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_HOST = 'localhost'
BROKER_PORT = 5672

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
def main(reactor):
    # ClientCreator for AMQP client (this will disappear from view
    # eventually. Don't confuse it with the ClientCreator you (the app
    # developer) wants to use
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, p_reactor, FactorClient)
    d = client_creator.connectWorkProducer('factor')
    factor_client = yield d
    

    # l = task.LoopingCall(factor_int, factor_client)
    l = task.LoopingCall(sleep_time, factor_client)
    l.start(1)

    p_reactor.run()



if __name__ == '__main__':
    main(reactor)
    reactor.run()
