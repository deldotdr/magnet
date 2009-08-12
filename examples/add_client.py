import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.python import log
from twisted.protocols import basic


from magnet.amqp import AMQPClientCreator
from magnet.core import PocketReactor
from magnet.protocol import ClientCreator

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

log.startLogging(sys.stdout)

class AddClient(basic.LineReceiver):

    def add(self, a, b):
        to_send = 'add, %d, %d' % (a, b)
        self.sendLine(to_send)

    def lineReceived(self, line):
        print 'Result: ', line



@inlineCallbacks
def main(reactor):
    # ClientCreator for AMQP client (this will disappear from view
    # eventually. Don't confuse it with the ClientCreator you (the app
    # developer) wants to use
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    # ClientCreator for connectMS
    client_creator = ClientCreator(reactor, p_reactor, AddClient)
    d = client_creator.connectMS('add-service')
    add_client = yield d
    
    # use client
    add_client.add(2, 2)
    add_client.add(23, 2)
    add_client.add(1000, 99999)

    p_reactor.run()



if __name__ == '__main__':
    main(reactor)
    reactor.run()
