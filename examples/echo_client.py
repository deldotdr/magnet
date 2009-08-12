import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.python import log

log.startLogging(sys.stdout)

from magnet.amqp import AMQPClientCreator
from magnet.core import PocketReactor

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

# log.startLogging(log.StdioOnnaStick())

class MSClient(protocol.Protocol):

    def dataReceived(self, data):
        print 'echo from server:', data
        # self.transport.loseConnection()

    def connectionMade(self):
        self.transport.writeSequence('Hello, world!')

class MSClientFactory(protocol.ClientFactory):
    protocol = MSClient

@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    f = MSClientFactory()
    p_reactor.connectMS('echo-server', f)
    p_reactor.run()



if __name__ == '__main__':
    main(reactor)
    reactor.run()
