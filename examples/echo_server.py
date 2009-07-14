
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from misted.amqp import AMQPClientCreator
from misted.core import PocketReactor

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672


class EchoMessage(protocol.Protocol):

    def dataReceived(self, data):
        print 'data from client:', data
        self.transport.write(data)
        # self.transport.loseConnection()

class EchoFactory(protocol.ServerFactory):
    protocol = EchoMessage


@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    f = EchoFactory()

    p_reactor.listenMS(['amq.direct','echo-server'], f)
    p_reactor.run()

if __name__ == '__main__':
    main(reactor)
    reactor.run()
