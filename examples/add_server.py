
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy, resource, static
from twisted.python import log

from misted.amqp import AMQPClientCreator
from misted.core import PocketReactor

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672



class AddProtocol(basic.LineReceiver):

    def lineReceived(self, line):
        print self, self.transport, 'Add request: ', line
        if line[0:3] == 'add':
            _, a, b = line.split(',')
            c = self.add(int(a), int(b))
            self.sendLine(str(c))

    def add(self, a, b):
        """Simple example of specific protocol functionality
        """
        return a + b

class AddFactory(protocol.ServerFactory):
    protocol = AddProtocol



@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    f = AddFactory()

    p_reactor.listenMS(['amq.direct','add-service'], f)
    p_reactor.run()

if __name__ == '__main__':
    main(reactor)
    reactor.run()
