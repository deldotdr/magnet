
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy

from misted.amqp import AMQPClientCreator
from misted import fog
from misted.hot_pocket import PocketDynamo

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class UtilClient(basic.LineReceiver):

    def connectionMade(self):
        send = 'add, 2, 3'
        # send = 'whois'
        print 'send: ', send
        self.sendLine(send)

    def lineReceived(self, line):
        print 'Result: ', line
        if line[0:3] == 'add' and int(line[3:]) < 20:
            send = 'add, 2,'+line[3:]
            self.sendLine(send)

class UtilClientFactory(protocol.ClientFactory):
    protocol = UtilClient

class MSClient(protocol.Protocol):

    def dataReceived(self, data):
        print 'dataReceived', data
        self.transport.write("Im gonna close the connection now, BYE!")
        self.transport.loseConnection()

    def connectionMade(self):
        self.transport.write('HOLA, World on a wing!')

class MSClientFactory(protocol.ClientFactory):
    protocol = MSClient

@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
    yield client.authenticate(clientCreator.username,
            clientCreator.password)

    dynamo = PocketDynamo(reactor, client)
    # f = HTTPClientFactory('http://amoeba.ucsd.edu')
    # f = MSClientFactory()
    f = UtilClientFactory()
    c = dynamo.connectMS('test-http-server', f)
    dynamo.run()



if __name__ == '__main__':
    main(reactor)
    reactor.run()
