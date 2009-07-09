
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy, resource, static
from twisted.python import log

from misted.amqp import AMQPClientCreator
from misted import fog
from misted.hot_pocket import PocketDynamo

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class Root(resource.Resource):
    """
    """

    def getChild(self, name, request):
        print 'get child %%%%%%%'
        if name == '':
            return self

    def render_GET(self, request):
        print 'render GET # # # # # # '

        return 'Heelllllllllllooooooo'

class MessageEcho(protocol.Protocol):

    def dataReceived(self, data):
        print 'dataReceived', data
        self.transport.write(data)

class MSEchoFactory(protocol.ServerFactory):
    protocol = MessageEcho


@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
    yield client.authenticate(clientCreator.username,
            clientCreator.password)

    dynamo = PocketDynamo(reactor, client)

    # root = static.File('.')
    root = Root()
    # site = server.Site(proxy.ReverseProxyResource('amoeba.ucsd.edu', 80, ''))
    # site = server.Site(root)
    f = MSEchoFactory()
    dynamo.listenMS(['amq.direct','test-http-server'], f)
    dynamo.run()

if __name__ == '__main__':
    main(reactor)
    reactor.run()
