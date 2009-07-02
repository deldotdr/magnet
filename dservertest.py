
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy, resource, static
from twisted.python import log

from misted.amqp import AMQPClientCreator
from misted.river import MessageService
from misted import fog

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class Root(resource.Resource):
    """
    """

@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor, MessageService)
    msgsrv = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
    yield msgsrv.authenticate(clientCreator.username,
            clientCreator.password)

    root = static.File('.')
    # site = server.Site(proxy.ReverseProxyResource('google.com', 80, ''))
    site = server.Site(root)
    fog.listenMS('test-http-server', site, reactor, msgsrv)

if __name__ == '__main__':
    main(reactor)
    reactor.run()
