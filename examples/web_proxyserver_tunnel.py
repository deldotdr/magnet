import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import server, proxy
from twisted.web.client import HTTPClientFactory

from magnet.amqp import AMQPClientCreator
from magnet.core import PocketReactor

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

log.startLogging(sys.stdout)

@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    f = server.Site(proxy.ReverseProxyResource('amoeba.ucsd.edu', 80, ''))

    p_reactor.listenMS(['amq.direct','test-http-server'], f)
    p_reactor.run()

if __name__ == '__main__':
    main(reactor)
    reactor.run()
