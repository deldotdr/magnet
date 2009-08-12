import sys

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

log.startLogging(sys.stdout)

from twisted.web.client import HTTPClientFactory

from magnet.amqp import AMQPClientCreator
from magnet.core import PocketReactor

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672



def show_page(page):
    print 'Page received'
    print page[0:100]
    print '...'
    print page[-100:]
    f = open('test.html', 'w')
    f.write(page)
    f.close()


@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    f = HTTPClientFactory('http://amoeba.ucsd.edu')
    f.deferred.addCallback(show_page)

    p_reactor.connectMS('test-http-server', f)
    p_reactor.run()



if __name__ == '__main__':
    main(reactor)
    reactor.run()
