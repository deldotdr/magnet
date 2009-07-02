
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy

from misted.amqp import AMQPClientCreator
from misted.river import MessageService
from misted import fog

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor, MessageService)
    msgsrv = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
    yield msgsrv.authenticate(clientCreator.username,
            clientCreator.password)

    f = HTTPClientFactory('http://google.com')
    c = fog.connectMS('test-http-server', f, timeout=None, from_addr='test-adapter',
        reactor=reactor, msgsrv=msgsrv)



if __name__ == '__main__':
    main(reactor)
    reactor.run()
