

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from twisted.web.client import HTTPClientFactory
from twisted.internet.base import DelayedCall

DelayedCall.debug = True

from txamqp.client import Closed
from txamqp.queue import Empty
from txamqp.content import Content

from magnet.amqp import AMQPClientCreator
from magnet.river import MessageService
from magnet import fog

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class MSTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @inlineCallbacks
    def test_httpclient(self):
        clientCreator = AMQPClientCreator(reactor, MessageService)
        msgsrv = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
        yield msgsrv.authenticate(clientCreator.username,
                clientCreator.password)

        f = HTTPClientFactory('http://google.com')
        c = fog.connectMS('test-http-server', f, timeout=None, from_addr='test-adapter',
            reactor=reactor, msgsrv=msgsrv)




