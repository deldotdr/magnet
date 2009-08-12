
from twisted.trial import unittest
from twisted.internet import defer
from twisted.internet import reactor

from magnet.amqp import AMQPClientFactory
from magnet.amqp import AMQPClientCreator
from magnet.river import MessageService

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class AMQPClientFactoryTest(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        clientCreator = AMQPClientCreator(reactor, MessageService)
        client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
