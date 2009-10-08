

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from twisted.web.client import HTTPClientFactory
from twisted.internet.base import DelayedCall

DelayedCall.debug = True

from magnet.amqp import AMQPClientCreator

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class MSTests(unittest.TestCase):

    def setUp(self):
        self.timeout = 2
        pass

    def tearDown(self):
        pass


