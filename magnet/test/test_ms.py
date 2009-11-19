import sys

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet import defer 
from twisted.protocols import wire
from twisted.internet import protocol
from twisted.python import log

from magnet.preactor import Preactor
from magnet.protocol import ClientCreator
from magnet.protocol import RequestResponseLineReceiver

# log.startLogging(sys.stdout)

class MessagePatternTests(unittest.TestCase):

    @defer.inlineCallbacks
    def setUp(self):
        self.preactor = yield Preactor()

    def tearDown(self):
        self.preactor.stop()


    @defer.inlineCallbacks
    def test_client_server(self):
        test_name = 'magnet-test-client-server'

        server_factory = protocol.ServerFactory()
        server_factory.protocol = wire.Echo
        self.preactor.listenMS(test_name, server_factory)

        client_creator = ClientCreator(reactor, self.preactor, RequestResponseLineReceiver)
        client = yield client_creator.connectMS(test_name)

        response = yield client.makeRequest('Is there an echo in here?')
        self.assertEqual('Is there an echo in here?', response)


