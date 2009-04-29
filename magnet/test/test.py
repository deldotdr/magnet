#!/usr/bin/env python
"""Start of a Trial-based unit test suite for Magnet."""

__author__='hubbard'
__date__ ='$Apr 27, 2009 10:15:30 AM$'

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from NIB import NIBConnector
from magnet import field
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet import protocol, reactor

import logging

class NIBConnectorTest(unittest.TestCase):
#    @inlineCallbacks
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        self.nibc = NIBConnector()
        c = field.IAMQPClient(self.nibc)
        self.connector = field.AMQPClientConnectorService(reactor, c)
        logging.info('connecting')
        d = self.connector.connect(host='amoeba.ucsd.edu', spec_path='/Users/hubbard/code/basicAmqp/amqp0-8.xml')
        d.addCallback(self.connector.startService)
#        logging.info('starting service')
#        self.connector.startService()

        # Set a five-second timeout for login failures and similar
        self.timeout = 5

        logging.info('Setup complete')
        return d

    @inlineCallbacks
    def tearDown(self):
        yield self.nibc.disconnect()
        yield self.nibc.stopService()

    # Tests!
    @inlineCallbacks
    def test_connect(self):
        yield self.nibc.connect()

    @inlineCallbacks
    def test_snd_rcv(self):
        yield self.nibc.connect()
        yield self.nibc.pingPong()
