#!/usr/bin/env python
"""Start of a Trial-based unit test suite for Magnet."""

__author__='hubbard'
__date__ ='$Apr 27, 2009 10:15:30 AM$'
import os
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from NIB import NIBConnector
from magnet import field
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet import protocol, reactor

import magnet
# Spec file is loaded from the egg bundle
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')
import logging

nibc = NIBConnector()

class NIBConnectorTest(unittest.TestCase):
    @inlineCallbacks
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        # self.nibc = NIBConnector()
        c = field.IAMQPClient(nibc)
        self.connector = field.AMQPClientConnectorService(reactor, c)
        d = self.connector.connect(host='amoeba.ucsd.edu', spec_path=spec_path_def)
        self.connector.startService()
        dd = yield d

        # Set a five-second timeout for login failures and similar
        self.timeout = 5


    @inlineCallbacks
    def tearDown(self):
        yield self.connector.stopService()

    # Tests!
    @inlineCallbacks
    def test_connect(self):
        # yield self.nibc.connect()
        yield nibc.connect()

    @inlineCallbacks
    def test_snd_rcv(self):
        yield nibc.connect()
        yield nibc.pingPong()
