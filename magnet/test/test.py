#!/usr/bin/env python
"""Start of a Trial-based unit test suite for Magnet. Much hardwiring and hackery."""

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

        # Set a timeout for login failures and similar
        self.timeout = 10

    @inlineCallbacks
    def tearDown(self):
        yield self.connector.stopService()

    @inlineCallbacks
    def test_snd_rcv(self):
        yield nibc.pingPong()
        assert nibc.got_ack == True
        assert nibc.got_err == False
