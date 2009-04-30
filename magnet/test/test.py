#!/usr/bin/env python
"""Start of a Trial-based unit test suite for Magnet. Much hardwiring and hackery."""

__author__='hubbard'
__date__ ='$Apr 27, 2009 10:15:30 AM$'

import os
import logging

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet import protocol, reactor

import magnet
from magnet import field

from NIB import NIB

# Spec file is loaded from the egg bundle
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

class NIBTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        # Set a timeout for login failures and similar
        self.timeout = 10

    @inlineCallbacks
    def tearDown(self):
        yield self.connector.stopService()

    @inlineCallbacks
    def go(self, hostName='amoeba.ucsd.edu'):
        # Create the instance
        self.nib = NIB()
        # Adapt it to AMQP
        c = field.IAMQPClient(self.nib)
        # Create a connector
        self.connector = field.AMQPClientConnectorService(reactor, c)
        # Ask the connector to connect
        d = self.connector.connect(host=hostName, spec_path=spec_path_def)
        # startService is where connectTCP actually gets called
        self.connector.startService()
        dd = yield d

    @inlineCallbacks
    def test_bad_address(self):
        yield self.go(hostName='bad.example')
        assert self.nib.got_err == True
        
    @inlineCallbacks
    def test_snd_rcv(self):
        yield self.go(hostName='amoeba.ucsd.edu')
        yield self.nib.pingPong(msg='For the lulz')
        assert self.nib.got_ack == True
        assert self.nib.got_err == False
