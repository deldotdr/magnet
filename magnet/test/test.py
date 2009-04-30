#!/usr/bin/env python
"""Start of a Trial-based unit test suite for Magnet. Much hardwiring and hackery."""

__author__='hubbard'
__date__ ='$Apr 27, 2009 10:15:30 AM$'

from NIB import NIB
import logging
import magnet
from magnet import field
import os
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import DNSLookupError
from twisted.trial import unittest

# Spec file is loaded from the egg bundle
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

class NIBTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.ERROR, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        # Set a timeout for login failures and similar
        self.timeout = 10

    @inlineCallbacks
    def tearDown(self):
        try:
            yield self.connector.stopService()
        except:
            pass

 #   @inlineCallbacks
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
#        dd = yield d
        return d

    @inlineCallbacks
    def test_pingPong(self):
        """Send and receive basic messages using Magnet"""
        yield self.go(hostName='amoeba.ucsd.edu')
        yield self.nib.pingPong()
        self.failUnless(self.nib.got_ack == True)
        self.failUnless(self.nib.got_err == False)

    @inlineCallbacks
    def test_say_pingPong(self):
        """Send a say command, wait for ack"""
        yield self.go(hostName='amoeba.ucsd.edu')
        yield self.nib.sayPingPong(msg='For the lulz')
        self.failUnless(self.nib.got_ack == True)
        self.failUnless(self.nib.got_err == False)

#    @inlineCallbacks
#    def test_bad_address(self):
#        yield self.go(hostName='bad.example')
#        d = self.go('bad.example')
#        self.failUnlessFailure(d, DNSLookupError)
