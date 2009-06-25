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
from twisted.internet.error import DNSLookupError, UserError
from twisted.trial import unittest
from twisted.internet.base import DelayedCall

# Spec file is loaded from the egg bundle
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

class NIBTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.ERROR, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        # Set a timeout for login failures and similar
        self.timeout = 10

#        DelayedCall.debug = True

    @inlineCallbacks
    def tearDown(self):
        try:
            yield self.connector.stopService()
        except:
            pass

    def go(self, hostName='localhost'):
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
        return d

    @inlineCallbacks
    def test_pingPong(self):
        """Send and receive basic messages using Magnet"""
        yield self.go()
        # Send/receive 10 times
        for x in range(1,11):
            yield self.nib.pingPong()
            self.failUnless(self.nib.got_ack == True)
            self.failUnless(self.nib.got_err == False)

 #   @inlineCallbacks
 #   def test_bad_address(self):
 #       d = self.go(hostName='bad.example')
 #       self.failUnlessFailure(d,UserError)
