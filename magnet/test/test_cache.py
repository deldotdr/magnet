#!/usr/bin/env python

__author__='hubbard'

import logging
import magnet
from magnet import field, pole
import os
from twisted.internet import reactor, threads
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import DNSLookupError, UserError
from twisted.trial import unittest
from twisted.internet.base import DelayedCall
from twisted.trial import unittest
import time

# Spec file is loaded from the egg bundle
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

class CacheClient(pole.BasePole):
    """Magnet class corresponding to the Wallet cache controller"""

    def action_dataset_reply(self, msg):
        """This catches replies from Wallet"""
#        logging.info('Got reply. Code %s: "%s"' % (msg['return_code'], msg['payload']))
        self.rc = int(msg['return_code'])
        self.reply = msg['payload']
        self.got_reply = True

    def waitForReply(self):
        """Waits for reply or a 5 sec timeout.
        Synchronous, run in another thread."""
        st = time.time()
        while ((time.time() - st) < 5) and (self.got_reply == False):
            time.sleep(0.1)

    #######
    # internal methods
    def makeMsg(self, method, payload, returnCode=200):
        """Convenience method to create a message in the magnet style"""
        msg = dict()
        msg['method'] = method
        msg['payload'] = payload
        msg['return_code'] = str(returnCode)
        return msg

    def sendMsg(self, msg, key):
        """Convenience method - clear reply flag before sending"""
        self.got_reply = False
        self.reply = ''
        return self.sendMessage(msg, key)

class CacheTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.INFO, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        # Set a timeout for login failures and similar
        self.timeout = 10

    @inlineCallbacks
    def tearDown(self):
        try:
            yield self.connector.stopService()
        except:
            pass

    def go(self, hostName='amoeba.ucsd.edu'):
        """Main method - sets up and starts the connection et al. Returns a deferred."""
        # Create the instance
        self.cc = CacheClient(routing_pattern='dataset')
        # Adapt it to AMQP
        c = field.IAMQPClient(self.cc)
        # Create a connector
        self.connector = field.AMQPClientConnectorService(reactor, c)
        # Ask the connector to connect
        d = self.connector.connect(host=hostName, spec_path=spec_path_def)
        # startService is where connectTCP actually gets called
        self.connector.startService()
        return d


    @inlineCallbacks
    def test_commands(self):
        """Try the various actions that wallet should enact"""
        yield self.go(hostName='amoeba.ucsd.edu')

        logging.info('Connected to amoeba OK')

        logging.info('Listing all datasets')
        cmd = self.cc.makeMsg('dset_query', '*')
        yield self.cc.sendMsg(cmd, 'dataset')

        logging.info('Waiting for reply...')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            logging.info('Listing: %s' % self.cc.reply)
        else:
            logging.error('No reply from Wallet!')
            self.fail()

        # Known-missing dataset
        logging.info('Trying to query bad dataset')
        cmd = self.cc.makeMsg('dset_query', 'http://localhost:8080/bad-data.csv')
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            if self.cc.rc == 404:
                logging.info('Code 404 as expected, good')
            else:
                self.fail('Expected code 404!')
        else:
            logging.error('No reply from Wallet!')
            self.fail('No reply from Wallet')

        # Download the dataset into the local cache
        logging.info('Trying dataset download')
        cmd = self.cc.makeMsg('dset_fetch', 'http://localhost:8080/data.csv')
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            logging.info('Got reply! Code %d' % self.cc.rc)
            self.failUnlessEqual(200, self.cc.rc)
        else:
            logging.error('No reply from Wallet!')
            self.fail()

        # Verify that its there
        logging.info('Checking for newly downloaded dataset')
        cmd = self.cc.makeMsg('dset_query', 'http://localhost:8080/data.csv')
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            logging.info('Got reply! Code %d' % self.cc.rc)
            self.failUnlessEqual(200, self.cc.rc)
        else:
            logging.error('No reply from Wallet!')
            self.fail()

        logging.info('Purging downloaded dataset')
        cmd = self.cc.makeMsg('dset_purge', 'http://localhost:8080/data.csv')
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            logging.info('Got reply! Code %d' % self.cc.rc)
            self.failUnlessEqual(200, self.cc.rc)
        else:
            logging.error('No reply from Wallet!')
            self.fail()

        logging.info('Listing all datasets')
        cmd = self.cc.makeMsg('dset_query', '*')
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            logging.info('Got reply! Code %d' % self.cc.rc)
            self.failUnlessEqual(200, self.cc.rc)
        else:
            logging.error('No reply from Wallet!')
            self.fail()
