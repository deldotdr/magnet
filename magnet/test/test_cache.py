#!/usr/bin/env python
"""Unit test code for the Wallet cache controller.

Requires:
 - Redis on amoeba
 - Rabbit on amoeba
 - DAP server on localhost:8080 with sample.csv

"""
__author__='hubbard'

import logging
import magnet
from magnet import field, pole
import os
from twisted.internet import reactor, threads
from twisted.internet.defer import inlineCallbacks, Deferred, maybeDeferred
from twisted.internet import defer
from twisted.internet.task import LoopingCall
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
        logging.debug('Sending message')
        return maybeDeferred(self.sendMessage, msg, key)

    def sendRcv(self, msg, key):
        """Uses loopingCall to create a RPC pattern, returns a deferred"""
        def checkTimeout(endTime):
            """This is to be run as a looping call"""
            logging.debug('Checking timeout')
            if time.time() >= endTime:
                if not self.got_reply:
                    logging.info("Receive timed out, firing errback")
                    raise Exception('Timeout')
            if self.got_reply:
                logging.debug("Receive succeeded")
                self.lc.stop()
                return self.reply
            logging.debug('Not timeout, no message yet')    
            
        def runLCall(args):
            """Convenience inner routine to setup and run the loopingCall"""
            # Expect a response in 2 seconds or less
            endTime = time.time() + 2.0
            logging.debug("Starting looping call")
            self.lc = LoopingCall(checkTimeout, endTime)
            # Start it running, returns a deferred
            d = self.lc.start(0.1)
            return d

        # Chain looping call to run once send returns
        d = self.sendMsg(msg, key).addCallback(runLCall)
        return d        

class CacheTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.WARNING, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

        # Set a timeout for login failures and similar
        self.timeout = 20

    @inlineCallbacks
    def tearDown(self):
        try:
            yield self.connector.stopService()
        except:
            pass

    def go(self, hostName='localhost'):
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
    def test_send_receive(self):
        """Try new looping call"""
        host = 'localhost'
        yield self.go(hostName=host)
        
        logging.debug('Connected to exchange OK')

        # Try a dataset query
        logging.debug('Listing all datasets')
        cmd = self.cc.makeMsg('dset_query', '*')
        d =  self.cc.sendRcv(cmd, 'dataset').addErrback(self.fail)
        d.addCallback(logging.debug)
        yield d
        if self.cc.got_reply == True:
            # May be empty, so just roll with it
            pass
        else:
            logging.error('No reply from Wallet!')
            self.fail('Wallet timeout')
        
    @inlineCallbacks
    def test_bad_routing_key(self):
        """Force a timeout"""
        host = 'localhost'
        yield self.go(hostName=host)
        
        # Test sending to wrong routing key
        try:
            # Note that callback, if fired, will fail the test
            d = self.cc.sendRcv(cmd, 'Not-dataset').addCallback(lambda _: self.fail)
            yield d
            self.fail('Expected an error on non-existent routing key')
        except Exception:
            logging.debug('OK, exception as expected on bogus routing key')
            pass

    @inlineCallbacks
    def test_cache_listing(self):
        """Try simple list-all"""
        host = 'localhost'
        yield self.go(hostName=host)

        logging.debug('Connected to exchange OK')

        logging.debug('Listing all datasets')
        cmd = self.cc.makeMsg('dset_query', '*')
        d=  self.cc.sendRcv(cmd, 'dataset').addErrback(self.fail)
        d.addCallback(logging.debug)
        yield d
        if self.cc.got_reply == True:
            # May be empty, so just roll with it
            pass
        else:
            logging.error('No reply from Wallet!')
            self.fail('Wallet timeout')

    @inlineCallbacks
    def test_cache_lifecycle(self):
        """Try simple download/query/purge from local DAP server"""
        host = 'localhost'
        dset = 'http://localhost:8080/sample.csv'
        yield self.go(hostName=host)
        logging.debug('Connected to amoeba OK')

        cmd = self.cc.makeMsg('dset_fetch', dset)
        yield self.cc.sendMsg(cmd, 'dataset')

        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            logging.debug('Dataset cached successfully')
        else:
            logging.error('No reply from Wallet!')
            self.fail()

        # Verify presence in directory
        cmd = self.cc.makeMsg('dset_query', dset)
        yield self.cc.sendMsg(cmd, 'dataset')

        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            if self.cc.rc == 200:
                logging.debug('Dataset queried successfully')
            else:
                self.fail('Query after fetch failed')
        else:
            logging.error('No reply from Wallet!')
            self.fail('Wallet timeout')

        # Remove it
        logging.debug('Purging downloaded dataset')
        cmd = self.cc.makeMsg('dset_purge', dset)
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            self.failUnlessEqual(200, self.cc.rc)
        else:
            logging.error('No reply from Wallet!')
            self.fail()

        # Verify that purge succeeded
        logging.debug('verifying purge')
        cmd = self.cc.makeMsg('dset_query', dset)
        yield self.cc.sendMsg(cmd, 'dataset')
        yield threads.deferToThread(self.cc.waitForReply)
        if self.cc.got_reply == True:
            self.failUnlessEqual(404, self.cc.rc)
        else:
            logging.error('No reply from Wallet!')
            self.fail('Wallet timeout')
