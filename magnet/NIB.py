#!/usr/bin/env python

__author__='hubbard'
__date__ ='$Apr 27, 2009 10:15:30 AM$'


from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet import protocol, reactor, threads
import logging
from magnet import pole, field
from twisted.internet.utils import getProcessOutput
import time

class MyError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class NIB(pole.BasePole):
    """NIB is a type of magnet (http://en.wikipedia.org/wiki/Neodymium_magnet) and NIB.py
       is the pair class file for test.py, providing scaffolding that the test uses.

       See pingPong method.
       """

    def action_reply(self, message_object):
        """Triggered by replies to say, via Magnet"""
        logging.info('Got reply message: %s' % message_object['payload'])
        self.got_ack = True
        return None

    def action_ping(self, message_object):
        """Triggered by ping messages, via Magnet"""
        logging.debug('Got ping message')
        self.sendOK(None)
        return None

    def action_say(self, message_object):
        """Triggered by say messages, via Magnet"""
        to_say = message_object['payload']
        logging.debug('Got say message: %s' % message_object['payload'])
        d = getProcessOutput('/usr/bin/say', ['-v', 'pipe organ', to_say])
        d.addCallback(self.sendOK).addErrback(self.sendError)
        return None

    def sendOK(self, result):
        """Callback after say received, sends OK ack"""
        reply = {'method': 'reply', 'payload': 'ok'}
        logging.info('Sending back an OK')
        self.sendMessage(reply, 'test')

    def sendError(self, failure):
        """Errback after say received, sends back app error"""
        self.got_err = True
        reply = {'method': 'reply', 'payload': 'got an error running say'}
        logging.error('Sending back an error message')
        self.sendMessage(reply, 'test')


    def doSendSay(self, msgString):
        """Sends a say message into the exchange, initiates the handshake."""
        self.got_ack = False
        self.got_err = False
        smsg = {'method': 'say', 'payload': msgString}
        logging.info('Sending say message')
        self.sendMessage(smsg, 'test')

    def doSendPing(self, msgString):
        """Sends a ping message into the exchange, initiates the handshake."""
        self.got_ack = False
        self.got_err = False
        smsg = {'method': 'ping', 'payload': msgString}
        logging.info('Sending ping message')
        self.sendMessage(smsg, 'test')

    def waitForPingPong(self):
        """Waits for got_ack, got_err or a 10sec timeout.
        Synchronous, run in another thread."""
        st = time.time()
        while ((time.time() - st) < 5) and (self.got_ack == False) and (self.got_err == False):
            time.sleep(0.1)

        if self.got_ack:
            return 'ok'
        if self.got_err:
            raise MyError('got an error')
        else:
            raise MyError('timeout')

    def pingPong(self, msg='hi world'):
        """Does a ping/ack loop, via callbacks. Messages are sent and received,
        pretty decent communications test."""
        self.got_ack = False
        self.got_err = False
        # Kick off the initial say command
        self.doSendPing(msg)
        # Return a deferred that's a threaded function waiting for it all to finish.
        return threads.deferToThread(self.waitForPingPong)

    def sayPingPong(self, msg='hello, world'):
        """Does a send/speak/ack loop, via callbacks. Messages are sent and received,
        pretty decent communications test."""
        self.got_ack = False
        self.got_err = False
        # Kick off the initial say command
        self.doSendSay(msg)
        # Return a deferred that's a threaded function waiting for it all to finish.
        return threads.deferToThread(self.waitForPingPong)
