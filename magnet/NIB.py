#!/usr/bin/env python

__author__='hubbard'
__date__ ='$Apr 27, 2009 10:15:30 AM$'

"""
NIB is a type of magnet (http://en.wikipedia.org/wiki/Neodymium_magnet) and NIB.py
is the pair class file for test.py, providing scaffolding that the test uses."""

from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet import protocol, reactor
import logging
from magnet import pole, field

class NIBConnector(pole.BasePole):

    def action_reply(self, message_object):
        """Triggered by replies to say"""
        logging.info('Got reply message: %s' % message_object['payload'])
        self.got_ack = True
        return None

    def action_say(self, message_object):
        """Triggered by say messages"""
        to_say = message_object['payload']
        logging.debug('Got say message: %s' % message_object['payload'])
        d = getProcessOutput('/usr/bin/say', ['-v', 'pipe organ', to_say])
        d.addBoth(self.sendOK, self.sendError)
        return None

    def sendOK(self, result):
        """Callback after say received, sends OK ack"""
        reply = {'method': 'reply', 'payload': 'ok'}
        logging.info('Sending back an OK')
        self.sendMessage(reply, 'test')

    def sendError(self, failure):
        """Errback after say received, sends back app error"""
        reply = {'method': 'reply', 'payload': 'got an error running say'}
        logging.error('Sending back an error message')
        self.sendMessage(reply, 'test')


    def doSend(self, msgString):
        """Sends a say message into the exchange"""
        self.got_ack = False
        smsg = {'method': 'say', 'payload': msgString}
        logging.info('Sending say message')
        self.sendMessage(smsg, 'test')
#        self.send_when_running('test', 'say', msgString)

    def disconnect(self):
        """TODO: Implement me!"""
        return

    @inlineCallbacks
    def connect(self):
        pass

    @inlineCallbacks
    def pingPong(self):
        yield self.talker.doSend('hi world')
