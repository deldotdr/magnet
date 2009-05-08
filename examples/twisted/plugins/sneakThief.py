#!/usr/bin/env python
"""A sneak thief runs with a Wallet. The pun-roll continues!

This is useful for running the cache agent outside of the twisted plugin system.
Recommended by Dorian, code copied from Alex's agent framework.

"""
__author__ = "hubbard"
__date__ ='$May 6, 2009 9:29:42 AM$'

from cache_agent_plugin import Wallet
import os
import sys

from magnet import pole
from magnet import field

from string import Template

from twisted.application import service, internet
from twisted.web import resource, server, static
from twisted.internet import reactor

import logging

def okCB(arg):
    logging.info('Callback invoked OK; code should be running now')

def errCB(failure):
    logging.error(failure)
    reactor.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

    SPEC = '/Users/hubbard/code/basicAmqp/amqp0-8.xml'
    logging.debug('Creating wallet...')
    w_agent = Wallet(routing_pattern='dataset')

    logging.debug('Connecting to magnet and AMQP...')
    w_connector = field.AMQPClientConnectorService(reactor, field.IAMQPClient(w_agent), name='w_agent')
    w_connector.connect(host='amoeba.ucsd.edu', spec_path=SPEC).addCallback(okCB).addErrback(errCB)
    w_connector.startService()

    reactor.run()
