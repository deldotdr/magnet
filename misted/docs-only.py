#!/usr/bin/env python

"""
@file docs-only.py
@author Dorian Raymer
@author pfh
@date 7/9/09
@brief Docs for Misted


@mainpage
Welcome to Misted, the successor to Magnet. Misted, whose name may change without
warning, is the Shiny! New! Way! to use AMQP from OOI code. It provides a
twisted-compatible connection abstraction based on 'pockets', which are one
letter different from sockets.

@section Basic Usage
Pocket connections need a special reactor, the PocketReactor.

The PocketReactor is the main thing an application developer will use.
Nothing else is required outside of implementing Protocols and Factories
with standard Twisted modules. misted.protocol provides a ClientCreator for
conveniently connecting client protocols/factories to the 'messaging
service'. It accomplishes the same thing as t.i.p.ClientCreator except that
it uses p_reactor.connectMS, instead of reactor.connectTCP, .connectUNIX,
or .connectSSL.

The PocketReactor requires a running Twisted reactor and a running txAMQP
client as init args.

@code
from twisted.internet import defer
from twisted.internet import reactor
from misted import amqp
from misted import core

\@defer.inlineCallbacks
def startup():
    clientCreator = amqp.AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = core.PocketReactor(reactor, client)
    server_factory = ServerFactory()
    p_reactor.listenMS(bindAddress, server_factory)

    client_factory = ClientFactory()
    p_reactor.connectMS(address, client_factory)

@section Notes

Much more docs on the way, keep an eye out at http://amoeba.ucsd.edu/doxygen/Misted
"""

import os
print 'No code here, just documentation'
