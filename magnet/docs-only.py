#!/usr/bin/env python

"""
@file docs-only.py
@author Dorian Raymer
@author pfh
@date 7/9/09
@brief Docs for Magnet


@mainpage
Welcome to Magnet, the way to use AMQP from OOI code. 
It provides a twisted-compatible connection abstraction based on 
'pockets', which are one letter different from sockets.

@section basic Basic Usage
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
@section Notes

Much more docs on the way, keep an eye out at http://amoeba.ucsd.edu/doxygen/Magnet

Magnet provides two major things:
 1) An implementation of core interfaces, defined by Twisted and centered around the interface ITransport, that provides an event driven framework for writing standard Twisted code against, and that utilizes the 2nd major thing:  
 2) An object called Pocket that:
  a) represents the interface to the "messaging service" or "message based transport"
      pocket:"message transport"::socket:TCP/IP
  b) completely encapsulates and abstracts AMQP 

@section code Sample code
@code
from twisted.internet import defer
from twisted.internet import reactor

@defer.inlineCallbacks
def startup():
    from magnet.preactor import Preactor
    preactor = yield Preactor()

    server_factory = ServerFactory()
    preactor.listenMS(name, server_factory)

    client_factory = ClientFactory()
    preactor.connectMS(name, client_factory)

@endcode

For more complete sample code, see add_client.py, add_server.py and the others in
the examples directory.
"""

import os
print 'No code here, just documentation'
