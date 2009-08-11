"""
@file protocol.py
@author Dorian Raymer
@date 7/13/09
"""

from twisted.internet import defer
from twisted.internet.protocol import _InstanceFactory


class ClientCreator(object):
    """Create clients using messaging service pocket reactor.
    @note This is the analog of t.i.p.ClientCreator, providing only
    connectMS.
    """

    def __init__(self, reactor, p_reactor, protocolClass, *args, **kwargs):
        """Client Creator for 'messaging service'.
        @param reactor Usual Twisted reactor object.
        @param p_reactor The Pocket Reactor instance.
        @param protocolClass The client protocol class to connect to the
        'messaging service'
        """
        self.reactor = reactor
        self.p_reactor = p_reactor
        self.protocolClass = protocolClass
        self.args = args
        self.kwargs = kwargs

    def connectMS(self, address, timeout=30, bindAddress=None):
        """Connect to remote address (messaging service name), return
        q Deferred of resulting protocol instance.
        @param address 'Messaging service' remote address (name)
        @param bindAddress 'Messaging service' address (name) to listen on.
        """
        d = defer.Deferred()
        f = _InstanceFactory(self.reactor, self.protocolClass(*self.args, **self.kwargs), d)
        self.p_reactor.connectMS(address, f, timeout=timeout, bindAddress=bindAddress)
        return d

    def connectWorkConsumer(self, name, timeout=30, bindName=None):
        """
        Connect to remote address (messaging service name), return
        q Deferred of resulting protocol instance.
        @param address 'Messaging service' remote address (name)
        @param bindAddress 'Messaging service' address (name) to listen on.
        """
        d = defer.Deferred()
        f = _InstanceFactory(self.reactor, self.protocolClass(*self.args, **self.kwargs), d)
        self.p_reactor.connectWorkConsummer(name, f, timeout=timeout, bindAddress=bindName)
        return d

    def connectWorkProducer(self, name, timeout=30, bindName=None):
        """
        Connect to remote address (messaging service name), return
        q Deferred of resulting protocol instance.
        @param address 'Messaging service' remote address (name)
        @param bindAddress 'Messaging service' address (name) to listen on.
        """
        d = defer.Deferred()
        f = _InstanceFactory(self.reactor, self.protocolClass(*self.args, **self.kwargs), d)
        self.p_reactor.connectWorkProducer(name, f, timeout=timeout, bindAddress=bindName)
        return d
