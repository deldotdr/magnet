"""
Basic Protocol - Factory pattern implementation for the AMQP protocol using
txAMQP
"""
import os

from txamqp import spec
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate

from twisted.python import log
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import protocol

import misted
# Spec file is loaded from the egg bundle. Hard code 0-8 for now...
spec_path_def = os.path.join(misted.__path__[0], 'spec', 'amqp0-8.xml')

class AMQPProtocol(AMQClient):
    """
    Really the client.
    """
    
    next_channel_id = 1

    @defer.inlineCallbacks
    def newChannel(self):
        self.next_channel_id += 1
        chan = yield self.channel(self.next_channel_id)
        defer.returnValue(chan)

    def connectionMade(self):
        AMQClient.connectionMade(self)
        print 'Connecto made'
        # d = self.authenticate(self.factory.username, self.factory.password)
        # d.addCallback

class AMQPClientFactory(protocol.ClientFactory):
    """
    This plugs the amqp protocol/client into the transport.
    """

    protocol = AMQPProtocol

    def __init__(self, username='guest', password='guest', 
                        vhost='/', spec_path=spec_path_def):
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec= spec.load(spec_path)

    def buildProtocol(self, addr):
        """
        TwistedDelegate is the extendable default handler for amqp methods.
        """
        delegate = TwistedDelegate()
        p = self.protocol(delegate, self.vhost, self.spec)
        p.factory = self
        return p 

class AMQPClientCreator(object):

    def __init__(self, reactor, protocolClass=AMQPProtocol, 
                        delegateClass=TwistedDelegate,
                        username='guest', password='guest', 
                        vhost='/', spec_path=spec_path_def):
        self.reactor = reactor
        self.protocolClass = protocolClass
        self.delegateClass = delegateClass
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec= spec.load(spec_path)

    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        d = defer.Deferred()
        p = self.protocolClass(self.delegateClass(), 
                                    self.vhost,
                                    self.spec)
        # p.factory = self
        f = protocol._InstanceFactory(self.reactor, p, d)
        self.reactor.connectTCP(host, port, f, timeout=timeout,
                bindAddress=bindAddress)
        return d





if __name__ == '__main__':
    from twisted.internet import reactor

    f = AMQPFactory()
    reactor.connectTCP('amoeba.ucsd.edu', 5672, f)
    reactor.run()
