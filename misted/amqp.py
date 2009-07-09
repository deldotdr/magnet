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
from twisted.internet import protocol

import misted
# Spec file is loaded from the egg bundle. Hard code 0-8 for now...
spec_path_def = os.path.join(misted.__path__[0], 'spec', 'amqp0-8.xml')

class PocketDelegate(TwistedDelegate):
    """TwistedDelegate is a little frameworkish thing utilized by txAMQP
    for writing handlers to amqp methods from the broker.

    This should somehow be related with the Dynamo of the messaging
    service.

    Handling methods, like deliver, will provide the mechanism for driving
    dynamo read events (the analog to select notification of ready file
    descriptors)

    """

    def basic_deliver(self, ch, msg):
        """This overrides TwistedDelegate.basic_deliver.
        
        The deferred queue implementation of txAMQP will be replaced with a
        regular queue (incomming buffer) and the pocket will be notified
        when to read from it.

        XXX plan for handling syconicity of buffering and reading messages.
        """
        # best design? maybe use setter, 
        # or maybe client can manage buffers, just don't use deferred
        # queues
        ch._basic_deliver_buffer.append(msg)


class AMQPProtocol(AMQClient):
    """
    txAMQP defines AMQClient, but this is really the client (hence the name
    AMQPProtocol ;-) 
    """
    
    next_channel_id = 0

    def channel(self, id=None):
        """Overrides AMQClient. Improvements: 
            1) no need to return deferred.
            2) auto channel numbering
            3) replace deferred queue for basic_deliver(s) with simple
               buffer(list)
        """
        if id is None:
            id = self.next_channel_id += 1
        try:
            ch = self.channels[id]
        except KeyError:
            # XXX The real utility is in the exception body; is that good
            # style?
            ch = self.channelFactory(id, self.outgoing)
            # the PacketDelegate defined above requires this buffer
            ch._basic_deliver_buffer = []
            self.channels[id] = ch
        return ch

    def connectionMade(self):
        AMQClient.connectionMade(self)
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
    """Create AMQP Client. 
    The AMQP Client uses one persistent connection, so a Factory is not
    necessary.
    
    Client Creator is initialized with AMQP Broker configuration.

    ConnectTCP is called with TCP configuration.
    """

    amqpProtocol = AMQPProtocol
    amqpDelegate = PocketDelegate

    def __init__(self, reactor, username='guest', password='guest', 
                                vhost='/', spec_path=spec_path_def):
        self.reactor = reactor
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec= spec.load(spec_path)
        self.connector = None

    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        """Connect to remote Broker host, return a Deferred of resulting protocol
        instance.
        """
        d = defer.Deferred()
        p = self.amqpProtocol(self.amqpDelegate(), 
                                    self.vhost,
                                    self.spec)
        # p.factory = self
        f = protocol._InstanceFactory(self.reactor, p, d)
        self.connector = self.reactor.connectTCP(host, port, f, timeout=timeout,
                bindAddress=bindAddress)
        return d





if __name__ == '__main__':
    """make this do something convenient
    """
    from twisted.internet import reactor

    f = AMQPFactory()
    reactor.connectTCP('amoeba.ucsd.edu', 5672, f)
    reactor.run()
