"""
Basic Protocol - Factory pattern implementation for the AMQP protocol using
txAMQP

@file amqp.py
@author Dorian Raymer
@date 7/8/09
"""
import os

from txamqp import spec
from txamqp.protocol import AMQClient, AMQChannel
from txamqp.client import TwistedDelegate
from txamqp.queue import TimeoutDeferredQueue

from twisted.python import log
from twisted.internet import defer
from twisted.internet import protocol

import misted
# Spec file is loaded from the egg bundle. Hard code 0-8 for now...
spec_path_def = os.path.join(misted.__path__[0], 'spec', 'amqp0-8.xml')

class AMQPChannel(AMQChannel):
    """
    @note Adds to and augments functionality of the txAMQP library.
    """

    def __init__(self, id, outgoing):
        AMQChannel.__init__(self, id, outgoing)
        self.deliver_queue = TimeoutDeferredQueue()
        self._control_queue = TimeoutDeferredQueue()
        self._basic_deliver_buffer = []

class PocketDelegate(TwistedDelegate):
    """TwistedDelegate is the mechanism txAMQP provides for writing 
    specialized handlers for specific amqp methods from the broker.

    @note This should somehow be related with the PocketReactor of the messaging
    service.

    Handling methods, like deliver, will provide the mechanism for driving
    dynamo read events (the analog to select notification of ready file
    descriptors)

    """

    def basic_deliver(self, ch, msg):
        """This overrides TwistedDelegate.basic_deliver.
        
        The deferred queue implementation of txAMQP will be replaced with a
        regular queue (incoming buffer) and the pocket will be notified
        when to read from it.

        """
        # @todo, don't check protocol info here, just pass the message
        # along to the agreed spot...which is, what?
        ch.pocket.messageReceived(msg)

    def basic_return(self, ch, msg):
        """
        @todo This needs to be implemented, and needs to do something
        sensible for work producers.
        """


class AMQPClient(AMQClient):
    """
    @note Adds to and augments functionality of the txAMQP library.
    """
    
    next_channel_id = 0
    channelClass = AMQPChannel

    def channel(self, id=None):
        """Overrides AMQClient. Changes: 
            1) no need to return deferred. The channelLock doesn't protect
            against any race conditions; the channel reference is returned,
            so any number of those references could exist already. 
            2) auto channel numbering
            3) replace deferred queue for basic_deliver(s) with simple
               buffer(list)
        """
        if id is None:
            self.next_channel_id += 1
            id = self.next_channel_id
        try:
            ch = self.channels[id]
        except KeyError:
            # XXX The real utility is in the exception body; is that good
            # style?
            ch = self.channelFactory(id, self.outgoing)
            # the PacketDelegate defined above requires this buffer
            self.channels[id] = ch
        return ch

    def queue(self, key):
        """channel basic_deliver queue
        overrides AMQClient
            1) no need to be deferred
        """
        try:
            q = self.queues[key]
        except KeyError:
            q = TimeoutDeferredQueue()
            self.queues[key] = q
        return q

    def connectionMade(self):
        """
        Here you can do something when the connection is made.
        """
        AMQClient.connectionMade(self)

    def frameLengthExceeded(self):
        """
        """

class AMQPClientFactory(protocol.ClientFactory):
    """
    This plugs the amqp protocol/client into the transport.
    """

    protocol = AMQPClient

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

    amqpProtocol = AMQPClient
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
        d.addCallback(self._broker_login)
        return d

    @defer.inlineCallbacks
    def _broker_login(self, client):
        """
        @note timing of this is funny because instance factory already
        returned client reference in deferred callback
        Maybe instance factory isn't needed?
        """
        yield client.authenticate(self.username, self.password)
        defer.returnValue(client)



if __name__ == '__main__':
    """make this do something convenient
    """
    from twisted.internet import reactor

    f = AMQPFactory()
    reactor.connectTCP('amoeba.ucsd.edu', 5672, f)
    reactor.run()
