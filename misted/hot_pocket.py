"""
This is the core abstraction of amqp. This class provides the fundamental
unit of communication. As the socket is to TCP/IP, the "pocket" is to the
messaging service diff thingy.

In magnet, maybe this should be called core, or dynamo, or something. Here,
we get to have fun with names...hot pocket hot pocket: it's a hot pocket
stuffed inside a hot pocket, and it tastes just like a hot pocket.



The simplest case is that the hot_pocket conveniently wraps around one amqp
channel. 
For the framework, it needs to provide a mechanism for notification when
ready for reading and writing; the same idea as the relationship between a
socket and select.

This might also be where the analog to the select reactor implementation is
defined.
"""

import uuid


from twisted.internet import defer


class _AMQPChannelConfig(object):
    """Mixin utility class for Pocket providing common amqp channel
    configuration convenience functions
    """

    @defer.inlineCallbacks
    def simple_bidirectional(self):
        """configure a channel to consume off a certain queue, and make it
        so calling send will publish a message to a pre-determined
        routing_key/exchange pair

        Setup a converstion pattern:
        need a queue to receive 
        need a route (key, exchange name) to sendto

        Notes:
        if both routing key and queue name not specified, routing key will
            be the current queue for the channel, which is the last
            declared queue

        Same for basic_consume, no queue name defaults to current queue of
        the channel

        This set sets up a unique queue for receiving from one peer

        to start the consumer, txAMQP requires us to know the consumer_tag
        for the local queue buffer. This might be improvable
        """
        yield self.channel.queue_declare(auto_delete=True)
        yield self.channel.queue_bind()
        self.consumer_tag = str(uuid.uuid4())
        # basic_consume is supposed to be synchronous, no yield
        self.channel.basic_consume(consumer_tag=consumer_tag)

    def simple_consume(self):
        """consumer only configuration
        """


class _PocketObject(_AMQPChannelConfig):
    """Like socket...heh.
    
    First prototype:
        uses an amqp channel

    Notes:
    The config methods of this api should be sychronous to reduce
    complexity of Connection code (don't want deferreds in init procedures)
    Might be able to acomplish this with TwistedDelegate and doWrite
    pattern
    """

    # reference back to messaging service core
    dynamo = None

    def __init__(self, channel):
        self.channel = channel
        self.bindAddress = ''
        self.consumer_tag = ''


    def accept(self):
        """for listening port pattern

        get peers address 
        make new bi-directional pocket with dest_addr set to the connecting
        peer

        return pocket, addr
        """
        self.dynamo.pocket()

    def bind(self, addr):
        """local address

        XXX us config mixin
        """
        self.bindAddress = addr

    def close(self):
        """close pocket
        """
        self.channel.close()

    @defer.inlineCallbacks
    def connect(self, addr):
        """Establish bi-directional messaging connection with peer
        application at remote address addr

        @param addr address
        address should already be resolved into a real amqp address
        XXX the amqp address will be specified with an IAddress interface
        This prototype uses (exchange, routing_key)

        XXX: use config mixin to set up channel
        set up bi-directional connection to receive replies

        """
        self.dest_addr = addr
        # this could result in warnings from the broker...
        yield self.simple_bidirectional()

    def getpeername(self):
        """return remote addr (if connected)
        """

    def getlocalname(self):
        """return local addr (if bound)
        """

    def getid(self):
        """return local unique id (of channel)

        # with a FD (socket), this method is called fileno

        XXX in this prototype, 1:1 channel to pocket mapping makes sense
        """
        return self.channel.id

    # prototype fd interface compatibility
    fileno = getid

    def getpocketopt(self, optname):
        """get a pocket cofig option
        """

    def setpocketopt(self, option, value):
        """set a pocket config option
        """

    def listen(self, num):
        """listen for bi-directional incoming bi-directional connections,
        limit to num connections...

        XXX use config mixin
        """

    def consume(self):
        """similar to listen, but instead of accepting a new pocket
        dedicated bi-directional connection, take receipt of incoming
        data with no intention of an implicit reply response.
        """

    def recv(self, num):
        """receive num message[s]
        """

    def send(self, data):
        """send data. data is really a message...

        XXX need checking to make sure pocket is setup for send
            (connected, etc.)
        """
        exchange, routing_key = self.dest_addr
        content = Content(data)
        self.channel.basic_publish(exchange=exchange,
                                    content=content,
                                    routing_key=routing_key)

class DynamoCore(object):
    """The AMQP Connection

    This is the Messaging Service 

    This must always be running. Pockets depend on channels from the AMQP
    client.
    """

    def __init__(self):
        """the client attribute of the messaging service core is the amqp
        client instance, (connected and running)
        """
        self.client = None

    
    def pocket(self):
        """pocket instance factory

        For prototype, always return new pocket
        """
        chan = self.client.channel()
        p = _PocketObject(chan)
        p.dynamo = self
        return p



class Dynamo(object):
    """Analog of reactor
    event driver for pockets

    XXX This will be a service, or service collection
    XXX  or a cooperator service
    """

    def __init__(self):
        self._readers = {}
        self._writers = {}

    def addReader(self, reader):
        """
        """
        self._readers[reader] = 1

    def addWriter(self, writer):
        """
        """
        self._writers[writer] = 1

    def removeReader(self, reader):
        """
        """
        if reader in self._readers:
            del self._readers[reader]

    def removeWriter(self, writer):
        """
        """
        if writer in self._writers:
            del self._writers[writer]

    def removeAll(self):
        """
        """

    def getReaders(self):
        """
        """
        return self._readers.keys()

    def getWriters(self):
        """
        """
        return self._writers.keys()










