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
from twisted.python import log


class _AMQPChannelConfig(object):
    """Mixin utility class for Pocket providing common amqp channel
    configuration convenience functions

    amqp config methods often return deferreds; this mixin attempts to mask
    those deferreds from the ITransport framework using it...
    """
    consumer_tag = None

    @defer.inlineCallbacks
    def _bidirectional_connect(self):
        """configure a channel to consume off a certain queue, and make it
        so calling send will publish a message to a pre-determined
        routing_key/exchange pair

        Setup a converstion pattern:
        need a queue to receive 
        need a route (key, exchange name) to sendto

        AMQP Notes:
        if both routing key and queue name not specified, routing key will
            be the current queue for the channel, which is the last
            declared queue

        Same for basic_consume, no queue name defaults to current queue of
        the channel

        This set sets up a unique queue for receiving from one peer

        to start the consumer, txAMQP requires us to know the consumer_tag
        for the local queue buffer. This might be improvable
        """
        yield self.channel.open()
        yield self.channel.queue_declare(auto_delete=True)
        yield self.channel.queue_bind()
        # hopefully don't need to create consumer tags anymore
        # self.consumer_tag = str(uuid.uuid4())
        # basic_consume is supposed to be synchronous, no yield
        self.channel.basic_consume()

    def _connect(self):
        """
        """

    @defer.inlineCallbacks
    def _listen(self):
        """start consumer

        XXX need to check not already listening, connected, etc.

        AMQP Notes:
        Same for basic_consume, no queue name defaults to current queue of
        the channel
        """
        self.channel.basic_consume()

    @defer.inlineCallbacks
    def _bind(self):
        """consumer configuration

        use declare when given an explicit name to bind to
        """
        # prototype: routing_key == queue name
        queue = self.bindAddress[1]
        yield self.channel.open()
        yield self.channel.queue_declare(queue=queue, auto_delete=True)
        yield self.channel.queue_bind()

    def _send(self, payload):
        """
        """
        props = {}

    def _accept(self):
        """Listening socket gets first message from a peer

        For this to work, the connecting peer *must* bind.
         The bind can result in the broker creating a unique queue, or
         The peer could specify a queue name. Either way, the peer is
         responsible for setting the reply_to property
        """
        amqp_msg = self.channel._basic_deliver_buffer.pop()
        try:
            amqp_msg.content.properties.['reply_to']
        except KeyError:
            # need reply_to for this 'server' pattern of pocket
            # XXX need to learn best way to throw relavent exceptions
            # and print trace backs
            log.err()

    def _recv(self):
        """Pop a message off the _basic_deliver_buffer, process amqp
        Content class, and return payload
        """
        amqp_msg = self.channel._basic_deliver_buffer.pop()
        props = 
    
    def _close(self):
        self.channel.close()


class _PocketObject(_AMQPChannelConfig):
    """Like socket...heh.
    
    First prototype:
        uses an amqp channel

    Notes:
    The config methods of this api should be synchronous to reduce
    complexity of Connection code (don't want deferreds in init procedures)
    Might be able to accomplish this with TwistedDelegate and doWrite
    pattern
    """

    # reference back to messaging service core (set by dynamo itself)
    dynamo = None

    def __init__(self, channel):
        self.channel = channel
        self.bindAddress = ''
        self.dest_addr = ''


    def accept(self):
        """for listening port pattern

        get peers address 
        make new bi-directional pocket with dest_addr set to the connecting
        peer

        return pocket, addr
        """
        # XXX need formal read method
        # need to establish connect protocol... 
        msg = self.channel._basic_deliver_buffer.pop()
        # extract peer address
        self.dynamo.pocket()

    def bind(self, addr):
        """local address

        @param addr address
        address should already be resolved into a real amqp address
        XXX us config mixin
        XXX shouldn't be deferred. 
        Should queue bind be called here?
         - might hold off until listen is called
         - might not always be for listen..
        """
        self.bindAddress = addr
        self._listen_queue_bind()

    def close(self):
        """close pocket
        """
        self._close()

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
        yield self._bidirectional_connect()

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

    # prototype FD interface compatibility
    # may or may not be needed
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
        XXX is this method of sending for bi-directional only?
        """
        exchange, routing_key = self.dest_addr
        content = Content(data)
        self.channel.basic_publish(exchange=exchange,
                                    content=content,
                                    routing_key=routing_key)

    def _is_read_ready(self):
        """If True, the poll indicates this pocket as ready for reading.

        Prototype: check channel._basic_deliver_buffer
        Eventually, this buffer check should gerneralize beyound
        basic_deliver
        """
        return bool(len(self.channel._basic_deliver_buffer))

    def _is_write_ready(self)
        """If True, the poll indicates this pocket as ready for writing.
        
        XXX simple criteria for prototype
        XXX always ready for now
        """
        return True

    # convenience for prototype
    read_ready, write_ready = _is_read_ready, _is_write_ready

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

    def listenMS(self, addr, factory, backlog=50):
        """Connects given factory to the given message service address.
        """
        p = fog.ListeningPort(addr, factory, self.reactor, self)
        p.startListening()
        return p

    def connectMS(self, addr, factory, timeout=30, bindAddress=None):
        """Connect a message service client to given message service
        address.
        """
        c = fog.Connector(addr, factory, timeout, bindAddress,
                self.reactor, self)
        c.connect()
        return c

def _pocket_poll(self, readers, writers):
    """Poll over read and write pocket objects checking for read and
    writeablity (analog of select)

    The getting of the pocket reference explicitly from the abstract pocket
    might best be wrapped by a getter (maybe that's what filehandle is for)
    """
    readables = [r for r in readers if r.pocket.read_ready()]
    writeables = [w for w in writers if w.pocket.write_ready()]
    return readables, writeables

class PocketDynamo(object):
    """Analog of select reactor
    Event driver for pockets

    XXX This will be a service, or service collection
    XXX  or a cooperator service
    """

    def __init__(self):
        """
        readers and writers are pocket obects
        """
        self._readers = {}
        self._writers = {}

    def doIteration(self):
        """Run one iteration of checking the channel buffers

        (This is the analog to what select does with file descriptors)
        """
        r, w = _pocket_poll(self._readers, self._writers)

        _drdw = self._doReadOrWrite
        _logrun = log.callWithLogger

        for pkts, method in ((r, 'doRead'), (w, 'doWrite')):
            for pkt in pkts:
                # XXX not sure why dict is passed in and out of logger?
                _logrun(pkt, _drdw, pkt, method, dict)

    def _doReadOrWrite(self, pkt, method, dict):
        """XXX not sure why dict
        """
        try:
            # leaving out other error checking done in select reactor
            why = getattr(pkt, method)()
        except:
            log.err()

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



def test():
    """
    How is the amqp client part of the dynamo going to get set up?
    
    """






