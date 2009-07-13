""" 
A pocket represents one end point of a connection in the message service
(message based IPC).

_PocketObect is the class pocket connections are created from.  

Pocket is to messaging service transport (mtp) as socket is to TCP/IP


@todo Robust control protocol inside of Pocket. These messages are not
visible or accessible out side of the pocket.

@file pocket.py
@author Dorian Raymer
@date 7/9/09
"""

import uuid

from twisted.internet import defer
from twisted.python import log

from txamqp.content import Content

from misted import mtp

# Message Service message types 


class _AMQPChannelConfig(object):
    """Mixin utility class for Pocket providing common amqp channel
    configuration convenience functions

    The use of the amqp channel is based on the amqp specification and
    therefore is theoretically capable of being decoupled and independent
    of the underlying amqp client library. In this prototype, specific
    knowledge of message delivery specific to the client library is
    necessary, and in fact directly dictated/tailored in misted.amqp.  
    """
    consumer_tag = None

    @defer.inlineCallbacks
    def _bidirectional_connect(self):
        """Configure a channel to consume off a certain queue, and make it
        so calling send will publish a message to a pre-determined
        routing_key/exchange pair

        Implement control protocol interactions.

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
        # hopefully don't need to create consumer tags anymore
        self.consumer_tag = str(uuid.uuid4())
        yield self.channel.basic_consume(consumer_tag=self.consumer_tag)
        d_starting_msg = self._connect_start()
        starting_msg = yield d_starting_msg()
        props = starting_msg.content.properties
        self.dest_addr = props['reply to'].split(':')
        self.started = True
        self._connect_started()
        defer.returnValue('connected')
        

    @defer.inlineCallbacks
    def _bind_and_connect_from_accept(self, dest_addr):
        """
        Start connection endpoint resulting from an accept
        """
        self.dest_addr = dest_addr
        if self.bindAddress[1]:
            exchange = self.bindAddress[0]
            queue = self.bindAddress[1]
        else:
            exchange = 'amq.direct'
            queue = None
        yield self.channel.channel_open()
        if queue:
            yield self.channel.queue_declare(queue=queue, auto_delete=True)
        else:
            reply = yield self.channel.queue_declare(auto_delete=True)
            self.bindAddress[1] = queue = reply.queue
        yield self.channel.queue_bind(exchange=exchange)
 
        yield self.channel.basic_consume()
        self._connect_starting()
        defer.returnValue(queue)

    @defer.inlineCallbacks
    def _listen(self):
        """start consumer

        @todo need to check not already listening, connected, etc.

        AMQP Notes:
        Same for basic_consume, no queue name defaults to current queue of
        the channel
        """
        yield self.channel.basic_consume()

    @defer.inlineCallbacks
    def _bind(self):
        """consumer configuration

        use declare when given an explicit name to bind to

        when broker creates queue name, set bindAddress
        """
        # prototype: routing_key == queue name
        if self.bindAddress[1]:
            exchange = self.bindAddress[0]
            queue = self.bindAddress[1]
        else:
            exchange = 'amq.direct'
            queue = None
        yield self.channel.channel_open()
        if queue:
            yield self.channel.queue_declare(queue=queue, auto_delete=True)
        else:
            reply = yield self.channel.queue_declare(auto_delete=True)
            self.bindAddress[1] = queue = reply.queue
        yield self.channel.queue_bind(exchange=exchange)
        defer.returnValue(queue)

    def _send(self, payload='', props={'type':'regular'}):
        """
        """
        if self.bindAddress[1]:
            props['reply to'] = ':'.join(self.bindAddress)
        exchange, routing_key = self.dest_addr
        content = Content(payload, properties=props)
        self.channel.basic_publish(exchange=exchange,
                                    content=content,
                                    routing_key=routing_key)

    def _accept(self):
        """Listening socket gets first message from a peer

        For this to work, the connecting peer *must* bind.
         The bind can result in the broker creating a unique queue, or
         The peer could specify a queue name. Either way, the peer is
         responsible for setting the reply_to property
        """
        # @todo need formal read method for buffer
        reply_to = ''
        amqp_msg = self.channel._basic_deliver_buffer.pop(0)
        try:
            reply_to = amqp_msg.content.properties['reply to'].split(':')
        except KeyError:
            # need reply_to for this 'server' pattern of pocket
            # @todo need to learn best way to throw relavent exceptions
            # and print trace backs
            log.err()
        return reply_to

    def _recv(self):
        """Pop a message off the _basic_deliver_buffer, process amqp
        Content class, and return payload
        """
        amqp_msg = self.channel._basic_deliver_buffer.pop(0)
        return amqp_msg.content.body
    
    def _close(self):
        self.channel.close()

    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  
    # Control protocol
    # @todo Figure out where to draw the line between config utility
    #     functions and control protocol functions
    started = False

    def _connect_start(self):
        """first msg of control protocol
        client to listener
        """
        props = {}
        props['type'] = 'ccontrol'
        props['message id'] = 'start'
        self._send(props=props)
        d = self.channel.deliver_queue.get
        return d

    def _connect_starting(self):
        """second msg of control protocol
        server (new connection) to client
        """
        props = {}
        props['type'] = 'control'
        props['message id'] = 'starting'
        self._send(props=props)

    def _connect_started(self):
        """third msg of control protocol
        client to server

        @todo is reply_to needed at this point?
        """
        props = {}
        props['type'] = 'ccontrol'
        props['message id'] = 'started'
        self._send(props=props)


class _PocketObject(_AMQPChannelConfig):
    """
    A pocket represents one end point of a connection in the message service
    (message based IPC).
    
    First prototype:
        uses one amqp channel

    Notes:
    The methods of this api should be synchronous to reduce
    complexity of Connection code (don't want deferreds in init procedures)
    Might be able to accomplish this with TwistedDelegate and doWrite
    pattern.
    @todo 
    Design such that usage of deferred calls drops to zero. The Inherently
    asynchronous methods (like recv, for example) may return a
    """

    # reference back to messaging service core (set by dynamo itself)
    dynamo = None

    def __init__(self, channel):
        self.channel = channel
        self.bindAddress = ['amq.direct', '']
        self.dest_addr = ['amq.direct', '']

        self.started_deferred = None
        self._bound = False


    @defer.inlineCallbacks
    def accept(self):
        """for listening port pattern

        get peers address 
        make new bi-directional pocket with dest_addr set to the connecting
        peer

        return pocket, addr
        """
        # peer_addr should be resolved already (arrive resolved in connect
        # message)
        peer_addr = self._accept()
        pkt = self.dynamo.pocket()
        yield pkt._bind_and_connect_from_accept(peer_addr)
        defer.returnValue((pkt, peer_addr))

    @defer.inlineCallbacks
    def bind(self, addr):
        """local address

        @param addr address
        address should already be resolved into a real amqp address
        @todo us config mixin
        @todo shouldn't be deferred. 
        Should queue bind be called here?
         - might hold off until listen is called
         - might not always be for listen..
        """
        self.bindAddress = addr
        yield self._bind()
        self._bound = True
        defer.returnValue(None)

    def close(self):
        """close pocket
        """
        self._close()

    def connect(self, addr):
        """
        Initiate a bi-directional messaging connection with peer
        application at remote address addr

        @param addr address
        address should already be resolved into a real amqp address
        @todo the amqp address will be specified with an IAddress interface
        This prototype uses (exchange, routing_key)

        @todo: use config mixin to set up channel
        set up bi-directional connection to receive replies

        """
        self.dest_addr = addr
        # this could result in warnings from the broker...
        d = self._bidirectional_connect()
        return d

    def getpeername(self):
        """return remote addr (if connected)
        """

    def getlocalname(self):
        """return local addr (if bound)
        """

    def getid(self):
        """return local unique id (of channel)

        # with a FD (socket), this method is called fileno

        @todo in this prototype, 1:1 channel to pocket mapping makes sense
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

    def listen(self):
        """listen for bi-directional incoming bi-directional connections,
        limit to num connections...

        @todo use config mixin
        """
        self._listen()
        

    def consume(self):
        """similar to listen, but instead of accepting a new pocket
        dedicated bi-directional connection, take receipt of incoming
        data with no intention of an implicit reply response.
        """

    def recv(self):
        """receive message
        used for really receiving data, never control data
        """
        return self._recv()

    def send(self, data):
        """send data. data is really a message...
        

        @todo need checking to make sure pocket is setup for send
            (connected, etc.)
        @todo is this method of sending for bi-directional only?
        """
        self._send(data)

    def _is_read_ready(self):
        """If True, the poll indicates this pocket as ready for reading.

        Prototype: check channel._basic_deliver_buffer
        Eventually, this buffer check should gerneralize beyound
        basic_deliver
        """
        return bool(len(self.channel._basic_deliver_buffer))

    def _is_write_ready(self):
        """If True, the poll indicates this pocket as ready for writing.
        
        @todo simple criteria for prototype
        @todo always ready for now
        """
        return True

    # convenience for prototype
    read_ready, write_ready = _is_read_ready, _is_write_ready






