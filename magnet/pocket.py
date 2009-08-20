""" 
A pocket represents one end point of a connection in the message service
(message based IPC).


Pocket is to messaging service transport (mtp) as socket is to TCP/IP


@todo Robust control protocol inside of Pocket. These messages are not
visible or accessible out side of the pocket.

@file pocket.py
@author Dorian Raymer
@date 7/9/09
"""


from zope.interface import implements

from twisted.python import log
from twisted.internet import defer
from twisted.internet.interfaces import ILoggingContext

from txamqp.content import Content

class BasePocket(object, log.Logger):
    """General methods and attributes of all pocket flavors.
    """

    implements(ILoggingContext)

    def __init__(self, channel, debug=False):
        """
        """
        channel.pocket = self
        self.channel = channel

        self._recv_buff = []

        self.send_exchange = ''
        self.send_routing_key = ''

        self.immediate = False
        self.mandatory = False

    logstr = "Uninitialized Pocket"

    def logPrefix(self):
        """
        """
        return self.logstr

    def accept(self):
        """
        Accept a new unique connection request.
        """

    def bind(self, name):
        """
        Bind this connection to a name.
        The name should be translatable/resolvable into an amqp address.

        @param name 
        """

    def close(self):
        """
        Close pocket connection.
        """

    def connect(self, name):
        """
        Connect to a name.
        The name should be translatable/resolvable into an amqp address.

        @param name
        """

    def listen(self):
        """
        Listen for unique connection requests.
        """

    def recv(self):
        """
        Receive one message.
        """

    def send(self, data):
        """
        Send data as a message to remote name.
        """
        self.sendMessage(data)

    # # # # # # # # # # # # # # # # # # # # # # # 
    # 
    def messageReceived(self, msg):
        """txAMQP delegate calls this
        """
        log.msg('messageReceived', msg.content.properties)
        if not msg.content.properties.has_key('type'):
            log.msg('Message has no control type!')
            raise KeyError('Message has no control type!')
        control_method = msg.content.properties['type']
        if not hasattr(self, 'pocket_%s' % control_method):
            raise KeyError("%s is NOT a Pocket Transport Control Method" % control_method)
        getattr(self, 'pocket_%s' % control_method)(msg)

    def sendMessage(self, payload, props={}):
        """
        This ties together:
        - content, what is to be sent (with header data included)
        - exchange and routing key, where to be sent
        - other config
          - immediate, detect lost peer
          - mandatory, make sure msg is routed to a queue
        """
        props.update({'type':'app_msg'}) 
        content = Content(payload, properties=props)
        self.channel.basic_publish(exchange=self.send_exchange,
                                    content=content,
                                    routing_key=self.send_routing_key,
                                    immediate=self.immediate,
                                    mandatory=self.mandatory)

    def _set_send_address(self, exchange, routing_key):
        """
        """
        self.send_exchange = exchange
        self.send_routing_key = routing_key

    def _set_receive_address(self, exchange, binding, queue):
        """
        """
        self.recv_exchange = exchange
        self.recv_binding = binding
        self.recv_queue = queue

    def pocket_app_msg(self, amqp_msg):
        """
        @todo Check amqp headers. See if deliver ack is needed.
        """
        data = amqp_msg.content.body
        self._recv_buff.append(data)

    def read_ready(self):
        """If True, the poll will indicate this pocket ready for doRead.
        """
        return bool(len(self._recv_buff)) 

    def write_ready(self):
        """If True, the poll will indicate this pocket ready for doWrite.
        """
        return True

    @defer.inlineCallbacks
    def purge_name(self, name):
        yield self.channel.channel_open()
        yield self.channel.queue_purge(queue=name)

    @defer.inlineCallbacks
    def delete_name(self, name):
        yield self.channel.channel_open()
        yield self.channel.queue_delete(queue=name, if_unused=False, if_empty=False)
        yield self.channel.exchange_delete(exchange=name)

class Bidirectional(BasePocket):
    """
    A pocket protocol for handling connection control messages and delegating
    content messages to a buffering mechanism for the pocket.


    The pocket control protocol deals with managing connections in an
    abstraction space directly above amqp channels. So far in this
    prototype, there is not much complexity beyond configuration
    conventions. The pocket is pretty much coupled to a channel. 
    """

    def __init__(self, channel):
        BasePocket.__init__(self, channel)
        self.logstr = self.__class__.__name__

        self._connections_to_accept = []

        self.can_write = False

        # publish configuration
        self.immediate = True
        self.mandatory = True



    def accept(self):
        """
        Accept returns a pocket instance that was created by a remote peer
        connecting.
        """
        pkt = self._connections_to_accept.pop(0)
        return pkt

    @defer.inlineCallbacks
    def bind(self, name):
        """Bind this pocket to the amqp address @param bind_addr.
        """
        # exchange = bind_addr['exchange']
        # queue = bind_addr['queue']
        # binding = bind_addr['binding']

        exchange = 'amq.direct'
        queue = name
        binding = name

        yield self.channel.channel_open()

        if queue:
            yield self.channel.queue_declare(queue=queue, exclusive=True)
        else:
            reply = yield self.channel.queue_declare(auto_delete=True, exclusive=True)
            queue = reply.queue

        if binding:
            yield self.channel.queue_bind(exchange=exchange, routing_key=binding)
            # need to set what ended up being the queue name for replys
        else:
            yield self.channel.queue_bind(exchange=exchange)
            binding = queue

        self._set_receive_address(exchange, binding, queue)
        defer.returnValue(None)


    def _set_receive_address(self, exchange, binding, queue):
        """
        """
        self.recv_exchange = exchange
        self.recv_binding = binding
        self.recv_queue = queue


    def close(self):
        """
        @todo add connection termination
        """
        self.channel.close()

    @defer.inlineCallbacks
    def connect(self, name):
        """Configure a channel to consume off a certain queue, and make it
        so calling send will publish a message to a pre-determined
        routing_key/exchange pair

        Implement control protocol interactions.

        Setup a conversation pattern:
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

        exchange = 'amq.direct'
        routing_key = name

        self._set_send_address(exchange, routing_key)

        # self.consumer_tag = str(uuid.uuid4())
        # yield self.channel.basic_consume(consumer_tag=self.consumer_tag)
        yield self.channel.basic_consume()

        self._start()
        defer.returnValue(None)


    def _set_send_address(self, exchange, routing_key):
        """
        """
        self.send_exchange = exchange
        self.send_routing_key = routing_key

    @defer.inlineCallbacks
    def _bind_and_connect(self, exchange, routing_key):
        """
        Short cut config for pockets created by listening server.
        """
        self._set_send_address(exchange, routing_key)

        yield self.channel.channel_open()

        reply = yield self.channel.queue_declare(auto_delete=True, exclusive=True)
        yield self.channel.queue_bind(exchange=exchange)

        queue = reply.queue
        binding = queue

        self._set_receive_address(exchange, binding, queue)

        yield self.channel.basic_consume()
        defer.returnValue(None)

    
    @defer.inlineCallbacks
    def listen(self):
        """
        """
        yield self.channel.basic_consume()


    # # # # # # # # # # # # # # # # # # # # # #  
    # Connection control interaction protocol
    # 

    def sendControl(self, payload='', props=None):
        """
        """
        exchange, routing_key = self.send_exchange, self.send_routing_key
        content = Content(payload, properties=props)
        self.channel.basic_publish(exchange=exchange,
                                    content=content,
                                    routing_key=routing_key,
                                    immediate=True,
                                    mandatory=True)

    def _start(self):
        """
        1.a <- Client Connection
        Connect to a peer.
        Set reply to address to be the name of this peers private queue.

        Wait for peer to return starting
        """
        props = {}
        props['type'] = 'start'
        # in the amqp header, reply to should be an amqp address...
        props['reply to'] = "%s:%s" % (self.recv_exchange, self.recv_binding)
        self.sendControl(props=props)

    @defer.inlineCallbacks
    def pocket_start(self, msg):
        """
        1.b -> Server Listener
        Connection establishment.
        This is where the listener receives first contact from the remote peer.
        Retrieve remote peer address (reply to header, (routing key)).

        Create a new pocket for this connection. Set the reply to address
        to the private queue of the resulting pocket.

        Reply by calling _starting 
        """
        props = msg.content.properties
        # XXX @todo remove address processing
        replyto = props['reply to']
        send_exchange, send_routing_key = props['reply to'].split(':')
        pkt = self.dynamo.pocket()
        # configure new pocket to be a bidirectional connection
        yield pkt._bind_and_connect(send_exchange, send_routing_key)
        self._connections_to_accept.append((pkt, replyto,))
        pkt._starting()
        # Server is now done in this connection establishment cycle

    def _starting(self):
        """
        2.a <- Server Connection
        First message sent by newly created pocket.
        Reply to connecting peer with address to talk on (queue name of new
        connection).
        """
        props = {}
        props['type'] = 'starting'
        props['reply to'] = "%s:%s" % (self.recv_exchange, self.recv_binding)
        self.sendControl(props=props)

    def pocket_starting(self, msg):
        """
        2.b -> Client Connection
        Receive acknowledgment and reply to address from listener.
        This is the address of the private queue the listener set up for
        this conversation.
        """
        props = msg.content.properties
        send_exchange, send_routing_key = props['reply to'].split(':')
        self._set_send_address(send_exchange, send_routing_key)
        self.starting = 1
        self._started()

    def _started(self):
        """
        3.a <- Client Connection
        Send acknowledgment to pocket created by listener.
        """
        props = {}
        props['type'] = 'started'
        self.started = True
        self.sendControl(props=props)
        self.can_write = True

    def pocket_started(self, msg):
        """
        3.b -> Server Connection
        Final message in three way tcp-like handshake establishing a 
        bidirectional conversation.
        """
        self.started = True
        # put something in recv queue to trigger accept by server
        # self._recv_buff.append()
        self.can_write = True
    
    # 
    # End connection establishment protocol 
    # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

    def recv(self):
        """
        """
        data = self._recv_buff.pop(0)
        return data

    def read_ready(self):
        """If True, the poll will indicate this pocket ready for doRead.
        """
        return bool(len(self._recv_buff)) or bool(len(self._connections_to_accept))

    def write_ready(self):
        return self.can_write

class WorkerPatternBase(BasePocket):
    """
    Mixin, most useful for toggling development mode of amqp worker queue
    parameters.
    """

    def __init__(self, channel, debug=False):
        BasePocket.__init__(self, channel, debug)
        self.logstr = self.__class__.__name__

        self.last_delivery = None

        self.mandatory = True
        self.immediate = False
        self.can_write = False

        self.pfetch_size = 1
        self.pfetch_count = 1

        if debug:
            self.auto_delete = True
            self.durable = False
        else:
            self.auto_delete = False
            self.durable = True


class WorkConsumer(WorkerPatternBase):
    """
    @note AMQP configuration:
    - bind to shared queue (pre determined name)
    - set qos to one message per deliver
    - ack deliver *only* when work is done
    """


    @defer.inlineCallbacks
    def bind(self, name):
        """Hack to minimize changes to BaseClient..it expects this to be
        deferred.
        """
        yield self.channel.channel_open()
        defer.returnValue(None)


    @defer.inlineCallbacks
    def connect(self, name):
        """
        Set up a shared named work queue.
        @note These idempotent config commands are unnecessary once one
        worker is active, it is necessary, however, for this
        general procedure to always config the queue.
        """
        yield self.channel.queue_declare(queue=name, auto_delete=self.auto_delete)
        yield self.channel.exchange_declare(exchange=name, 
                                        type='topic',
                                        durable=self.durable,
                                        auto_delete=self.auto_delete)
        # If queue and routing key empty, routing key will be the current
        # queue for the channel, which is the last declared queue
        # nowait=False, make sure it works...
        yield self.channel.queue_bind(exchange=name)
        yield self.channel.basic_qos(prefetch_count=self.pfetch_count)
        yield self.channel.basic_consume(queue=name, no_ack=False)
        defer.returnValue(None)

    def pocket_app_msg(self, amqp_msg):
        """
        @todo Check amqp headers. See if deliver ack is needed.
        """
        data = amqp_msg.content.body
        self.last_delivery = amqp_msg.delivery_tag
        self._recv_buff.append(data)

    def recv(self):
        """
        """
        data = self._recv_buff.pop(0)
        return data

    # @defer.inlineCallbacks
    def ack(self):
        """Basic ack prototype
        """
        if self.last_delivery:
            self.channel.basic_ack(delivery_tag=self.last_delivery)
            self.last_delivery = None


class WorkProducer(WorkerPatternBase):
    """Send all messages to a name.
    The name represents a shared queue.
    The pocket should verify published messages are queued by waiting for
    the queue to ack. It will then be the responsibility of the queue to
    ensure the message is processed by a worker.
    """


    @defer.inlineCallbacks
    def bind(self, name):
        """Hack to minimize changes to BaseClient..it expects this to be
        deferred.
        """
        yield self.channel.channel_open()
        defer.returnValue(None)

    @defer.inlineCallbacks
    def connect(self, name):
        """Set up exchange and shared named work queue (for worker
        counterpart).
        @note These idempotent config commands are unnecessary once one
        worker is active, it is necessary, however, for this
        general procedure to always config the queue.
        @note The WorkConsumer applies the same configuration.
        @todo Introduce another element in the system responsible for this
        configuration.
        """
        self._set_send_address(name, name)
        yield self.channel.queue_declare(queue=name, auto_delete=self.auto_delete)
        yield self.channel.exchange_declare(exchange=name, 
                                        type='topic',
                                        durable=self.durable,
                                        auto_delete=self.auto_delete)
        # If queue and routing key empty, routing key will be the current
        # queue for the channel, which is the last declared queue
        # nowait=False, make sure it works...
        yield self.channel.queue_bind(exchange=name)
        self.can_write = True
        defer.returnValue(None)

    def write_ready(self):
        return self.can_write

