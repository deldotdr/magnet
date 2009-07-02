import uuid

from twisted.internet import defer

from txamqp.content import Content

from misted.amqp import AMQPProtocol

TEST_EXCHANGE = 'misted'

class MessageServiceConfiguration:

    def registerTopic(self, topic_address):
        """
        """

class MessageService(AMQPProtocol):
    """
    This is the thing that manages amqp channels.
    It knows how to create them, keep track of them, and abstract them.
    """

    @defer.inlineCallbacks
    def createReceivingChannel(self, exchange, routing_key):
        """
        """
        log.msg('new receiving channel')
        channel_num = self.next_channel_id 
        self.next_channel_id += 1
        chan = yield self.channel(channel_num)
        yield chan.channel_open()
        reply = yield channel.queue_declare(auto_delete=True)
        yield chan.queue_bind(queue=reply.queue,
                                exchange=exchange,
                                routing_key=routing_key)
 
        consumer_tag = str(uuid.uuid4())
        yield chan.basic_consume(queue=reply.queue, consumer_tag=consumer_tag)
        chQueue = yield self.queue(consumer_tag)
        defer.returnValue((chQueue, chan))

    @defer.inlineCallbacks
    def createSendingChannel(self, address):
        """
        """
        log.msg('new sending channel')
        channel_num = self.next_channel_id 
        self.next_channel_id += 1
        chan = yield self.channel(channel_num)
        yield chan.channel_open()
        defer.returnValue(chan)

    def resolveAddress(self, addr):
        """
        prototype directory thing
        translate topics and other named things to exchange+routing_key
        """
        return (TEST_EXCHANGE, addr,)

class AbstractMessageChannel:
    """
    Somewhere between a Connection and a channel...
    A MSConnection could hold more than one channel

    The connection delegates to me? 
    OR
    The connection subclasses me?
    Which one?
    """

    def __init__(self, msgsrv):
        self.msgsrv = msgsrv
        self.channels = []
        self.queues =  []
        self.exchanges = []
        self.received_buffer = []

    # @defer.inlineCallbacks
    def bind(self, this_addr):
        """
        listen to this topic address
        """
        addr = self.msgsrv.resolveAddress(this_addr)
        self.this_addr = addr
        # yield self._createListeningChannel()

    def connect(self, to_addr):
        """out going messages go here
        """
        addr = self.msgsrv.resolveAddress(to_addr)
        self.to_addr = addr

    @defer.inlineCallbacks
    def _createListeningChannel(self):
        """The deferred newChannel might not be necessary with improvments
        to txamqp
        """
        chan = yield self.msgsrv.newChannel()
        print 'one', chan
        yield chan.channel_open()
        print '2one'
        # reply = yield self.queue_declare(channel=channel)
        reply = yield chan.queue_declare(auto_delete=True)
        print 'one2'
        self.queue_name = reply.queue
        yield chan.exchange_declare(exchange=self.this_addr[0],
        type='topic')
        print 'one3'
        yield chan.queue_bind(queue=reply.queue,
                                exchange=self.this_addr[0],
                                routing_key=self.this_addr[1])
        self.chan = chan
                

    @defer.inlineCallbacks
    def queue_declare(self, channel=None, *args, **keys):
        # channel = channel or self.channel
        reply = yield channel.queue_declare(*args, **keys)
        self.queues.append((channel, reply.queue))
        defer.returnValue(reply)

    @defer.inlineCallbacks
    def exchange_declare(self, channel=None, ticket=0, exchange='',
                         type='', passive=False, durable=False,
                         auto_delete=False, internal=False, nowait=False,
                         arguments={}):
        channel = channel or self.channel
        reply = yield channel.exchange_declare(ticket, exchange, type, passive, durable, auto_delete, internal, nowait, arguments)
        self.exchanges.append((channel,exchange))
        # defer.returnValue(reply)

    @defer.inlineCallbacks
    def consume(self):
        """Consume from named queue returns the Queue object.
        
        Needs to notify msg available, or push msg.
        """
        reply = yield self.chan.basic_consume(queue=self.queue_name, no_ack=True)
        defer.returnValue((yield self.msgsrv.queue(reply.consumer_tag)))

    @defer.inlineCallbacks
    def _queue_reader(self):
        msg = yield self.q.get()
        self.received_buffer.append(msg)
        self.reader.doRead()
        self._queue_reader()

    def sendMessage(self, payload):
        """
        this is where things will get cool...
        the header
        """
        amqp_msg = Content(payload)
        self.chan.basic_publish(exchange=self.to_addr[0],
                                content=amqp_msg,
                                routing_key=self.to_addr[1])

    @defer.inlineCallbacks
    def startReading(self, hack_ref):
        """
        this might similar to consume
        """
        self.reader = hack_ref
        self.q = yield self.consume()
        yield self._queue_reader()

class BaseListener(AbstractMessageChannel):
    """
    """





