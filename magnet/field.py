"""
Field has to do with network connections.

The primary prtocol is AMQP. Here, the AMQP client adapter, called
AMQPClientFromAgentService, adapts a pole.py service.

It is possible to create other adapters for poles. An http (web or XML-RPC)
protocol could be used instead of AMQP.

The format of the messages on the wire is interchangeable, different
formats defined in particle.py


"""


import os
import uuid
import simplejson as json

from zope.interface import Interface, implements

from twisted.python import components
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.application import service

from txamqp import spec
from txamqp import content
from txamqp.protocol import AMQClient, TwistedDelegate

from magnet import pole
from magnet import particle

class AMQPClientConnectorService(service.MultiService):
    """Field Service Connector.
    This connector is used to keep the pole service
    from having to know anything about the transport connection.
    """

    def __init__(self, reactor, amqpclient, name='magnet'):
        service.MultiService.__init__(self)
        self.reactor = reactor
        self.amqpclient = amqpclient
        self.amqpclient.service.setServiceParent(self)
        self.name = name

    def connect(self, host='localhost', port=5672, username='guest', password='guest', vhost='/', spec_path='.'):
        d = defer.Deferred()
        delegate = TwistedDelegate()
        d.addCallback(self.amqpclient.gotClient, username, password)
        self.host = host
        self.port = port
        spec = self._spec(spec_path)
        self.f = protocol._InstanceFactory(self.reactor,
                self.amqpclient.newConnection(delegate, vhost, spec),
                d)
        return d

    def _spec(self, spec_path):
        return spec.load(spec_path)

    def startService(self):
        service.MultiService.startService(self)
        self.connector = self.reactor.connectTCP(self.host, self.port, self.f)

    def stopService(self):
        self.connector.disconnect()

    @defer.inlineCallbacks
    def gotClient(self, client, username, password):
        yield client.start({"LOGIN":username, "PASSWORD":password})
        self.amqpclient.client = client

    def sendMessage(self, msg, key):
        self.amqpclient.sendMessage(msg, key)

class IAMQPClient(Interface):
    """This defines how the Connector Service should interface
    the client that connects the Pole to the Field (the service to the
    network). The pole service doesn't need to know anything about the
    network, but the names of its mechanics maps on to the AMQP protocol.
    """

    def handleMessage(msg, channel, channel_num, queue):
        pass

    def sendMessage(msg, key):
        pass

    def newConnection(delegate, vhost, spec):
        pass

    def connection_ready(connection):
        """When login to broker succeeds, client creator calls this and
        passes client connection
        """
        pass

class AMQPClientFromPoleService(object):
    """This adapter interfaces a pole service to the AMQP protocol.
    """

    implements(IAMQPClient)

    def __init__(self, service):
        self.service = service
        self.channels = []
        self.exchange = service.exchange
        self.direct_routing_key = service.system_name + '.' \
                                + service.service_name + '.' \
                                + service.token
        self.Xrouting_pattern = service.system_name + '.' \
                                + service.service_name
        self.routing_pattern = service.routing_pattern

    def newConnection(self, delegate, vhost, spec):
        """This isn't the right name...
        """
        return AMQClient(delegate, vhost, spec)

    @defer.inlineCallbacks
    def gotClient(self, client, username, password):
        yield client.start({"LOGIN":username, "PASSWORD":password})
        self.client = client
        channel_num = 1
        channel = yield self.client.channel(channel_num)
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic")
        yield channel.channel_close(reply_code=200, reply_text="Ok")
        # yield self.startDirectConsumer()
        yield self.startTopicConsumer()
        yield self.startProducer()
        self.service.do_when_running()

    @defer.inlineCallbacks
    def startDirectConsumer(self):
        """consume messages sent directly to this agent
        """
        channel_num = 1
        channel = yield self.client.channel(channel_num)
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        yield channel.queue_declare(queue=self.service.token, auto_delete=True)
        yield channel.queue_bind(queue=self.service.token,
                                exchange=self.exchange,
                                routing_key=self.direct_routing_key)
        consumer_tag = str(uuid.uuid4())
        yield channel.basic_consume(queue=self.service.token, consumer_tag=consumer_tag)
        chQueue = yield self.client.queue(consumer_tag)
        chQueue.get().addCallback(self.handleMessage, channel, channel_num, chQueue)
        self.channels.append(channel)
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def startTopicConsumer(self):
        """consume any message sent to this class of agents
        """
        channel_num = 2
        channel = yield self.client.channel(channel_num)
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic")
        reply = yield channel.queue_declare(auto_delete=True)
        yield channel.queue_bind(queue=reply.queue,
                                exchange=self.exchange,
                                routing_key=self.routing_pattern)
        consumer_tag = str(uuid.uuid4())
        yield channel.basic_consume(queue=reply.queue, consumer_tag=consumer_tag)
        chQueue = yield self.client.queue(consumer_tag)
        chQueue.get().addCallback(self.handleMessage, channel, channel_num, chQueue)
#        print 'topic consumer done'
        self.channels.append(channel)

    @defer.inlineCallbacks
    def startProducer(self):
        channel = yield self.client.channel(3)
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        self.channels.append(channel)
        self.send_channel = channel

    def sendMessage(self, message_object, routing_key):
        serialized_message = particle.serialize_application_message(message_object)
        mess_content = content.Content(serialized_message)
        self.send_channel.basic_publish(exchange=self.exchange,
                                        routing_key=routing_key,
                                        content=mess_content)

    @defer.inlineCallbacks
    def handleMessage(self, amqp_message, channel, channel_num, queue):
        """
        Use particle for message serialization/de-serialization.
        """
        message_object = particle.unserialize_application_message(amqp_message.content.body)

        response_message = yield self.service.handleMessage(message_object)

        # Ack when handleMessage succeeds
        channel.basic_ack(delivery_tag=amqp_message.delivery_tag)
        if response_message is not None:
            routing_key = message_object['reply-to']
            self.sendMessage(response_message, routing_key)

        queue.get().addCallback(self.handleMessage, channel, channel_num, queue)

components.registerAdapter(AMQPClientFromPoleService,
                            pole.IPoleService,
                            IAMQPClient)


class IMultiConsumerClient(Interface):
    """Implementing this expresses the intention of using multiple
    consumers within one connection.
    """

    def newClient(delegate, vhost, spec):
        """
        The connector/protocol/transport related class uses this
        """
        pass

    def connectionReady(connection):
        """When login to broker succeeds, client creator calls this and
        passes client connection

        The connector/protocol calls this
        """
        pass

    def createConsumer(key, handler, *args, **kw):
        """
        General method for creating a consumer listening on a particular
        key

        The caller needs to provide a callback function to handle messages
        when they are received.

        The Pole service uses this
        """
        pass

    def destroyConsumer(key):
        """

        The Pole service uses this
        """
        pass

    def createProducer(key, *args, **kw):
        """
        General method for creating a producer sending messages on a
        particular key

        The Pole service uses this
        """
        pass

    def messageReceived(message_object):
        """used to be handleMessage
        decide where the message should go, and pass it on

        Consumers created through create_consumer will provide their own
        handler, this probably shouldn't be used for MultiPoles unless they
        set up actions that aren't held by a role.

        The protocol calls this
        """
        pass

    def sendMessage(msg, key):
        """

        The Pole service uses this
        """
        pass


class AMQPClientFromMultiPoleService(object):

    implements(IMultiConsumerClient)

    client = AMQClient

    def __init__(self, service):
        self.service = service
        self.consumers = {}

    def newClient(self, delegate, vhost, spec):
        c = self.client(delegate, vhost, spec)
        return c

    def connectionReady(self, connection):
        self.connection = connection

    @defer.inlineCallbacks
    def createConsumer(self, key, handler, *args, **kw):
        """key should be unique for each consumer created. This is
        enforceable via the MultiService constraint of unique names for
        it's child services
        """
        channel = yield self.connection.channel(key)
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        yield channel.queue_declare(queue=self.service.token, auto_delete=True)
        yield channel.queue_bind(queue=self.service.token, routing_key=key)

        consumer_tag = str(uuid.uuid4())
        yield channel.basic_consume(queue=self.service.token, consumer_tag=consumer_tag)
        consumer_queue = yield self.connection.queue(consumer_tag)
        consumer_queue.get().addCallback(handler, channel, consumer_queue)


    def messageReceived(self, message_object):
        """
        Consumers created through create_consumer will provide their own
        handler, this probably shouldn't be used for MultiPoles unless they
        set up actions that aren't held by a role.
        """
