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

from twisted.python import log
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

class AMQPConnector(service.Service):
    """Field Service Connector.
    This connector is used to keep the pole service
    from having to know anything about the transport connection.

    This special connector is needed to login to the broker (normally
    TCPClient would be used).

    This is a hack to make amqp work with the application framework.
    """

    def __init__(self, channel_manager, host='localhost', port=5672,
                        username='guest', password='guest',
                        vhost='/', spec_path='.'):
        self.channel_manager = channel_manager
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec = self._spec(spec_path)
        log.msg('Connector __init__')

    def connect(self):
        d = defer.Deferred()
        delegate = TwistedDelegate()
        amqpclient = AMQClient(delegate, self.vhost, self.spec)
        d.addCallback(self.gotConnection)
        f = protocol._InstanceFactory(reactor, amqpclient, d)
        return reactor.connectTCP(self.host, self.port, f)

    def _spec(self, spec_path):
        return spec.load(spec_path)

    def startService(self):
        service.Service.startService(self)
        self.tcpconnection = self.connect()

    def stopService(self):
        self.tcpconnection.disconnect()

    @defer.inlineCallbacks
    def gotConnection(self, connection):
        """General handler for the brokers initial response.
        This should be apart of the client library.
        It is here in case a redirect is needed since a redirect requires
        the tcp connection to be rebuilt for a new host name.
        Until then, handling of redirect should happen here.
        """
        yield connection.start({"LOGIN":self.username, "PASSWORD":self.password})
        # self.amqpclient.connection = connection
        self.channel_manager.makeConnection(connection)


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
        # service.Service.startService(self)
        service.MultiService.startService(self)
        self.connector = self.reactor.connectTCP(self.host, self.port, self.f)

    @defer.inlineCallbacks
    def stopService(self):
        yield self.connector.disconnect()

    @defer.inlineCallbacks
    def gotClient(self, client, username, password):
        yield client.start({"LOGIN":username, "PASSWORD":password})
        self.amqpclient.client = client

    def sendMessage(self, msg, key):
        self.amqpclient.sendMessage(msg, key)


class IAMQPClientFactory(Interface):
    """
    """
    pass



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
    connection_class = AMQClient

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
        # yield channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        self.channels.append(channel)
        self.send_channel = channel

    def sendMessage(self, message_object, routing_key, payload=''):
        """the kw payload is here to for future design.
        Now, assume anything in message_object named payload is a string
        """
        # serialized_message = particle.serialize_application_message(message_object)
        payload = message_object.pop('payload', 'nada')
        msg_props = {'headers':message_object}
        msg_content = content.Content(payload, properties=msg_props)
        self.send_channel.basic_publish(exchange=self.exchange,
                                        routing_key=routing_key,
                                        content=msg_content)

    @defer.inlineCallbacks
    def handleMessage(self, amqp_message, channel, channel_num, queue):
        """
        Use particle for message serialization/de-serialization.
        """
        # message_object = particle.unserialize_application_message(amqp_message.content.body)
        message_object = amqp_message.content.properties['headers']
        message_object['payload'] = amqp_message.content.body

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

@defer.inlineCallbacks
def mesg_queue_gen(q):
    while 1:
        yield q.get()

class ChannelWrapper(object):
    """HACK!!! Don't let this go on for too long! ;-)
        May 8, 2009
    """

    def __init__(self, channel, queue):
        self.channel = channel
        self.queue = queue

    def send_message(self, exchange, routing_key, agent_message):
        """This is a leak :( The Agent wants to send a message without
        knowing about amqp specific configuration, but then the useful
        delivery attributes are hidden, or redundantly re-exposed.
        AMQP calls it basic produce because it should be a general middle
        ware thing.
        """
        serialized_agent_message = particle.serialize_application_message(agent_message)
        amqp_content = content.Content(serialized_agent_message)
        self.channel.basic_publish(exchange=exchange,
                routing_key=routing_key, content=amqp_content)

    @defer.inlineCallbacks
    def listen_to_queue(self, handler):
        log.msg('start listen to queue')
        amqp_message = yield self.queue.get()
        log.msg('yieled message')
        message_object = particle.unserialize_application_message(amqp_message.content.body)
        handler(message_object)



class ChannelManagementLayer(object):
    """Simple agent manager.
    Coordinates agents with amqp channels.
    """
    active = False
    next_channel_id = 1

    def __init__(self):
        self.agents = {}
        self.connection = None

    def start(self):
        self.active = True
        for name in self.agents.keys():
            reactor.callLater(0, self.startAgent, name)

    def stop(self):
        self.active = False
        for name in self.agents.keys():
            self.stopAgent(name)

    def makeConnection(self, connection):
        """
        when the connector finishes setting up the tcp connection, it will
        pass the amqp connection class here.
        """
        log.msg('makeConnection channel manag')
        self.connection = connection
        self.start()

    def addAgent(self, agent):
        if self.agents.has_key(agent.name):
            raise KeyError("already have agent named %s" % agent.name)
        self.agents[agent.name] = agent
        if self.active:
            self.startAgent(agent.name)

    @defer.inlineCallbacks
    def startAgent(self, name):
        agent = self.agents.get(name)
        incoming_queue, channel = yield self._new_consuming_channel(
                                    agent.exchange, agent.resource_name,
                                    unique_id=agent.unique_id)
        agent_chan = ChannelWrapper(channel, incoming_queue)
        agent.activateAgent(agent_chan)

    def stopAgent(self, name):
        self.agents.get(name).stopAgent()

    @defer.inlineCallbacks
    def _new_consuming_channel(self, exchange, routing_key, unique_id=None):
        log.msg('new consumer channel')
        channel_num = self.next_channel_id
        self.next_channel_id += 1
        channel = yield self.connection.channel(channel_num)
        yield channel.channel_open()
        # need to move this to configuration service
        yield channel.exchange_declare(exchange=exchange, type="direct")
        reply = yield channel.queue_declare(auto_delete=True)
        yield channel.queue_bind(queue=reply.queue,
                                exchange=exchange,
                                routing_key=routing_key)
        if unique_id is not None:
            yield channel.queue_bind(queue=reply.queue,
                                    exchange=exchange,
                                    routing_key=unique_id)


        consumer_tag = str(uuid.uuid4())
        yield channel.basic_consume(queue=reply.queue, consumer_tag=consumer_tag)
        chQueue = yield self.connection.queue(consumer_tag)
        defer.returnValue((chQueue, channel))




class AMQPClientFromAgent:
    """I adapt an agent to look like an amqp client.
    I set up consumers for the agent
    I am the AMQP side of an agent management system.
    """

    implements(IAMQPClient)

    def __init__(self, agent):
        self.agent = agent
        self.next_channel_id = 1
        self.exchange = agent.exchange
        self.routing_pattern = agent.resource_name



    def createConsumer(self, key, handler):
        """
        should handler be passed in and published to when messages come in,
        or should the deferred queue be returned so the caller can decide
        when to get from it?
        """

        channel_num = self.next_channel_id
        self.next_channel_id += 1
        channel = yield self.connection.channel(channel_num)
        yield channel.channel_open()
        # need to move this to configuration service
        yield channel.exchange_declare(exchange=self.exchange, type="topic")
        reply = yield channel.queue_declare(auto_delete=True)
        yield channel.queue_bind(queue=reply.queue,
                                exchange=self.exchange,
                                routing_key=self.routing_pattern)

        consumer_tag = str(uuid.uuid4())
        yield channel.basic_consume(queue=reply.queue, consumer_tag=consumer_tag)
        chQueue = yield self.connection.queue(consumer_tag)


    def consume_basic_deliver(self, amqp_message):
        """
        receive messages of basic_deliver method
        """
        agent_message = amqp_message.content.body




class Buffer:
    """An active DeferredQueue.
    Instead of using get for each object in the queue,

    """
