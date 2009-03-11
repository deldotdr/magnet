
from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from txamqp.content import Content


def log_message(self, msg):
    print '\n--------------------begin'
    print 'gotMessage ', msg
    print 'Task ', self.name
    print 'Exchange ', self.exchange
    print 'routing key ', self.routing_key
    print '---------------------end\n'

class Actor(object):
    """Instances of this represent a particular thing to do. 
    Actors are consumers of messages. Each Actor listens to a queue bound
    to an exchange. Depending on the intended behavior of the Actor/the
    nature of the action, the queue my be unique to the instance or common 
    to a class of Actors.
    """
    channel = None
    topic = None
    queue = None # change this to listen n specific queue

    def __init__(self, name, actor_type='consumer', exchange=''):
        """An Actors name is used for as the name of the service and as the
        base of the routing key. The name should represent the overall
        function of the action.
        actor_type can  be consumer or procucer
        exchange can be specified but is usually taken from the exchange of
        the Agent.
        """
        self.name = name
        self.actor_type = actor_type
        self.exchange = exchange

    def startService(self):
        self.running = True
        if not self.exchange:
            self.exchange = self.parent.exchange
        client = self.parent.client
        getattr(self, 'start_%s' % self.actor_type)(client)

    @defer.inlineCallbacks
    def start_consumer(self, client):
        """start for consumer
        """
        channel = yield client.newChannel()
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic")
        if self.queue:
            reply = yield channel.queue_declare(queue=self.queue)
        else:
            reply = yield channel.queue_declare()

        yield channel.queue_bind(queue=reply.queue,
                                exchange=self.exchange,
                                routing_key=self.routing_key)
        yield channel.basic_consume(queue=reply.queue)
        channel.deferred.addCallback(self.gotMessage)
        self.channel = channel
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def start_producer(self, client):
        channel = yield client.newChannel()
        self.channel = channel
        # yield self.channel.channel_open()
        # yield self.channel.exchange_declare(exchange=self.exchange, type="topic")
        defer.returnValue(self.channel)

    @defer.inlineCallbacks
    def sendMessage(self, content):
        log_message(self, content)
        content = Content(content)
        yield self.channel.channel_open()
        # yield self.channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        yield self.channel.exchange_declare(exchange=self.exchange, type="topic")
        self.channel.basic_publish(exchange=self.exchange,
            routing_key=self.routing_key, content=content)
        yield self.channel.channel_close(reply_code=200, reply_text="Ok")


    def gotMessage(self, msg):
        log_message(self, msg)
        if msg.method.name == u'close-ok':
            print '// channel closed ', self.name, self.exchange, self.routing_key
            return
        routing_keys = msg.routing_key.split('.')
        result = self.operation(msg.content.body, routing_keys)
        if result:
            self.channel.basic_ack(delivery_tag=msg.delivery_tag)
        self.channel.deferred.addCallback(self.gotMessage)

    def route_action(self, msg):
        """Determine what method to call based on the routing key.
        A multi-part routing key means this actor has multiple methods
        available and that method should be called.
        A single-part routing key means there is only one method of action
        supported by the actor; that name should be the same as the name of
        the actor
        """
        routing_keys = msg.routing_key.split('.')
        if len(routing_keys) > 1:
            getattr(self, 'action_%s' % routing_keys[1])


    def action(self, args):
        """Simple one function Actors must implement this method. 
        Multi-method Actors can implement this method but it is not
        required. 
        """
        pass


class Actor(service.Service):

    name = None
    type = None

    def __init__(self, config):
        self.exchange = config['exchange']
        self.node_type = config['node_type']
        # self.routing_key = config['routing_key']
        self.routing_key = self.node_type + '.' + self.name
        self.config = config

    def startService(self):
        self.running = True
        if not self.exchange:
            self.exchange = self.parent.exchange
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)

    def stopService(self):
        self.running = False
        if self.channel:
            self.channel.channel_close()

    def startChannel(self)












