"""
Consume messages from crawler/fetcher erddap nodes.
Messages will have:
    datasetId
    diff (yes/no)

"""

import commands
import urllib2

from zope.interface import Interface

from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from qpid.content import Content

from magnet.agent.amqp import AMQPClientFactory

META_DATA_BASE = 'http://169.254.169.254/latest/meta-data/'

class ITask(Interface):
    pass



class BaseTask(object):
    """Instances of this represent a particular thing to do, either produce
    or consume, with a particular exchange, maybe bound to a particular queue 
    and/or maybe filtering on a particular topic.
    """
    channel = None
    topic = None

    def __init__(self, config):
        self.type = config['type']
        self.name = config['name']
        self.exchange = config['exchange']
        self.routing_key = self.topic + config['base_routing_key']
        self.queue = config['queue']
        self.operation = config['operation']

    def startService(self):
        service.Service.startService(self)
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)

    def stopService(self):
        service.Service.stopService(self)
        if self.channel:
            self.channel.channel_close()


    @defer.inlineCallbacks
    def start_consume(self, client):
        """start for consumer
        """
        channel = yield client.newChannel()
        yield channel.channel_open()
        yield channel.queue_declare(queue=self.queue)
        yield channel.queue_bind(queue=self.queue,
                                exchange=self.exchange,
                                routing_key=self.routing_key)
        yield channel.basic_consume(queue=self.queue)
        channel.deferred.addCallback(self.gotMessage)
        self.channel = channel
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def start_produce(self, client):
        print 'start produce'
        channel = yield client.newChannel()
        self.channel = channel
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def sendMessage(self, content):
        content = Content(content)
        yield self.channel.channel_open()
        yield self.channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        self.channel.basic_publish(exchange=self.exchange,
            routing_key=self.routing_key, content=content)
        yield self.channel.channel_close(reply_code=200, reply_text="Ok")


    def gotMessage(self, msg):
        print "Task ", self.name, "gotMessage", msg.content.body
        self.operation(msg.content.body)
        self.channel.basic_ack(delivery_tag=msg.delivery_tag)
        self.channel.deferred.addCallback(self.gotMessage)

    def operation(self, *args):
        pass

        
class Task(service.Service, BaseTask):

    name = None
    type = None

    def __init__(self, config):
        self.exchange = config['exchange']
        self.routing_key = config['routing_key']
        self.queue = config['queue']
        self.config = config


class PeriodicTask(internet.TimerService, BaseTask):

    topic = None

    def __init__(self, config):
        self.type = config['type']
        self.name = config['name']
        self.exchange = config['exchange']
        self.routing_key = self.topic + config['base_routing_key']
        self.queue = config['queue']
        self.period = config['period']
        self.content = config['content']
        # self.operation = config['operation']
        internet.TimerService.__init__(self, self.period, self.operation)

    def startService(self):
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)
        internet.TimerService.startService(self)

    @defer.inlineCallbacks
    def operation(self, *args):
        self.sendMessage(self.content)


class Status(Task):

    name = 'status'
    type = 'produce'
    topic = 'status'

    def operation(self, *args):
        content = args[0]
        self.sendMessage(content)

class ReportHostname(Task):

    name = 'reporthostname'
    type = 'produce'
    exchange = None
    topic = 'status'
    queue = ''

    def start(self, client):
        getattr(self, 'start_%s' % self.type)(client)
        reactor.callLater(0, self.operation)

    def operation(self, *args):
        public_hostname = urllib2.urlopen(META_DATA_BASE + "public-hostname").read()
        instance_id = urllib2.urlopen(META_DATA_BASE + "instance-id").read()
        content = {'hostname':public_hostname, 'instance_id':instance_id}
        content = str(content)
        self.sendMessage(content)



class RunScript(Task):
    """General script running task
    """

    name = 'runscript'
    type = 'consume'
    exchange = ''
    topic = 'status'
    queue = ''

    def operation(self, *args):
        """
        Receive script in message. Run script.
        """
        script = args[0]
        status, output = commands.getstatusoutput(script)
        msg = {'status':status, 'output':output}
        msg = str(msg)
        self.parent.getServiceNamed('status').sendMessage(msg)

class SendScript(Task):

    name = 'sendscript'
    type = 'produce'
    topic = 'command'
    script_path = None

    def operation(self, *args):
        script = read_script_file(self.script_path)
        self.sendMessage(script)


class NodeStatusConsumer(Task):

    name = 'status'
    type = 'consumer'
    topic = 'status'


class SetupApps(Task):

    name = 'setupapps'
    type = 'produce'
    topis = 'command'
    script_path = None

    def operation(self, *args):
        script = read_script_file(self.script_path)
        self.sendMessage(script)


def read_script_file(path):
    f = open(path)
    s = f.read()
    f.close()
    return s





class AMQPService(service.MultiService):
    """
    A service needs a pre-configured client factory that it can use to make
    clients.
    Service needs to instantiate a client and then do stuff with that
    client. The service may also be some kind of channel factory, as things
    it does may have their own channels.
    Channels can probably be dynamically created and closed.
    """


    def __init__(self, config, tasks=None):
        service.MultiService.__init__(self)
        self.host = config['host']
        self.port = config['port']
        self.exchange = config['exchange']
        self.username = config['username']
        self.password = config['password']
        self.factory = AMQPClientFactory(config)
        self.factory.onConn.addCallback(self.gotClient)

    def startService(self):
        reactor.connectTCP(self.host, self.port, self.factory)

    @defer.inlineCallbacks
    def gotClient(self, client):
        yield client.start({"LOGIN":self.username, "PASSWORD":self.password})
        self.client = client
        service.Service.startService(self)
        for service in self:
            service.startService()






class xxAMQPService(service.Service):
    """
    A service needs a pre-configured client factory that it can use to make
    clients.
    Service needs to instantiate a client and then do stuff with that
    client. The service may also be some kind of channel factory, as things
    it does may have their own channels.
    Channels can probably be dynamically created and closed.
    """


    def __init__(self, config, tasks=None):
        self.host = config['host']
        self.port = config['port']
        self.exchange = config['exchange']
        self.username = config['username']
        self.password = config['password']
        self.factory = AMQPClientFactory(config)
        self.factory.onConn.addCallback(self.gotClient)
        self.tasks = tasks or {}

    def startService(self):
        service.Service.startService(self)
        reactor.connectTCP(self.host, self.port, self.factory)

    @defer.inlineCallbacks
    def gotClient(self, client):
        yield client.start({"LOGIN":self.username, "PASSWORD":self.password})
        channel = yield client.newChannel()
        yield channel.channel_open()
        yield channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)

        content = Content("Message Service: Server Greeting OK")
        channel.basic_publish(exchange=self.exchange,
            routing_key='test', content=content)

        yield channel.channel_close(reply_code=200, reply_text="Ok")

        self.client = client
        for name in self.tasks:
            reactor.callLater(0, self.startTask, name)

    def addService(self, s):
        self.addTask(s)

    def addTask(self, task):
        """ add a new task to the service
        """
        name = task.name
        if self.tasks.has_key(name):
            raise KeyError('remove %s first' % name)
        self.tasks[task.name] = task
        if self.running:
            self.startTask(name)

    def removeTask(self, name):
        """remove task by name from the service
        """


    def startTask(self, name):
        """
        start a sub service
        """
        if self.tasks.has_key(name):
            self.tasks[name].start(self.client)

    def stopTask(self, name):
        """stop a task by name
        """
        if self.tasks.has_key(name):
            self.tasks[name].stop()


