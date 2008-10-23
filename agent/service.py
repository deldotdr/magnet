"""
Every messaging task is a Service.
Every Task has a amqp channel.
"""

import os
import commands
import urllib2

from zope.interface import Interface

from twisted.internet import defer
from twisted.internet import reactor
from twisted.application import service
from twisted.application import internet

from qpid.content import Content

from magnet.agent.amqp import AMQPClientFactory

INSTANCE_DATA_BASE_URL = 'http://169.254.169.254/latest/'

class ITask(Interface):
    pass



class BaseTask(object):
    """Instances of this represent a particular thing to do, either produce
    or consume, with a particular exchange, maybe bound to a particular queue 
    and/or maybe filtering on a particular topic.
    """
    channel = None
    topic = None

    @defer.inlineCallbacks
    def start_consume(self, client):
        """start for consumer
        """
        channel = yield client.newChannel()
        yield channel.channel_open()
        reply = yield channel.queue_declare()
        yield channel.queue_bind(queue=reply.queue,
                                exchange=self.exchange,
                                routing_key=self.routing_key)
        yield channel.basic_consume(queue=reply.queue)
        channel.deferred.addCallback(self.gotMessage)
        self.channel = channel
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def start_produce(self, client):
        channel = yield client.newChannel()
        self.channel = channel
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def sendMessage(self, content):
        print '--------------------begin'
        print 'sendMessage ', content
        print 'Task ', self.name
        print 'Exchange ', self.exchange
        print 'routing key ', self.routing_key
        print '---------------------end'
        content = Content(content)
        yield self.channel.channel_open()
        # yield self.channel.exchange_declare(exchange=self.exchange, type="topic", auto_delete=True)
        yield self.channel.exchange_declare(exchange=self.exchange, type="topic")
        self.channel.basic_publish(exchange=self.exchange,
            routing_key=self.routing_key, content=content)
        yield self.channel.channel_close(reply_code=200, reply_text="Ok")


    def gotMessage(self, msg):
        print '--------------------begin'
        print 'gotMessage ', msg
        print 'Task ', self.name
        print 'Exchange ', self.exchange
        print 'routing key ', self.routing_key
        print '---------------------end'
        if msg.method.name == u'close-ok':
            print '// channel closed ', self.name, self.exchange, self.routing_key
            return
        self.operation(msg.content.body)
        self.channel.deferred.addCallback(self.gotMessage)

    def operation(self, *args):
        pass

        
class Task(service.Service, BaseTask):

    name = None
    type = None

    def __init__(self, config):
        # self.exchange = config['exchange']
        self.node_type = config['node_type']
        self.routing_key = config['routing_key']
        self.config = config

    def startService(self):
        self.running = True
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)

    def stopService(self):
        self.running = False
        if self.channel:
            self.channel.channel_close()



class PeriodicTask(internet.TimerService, BaseTask):


    def __init__(self, config):
        self.type = config['type']
        self.name = config['name']
        self.exchange = config['exchange']
        self.routing_key = config['routing_key']
        self.queue = config['queue']
        self.period = config['period']
        self.content = config['content']
        # self.operation = config['operation']
        internet.TimerService.__init__(self, self.period, self.operation)

    def startService(self):
        self.running = True
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)
        internet.TimerService.startService(self)

    @defer.inlineCallbacks
    def operation(self, *args):
        self.sendMessage(self.content)


class Status(Task):

    name = 'status'
    exchange = 'status'
    type = 'produce'

    def operation(self, *args):
        content = args[0]
        self.sendMessage(content)

class ReportHostname(Task):

    name = 'reporthostname'
    type = 'produce'
    exchange = 'announce'

    def startService(self):
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)
        reactor.callLater(0, self.operation)

    def operation(self, *args):
        public_dns_name = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/public-hostname").read()
        private_dns_name = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/local-hostname").read()
        instance_id = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/instance-id").read()
        self.parent.instance_id = instance_id
        self.parent.public_dns_name = public_dns_name
        self.parent.private_dns_name = private_dns_name
#        content = {
#                'public_dns_name':public_dns_name,
#                'private_dns_name':private_dns_name,
#                'instance_id':instance_id}
#         content = str(content)
        content = instance_id
        self.sendMessage(content)



class RunScript(Task):
    """General script running task
    """

    name = 'runscript'
    type = 'consume'
    exchange = 'command'
    queue = ''

    def operation(self, *args):
        """
        Receive script in message. Run script.
        """
        script = args[0]
        cmd = write_script_file(script)
        status, output = commands.getstatusoutput(cmd)
        msg = {'status':status, 'output':output}
        msg = str(msg)
        self.parent.getServiceNamed('status').sendMessage(msg)

class SendScript(Task):

    name = 'sendscript'
    exchange = ''
    type = 'produce'
    script_path = None

    def operation(self, *args):
        script = read_script_file(self.script_path)
        self.sendMessage(script)

class TopicCommandProducer(Task):

    name = 'runscript'
    type = 'produce'
    exchange = 'command'
    script_path = None

    def operation(self, *args):
        script_path = args[0]
        script = read_script_file(script_path)
        self.sendMessage(script)


class TopicConsumer(Task):

    name = 'status'
    type = 'consume'
    exchange = 'status'

    def operation(self, *args):
        msg = args[0]
        print msg

class SetupApps(Task):

    name = 'setupapps'
    type = 'produce'
    topic = 'command'
    script_path = None

    def operation(self, *args):
        script = read_script_file(self.script_path)
        self.sendMessage(script)

class ConfigDictConsumer(Task):
    """Agents accept a message containing a dictionary of config options.
    Update the dictionary with the nodes public and private dns names
    """

    name = 'config_dict'
    exchange = 'config_dict'
    type = 'consume'

    def operation(self, *args):
        config_dict_script_pair = eval(args[0])
        config_dict = config_dict_script_pair[0]
        script_temp = config_dict_script_pair[1]
        print 'config dict', config_dict
        print 'script templ', script_temp
        public_dns_name = self.parent.public_dns_name
        private_dns_name = self.parent.private_dns_name
        config_dict.update({
            'public_dns_name':public_dns_name,
            'private_dns_name':private_dns_name,
            })
        from string import Template
        config_dict = str(config_dict)
        config_script = Template(script_temp).substitute({'config_dict':config_dict})
        print 'config script after temp', config_script
        cmd = write_script_file(config_script)
        status, output = commands.getstatusoutput(cmd)
        msg = {'status':status, 'output':output}
        msg = str(msg)
        self.parent.getServiceNamed('status').sendMessage(msg)




def read_script_file(path):
    f = open(str(path))
    s = f.read()
    f.close()
    return s


def write_script_file(script):
    """write temp script file and return file name to be executed as a
    command.
    """
    # home = os.getenv('HOME')
    # fname = os.path.join(home,'remote_command.sh')
    cur_dir = os.getcwd()
    fname = os.path.join(cur_dir, 'remote_command.sh')
    f = open(fname, 'w')
    f.write(script)
    f.close()
    os.chmod(fname, 0755)
    cmd = fname
    return cmd
    



class AMQPService(service.MultiService):
    """
    A service needs a pre-configured client factory that it can use to make
    clients.
    Service needs to instantiate a client and then do stuff with that
    client. The service may also be some kind of channel factory, as things
    it does may have their own channels.
    Channels can probably be dynamically created and closed.
    """

    instance_id = None

    def __init__(self, config):
        service.MultiService.__init__(self)
        self.host = config['host']
        self.port = config['port']
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
        service.MultiService.startService(self)






