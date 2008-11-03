"""
Every messaging task is a Service.
Every Task has a amqp channel.

Dorian Raymer 2008
"""

import os
import commands
import urllib2
from string import Template

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
        routing_keys = msg.routing_key.split('.')
        self.operation(msg.content.body, routing_keys)
        self.channel.deferred.addCallback(self.gotMessage)

    def operation(self, *args):
        pass

class Conversation(service.Service, BaseTask):
    """Every conversation needs a producer and a consumer.
    For the sake of consistency, all members of a specific conversation
    are derived from here.
    """

    def __init__(self, name, role, exchange, routing_key):
        self.name = name
        self.role = role
        self.exchange = exchange
        self.routing_key = routing_key

    def startService(self):
        self.running = True
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)

    def stopService(self):
        self.running = False
        if self.channel:
            self.channel.channel_close()


class Task(service.Service, BaseTask):

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
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)

    def stopService(self):
        self.running = False
        if self.channel:
            self.channel.channel_close()



class PeriodicTask(internet.TimerService, BaseTask):

    type = 'produce'

    def __init__(self, config):
        self.name = config['name']
        self.exchange = config['exchange']
        self.node_type = config['node_type']
        self.routing_key = self.node_type + '.' + self.name
        self.period = config['period']
        self.content = config['content']
        internet.TimerService.__init__(self, self.period, self.operation)

    def startService(self):
        self.running = True
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)
        internet.TimerService.startService(self)

    def stopService(self):
        self.running = False
        if self.channel:
            self.channel.channel_close()

    @defer.inlineCallbacks
    def operation(self, *args):
        self.sendMessage(self.content)


class Status(Task):

    name = 'status'
    type = 'produce'

    def operation(self, *args):
        content = args[0]
        self.sendMessage(content)

class ReportHostname(Task):

    name = 'running'
    type = 'produce'

    def startService(self):
        client = self.parent.client
        getattr(self, 'start_%s' % self.type)(client)
        reactor.callLater(0, self.operation)

    def operation(self, *args):
        self.parent.get_user_and_meta_data()
        instance_id = self.parent.user_meta_data['instance_id']
        msg = {'instance_id':instance_id} 
        msg = str(msg)
        self.sendMessage(msg)



class RunScript(Task):
    """General script running task
    """

    name = 'runscript'
    type = 'consume'
    queue = ''

    def operation(self, *args):
        """
        Receive script in message. Run script.
        """
        script = args[0]
        cmd = write_script_file(script, var_dict=self.parent.user_meta_data)
        status, output = commands.getstatusoutput(cmd)
        msg = {'status':status, 'output':output}
        msg = str(msg)
        self.parent.getServiceNamed('status').sendMessage(msg)

class SendScript(Task):

    name = 'sendscript'
    type = 'produce'
    script_path = None

    def operation(self, *args):
        script = read_script_file(self.script_path)
        self.sendMessage(script)

class TopicCommandProducer(Task):

    name = 'runscript'
    type = 'produce'
    script_path = None

    def operation(self, *args):
        script_path = args[0]
        script = read_script_file(script_path)
        self.sendMessage(script)


class TopicConsumer(Task):

    name = 'status'
    type = 'consume'

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
    type = 'consume'

    def operation(self, *args):
        config_dict = eval(args[0])
        templ_file = config_dict['templ_file']
        final_path = config_dict['final_path']
        print 'config dict', config_dict
        public_dns_name = self.parent.public_dns_name
        private_dns_name = self.parent.private_dns_name
        config_dict.update({
            'public_dns_name':public_dns_name,
            'private_dns_name':private_dns_name,
            })
        from magnet.util import config_script as cf
        final_conf_str = cf.fill_in_template(templ_file, config_dict)
        cf.write_final_config(final_conf_str, final_path)
        msg = 'it might have worked...'
        self.parent.getServiceNamed('status').sendMessage(msg)

class ConfigTemplateConsumer(Task):
    """Agents accept a message dictionary with key/values:
        config_templ:[str of config template]
        path:[path to write file]
    Use string.Template substituion to fill in dns names and user data.
    Write the final script file to [path]
    """

    name = 'config_templ'
    type = 'consume'

    def operation(self, *args):
        config_templs = eval(args[0])
        output = ''
        for c in config_templs:
            final_path = c[0] 
            config_templ = c[1]
            config_final = Template(config_templ).substitute(self.parent.user_meta_data)
            output += 'writing this config file', final_path, config_final
            output += '\n'
            f = open(final_path, 'w')
            f.write(config_final)
            f.close()
            output += config_final
        status = '0'
        msg = {'status':status, 'output':output}
        msg = str(msg)
        self.parent.getServiceNamed('status').sendMessage(msg)


class AllNodeDnsConsumer(Task):

    name = 'dns'
    type = 'consume'

    def operation(self, *args):
        msg_dict = eval(args[0])
        self.dns_dict = msg_dict
        self.parent.user_meta_data.update(self.dns_dict)
        res_msg = str({'status':0, 'output':'got dns dict'})
        self.parent.getServiceNamed('status').sendMessage(res_msg)





def read_script_file(path):
    f = open(str(path))
    s = f.read()
    f.close()
    return s


def write_script_file(script, var_dict=None):
    """write temp script file and return file name to be executed as a
    command.
    Put env variables at the top.
    """
    if var_dict:
        headers = shell_script_env_var_header(var_dict)
        script = headers + script
    # home = os.getenv('HOME')
    # fname = os.path.join(home,'remote_command.sh')
    cur_dir = os.getcwd()
    print 'writing script in this dir: ', cur_dir
    fname = os.path.join(cur_dir, 'remote_command.sh')
    f = open(fname, 'w')
    f.write(script)
    print 'writing this content: ', script
    f.close()
    os.chmod(fname, 0755)
    cmd = fname
    return cmd
    
def shell_script_env_var_header(user_meta_data):
    header = '#!/bin/bash\n'
    header += '# Auto-generated list of environment variables\n' 
    for k,v in user_meta_data.iteritems():
        header += 'export %s=%s\n' % (k, v,)
    return header




class AMQPService(service.MultiService):
    """
    A service needs a pre-configured client factory that it can use to make
    clients.
    Service needs to instantiate a client and then do stuff with that
    client. The service may also be some kind of channel factory, as things
    it does may have their own channels.
    Channels can probably be dynamically created and closed.
    """

    def __init__(self, config):
        service.MultiService.__init__(self)
        self.host = config['broker_host']
        self.port = config['broker_port']
        self.username = config['broker_username']
        self.password = config['broker_password']
        self.factory = AMQPClientFactory(config)
        self.factory.onConn.addCallback(self.gotClient)

    def startService(self):
        self.connector = reactor.connectTCP(self.host, self.port, self.factory)

    def stopService(self):
        self.connector.disconnect()


    @defer.inlineCallbacks
    def gotClient(self, client):
        yield client.start({"LOGIN":self.username, "PASSWORD":self.password})
        self.client = client
        service.MultiService.startService(self)


class Agent(AMQPService):

    instance_id = None

    def get_user_and_meta_data(self):
        user_data = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "user-data").read()
        user_data = dict([d.split('=') for d in user_data.split()])
        self.user_data = user_data
        public_dns_name = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/public-hostname").read()
        private_dns_name = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/local-hostname").read()
        instance_id = urllib2.urlopen(INSTANCE_DATA_BASE_URL + "meta-data/instance-id").read()
        ami_launch_index = str(urllib2.urlopen(INSTANCE_DATA_BASE_URL + 'meta-data/ami-launch-index').read())
        self.instance_id = instance_id
        meta_data = {
                'this_public_dns_name':public_dns_name,
                'this_private_dns_name':private_dns_name,
                'this_instance_id':instance_id,
                'instance_id':instance_id,
                'ami_launch_index':ami_launch_index,
                }
        self.meta_data = meta_data
        self.user_meta_data = {}
        self.user_meta_data.update(user_data)
        self.user_meta_data.update(meta_data)
 
 








