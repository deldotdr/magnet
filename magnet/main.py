import os
import sys

from zope.interface import implements

from twisted.python import usage
from twisted.plugin import IPlugin
from twisted.plugin import getPlugins
from twisted.application.service import IServiceMaker
from twisted.internet import reactor

from magnet import field
from magnet import pole


import magnet
# Spec file is loaded from the egg bundle
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

class SendMessageSubCommand(usage.Options):
    """Used by MagnetServiceMaker, below. E.g. sendOne uses 'send'."""
    optParameters = [
            ['exchange', 'e', 'magnet', 'Exchange name (topic)'],
            ['routing_key', 'r', 'test'],
            ['command', 'c', None],
            ['payload', None, ''],
            ]



class Options(usage.Options):
    subCommands = [['send', None, SendMessageSubCommand, 'Send a message']]
    optParameters = [
            ['host', 'h', 'localhost', 'Broker Host'],
            ['port', None, 5672, 'Broker Port'],
            ['username', 'u', 'guest', 'Broker username'],
            ['password', 'p', 'guest', 'Broker password'],
            ['vhost', None, '/', 'Broker vhost'],
            ['spec_path', None, spec_path_def, 'Path to AMQP spec file']
            ]

    def opt_plugins(self):
        for p in getPlugins(pole.IPoleService):
            print p
        sys.exit(0)



class MagnetPluginServiceMaker(object):

    implements(IServiceMaker, IPlugin)
    tapname = "magnet"
    description = "Use this to run Poles"
    options = Options

    def __init__(self, pole):
        """Initialize with a single pole
        """
        self.pole = pole

    def makeService(self, options):

        c = field.IAMQPClient(self.pole)
        connector = field.AMQPClientConnectorService(reactor, c)
        connector.connect(host=options['host'],
                            port=options['port'],
                            username=options['username'],
                            password=options['password'],
                            vhost=options['vhost'],
                            spec_path=options['spec_path'])
        return connector


class MagnetServiceMaker(object):

    implements(IServiceMaker, IPlugin)
    tapname = "magnet"
    description = """Run something implemented with Magnet.
                    Activate a set of Actor services over an AMQP client.
                """
    options = Options


    def makeService(self, options):
        if options.subCommand == 'send':
            send_one = pole.SendOne(exchange=options.subOptions['exchange'],
                    routing_pattern='sendone')
            send_one.send_when_running(options.subOptions['routing_key'],
                                    options.subOptions['command'],
                                    options.subOptions['payload'])
            c = field.IAMQPClient(send_one)
        else:
            plugins = getPlugins(pole.IPoleService)
            c = field.IAMQPClient(plugins.next())
        connector = field.AMQPClientConnectorService(reactor, c)
        connector.connect(host=options['host'],
                            port=options['port'],
                            username=options['username'],
                            password=options['password'],
                            vhost=options['vhost'],
                            spec_path=options['spec_path'])
        return connector
