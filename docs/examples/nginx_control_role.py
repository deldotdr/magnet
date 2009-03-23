import sys
sys.path.insert(0, '/Users/dorian/test/magnet')

from magnet import pole
from magnet import field

class NginxControl(pole.BasePole):

    def action_start(self, message_object):
        """
        """
        print 'start', message_object

    def action_stop(self, message_object):
        """
        """

    def action_restart(self, message_object):
        """
        """

    def action_reload(self, message_object):
        """
        """


class NginxConfig(pole.BasePole):

    def action_addto_loadbalancer(self, message_object):
        """
        """

    def action_removefrom_loadbalancer(self, message_object):
        """
        """

    def action_add_server(self, message_object):
        """
        """

    def action_remove_server(self, message_object):
        """
        """


def makeService():
    from twisted.internet import reactor
    from magnet.field import IAMQPClient

    p = NginxControl('magnet', 'test', 'test', token='nginx')
    a = IAMQPClient(p)
    spec_path = '/Users/dorian/test/magnet/amqp0-8.xml'
    connector = field.AMQPClientConnectorService(reactor, a)
    connector.connect(host='amoeba.ucsd.edu', spec_path=spec_path)
    connector.startService()
    reactor.run()
makeService()
