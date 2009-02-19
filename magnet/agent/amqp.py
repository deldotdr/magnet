
from twisted.internet import defer
from twisted.internet import protocol

from qpid.content import Content
import qpid.spec
from txamqp.protocol import AMQClient



class SimpleAMQClient(AMQClient):

    id = 1

    def serverGreeting(self):
        if self.greetDeferred is not None:
            d, self.greetDeferred = self.greetDeferred, None
            d.callback(self)

    def newChannel(self):
        """auto count channels
        """
        self.id += 1
        return self.channel(self.id - 1)

class AMQPClientFactory(protocol.ClientFactory):
    """capable of producing clients to a specific broker at a specific
    host.
    A Client Factory should be set up to be used by a service

    When a service starts, it will use the Factory to make a new client.
    Then, the service can use the client to make any number of channels to
    use.
    """

    protocol = SimpleAMQClient

    def __init__(self, config):
        self.host = config['broker_host']
        self.vhost = config['broker_vhost']
        self.spec = qpid.spec.load(config['amqp_spec_path'])
        self.onConn = defer.Deferred()

    def buildProtocol(self, addr):
        """the deferred is for the initial creation of the client.
        """
        # We need to override buildProtocol, AMQClient needs the AMQP spec
        # as a constructor parameter
        p = self.protocol(self.spec, vhost=self.vhost)
        p.factory = self
        p.greetDeferred = self.onConn
        return p

