
from twisted.internet import defer
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.application import service

from txamqp.protocol import AMQClient
import txamqp.spec


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
    """Capable of producing clients to a specific broker at a specific
    host.
    A Client Factory should be set up to be used by a service.
    When a service starts, it will use the Factory to make a new client.
    Then the service can use the client to make any number of channels to
    use.
    """

    protocol = SimpleAMQClient

    def __init__(self, spec, vhost):
        self.vhost = vhost
        self.spec = txamqp.spec.load(spec)
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


class AMQPConnectionService(service.MultiService):
    """
    A service needs a pre-configured client factory that it can use to make
    clients.
    Service needs to instantiate a client and then do stuff with that
    client. The service may also be some kind of channel factory, as things
    it does may have their own channels.
    Channels can probably be dynamically created and closed.
    """

    def __init__(self, host, port=5672, username='guest', password='guest',
            spec='', vhost='/'):
        service.MultiService.__init__(self)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.factory = AMQPClientFactory()
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



