"""
Basic Protocol - Factory pattern implementation for the AMQP protocol using
txAMQP
"""
import txamqp

import misted
# Spec file is loaded from the egg bundle. Hard code 0-8 for now...
spec_path_def = os.path.join(magnet.__path__[0], 'spec', 'amqp0-8.xml')

class AMQPProtocol(txamqp.protocol.AMQClient):
    """
    """

class AMQPFactory(protocol.ClientFactory):
    """
    """

    protocol = AMQPProtocol

    def __init__(self, username='guest', password='guest', 
                        vhost='/', spec_path=spec_path_def):
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec_path = spec_path

    def buildProtocol(self, addr):
        d = defer.Deferred()
        delegate = TwistedDelegate()
        p = self.protocol(delegate, self.vhost, self.spec)
        p.factory = self
        d.addCallback(self.gotConnection)
        return p 


def factory_using_clientCreator(username='guest', password='guest',
                                vhost='/', spec_path=spec_path_def):

    c = protocol.ClientCreator(reactor, AMQPProtocol, username, password,
                                vhost, spec_path)



if __name__ == '__main__':
    from twisted.internet import reactor

    f = AMQPFactory()
    reactor.connectTCP('amoeba.ucsd.edu', 5672, f)
    reactor.run()
