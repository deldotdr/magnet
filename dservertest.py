
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet import protocol
from twisted.protocols import basic
from twisted.internet import task

from twisted.web.client import HTTPClientFactory
from twisted.web import server, proxy, resource, static
from twisted.python import log

from misted.amqp import AMQPClientCreator
from misted import fog
from misted.hot_pocket import PocketDynamo

BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_PORT = 5672

class Root(resource.Resource):
    """
    """

    def getChild(self, name, request):
        print 'get child %%%%%%%'
        print name
        if name == '':
            return self

    def render_GET(self, request):
        print 'render GET # # # # # # '

        return 'Heelllllllllllooooooo'

class Utility(object):

    def __init__(self, dynamo):
        self.dynamo = dynamo
        self.readers_len = 0

    def check_pockets(self):
        new_len = len(self.dynamo.getReaders())
        if new_len > self.readers_len:
            return self.whois()


    def add(self, a, b):
        return a + b

    def whois(self):
        pkts = self.dynamo.getReaders() 
        o = ''
        for pkt in pkts:
            o += repr(pkt) +'\r\n'
        return o

class UtiliProtocol(basic.LineReceiver):

    def lineReceived(self, line):
        print 'Received: ', self.transport, line
        if line[0:3] == 'add':
            _, a, b = line.split(',')
            c = self.factory.add(int(a), int(b))
            self.sendLine('add'+str(c))
            return
        if line[0:5] == 'whois':
            a = self.factory.whois()
            self.transport.write(a)
            return

class UtilFactory(protocol.ServerFactory):
    protocol = UtiliProtocol

    def __init__(self, util):
        self.util = util
        self.task = task.LoopingCall(self.check_pockets)
        self.pinsts = []
        self.task.start(1)

    def buildProtocol(self, addr):
        p = protocol.ServerFactory.buildProtocol(self, addr)
        self.pinsts.append(p)
        return p

    def check_pockets(self):
        pkts = self.util.check_pockets()
        if pkts:
            for p in self.pinsts:
                p.sendLine(pkts)


    def add(self, a, b):
        return self.util.add(a, b)

    def whois(self):
        return self.util.whois()

class MessageEcho(protocol.Protocol):

    def dataReceived(self, data):
        print 'dataReceived', data
        self.transport.write(data)

class MSEchoFactory(protocol.ServerFactory):
    protocol = MessageEcho


@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)
    yield client.authenticate(clientCreator.username,
            clientCreator.password)

    dynamo = PocketDynamo(reactor, client)

    # root = static.File('.')
    root = Root()
    # f = server.Site(proxy.ReverseProxyResource('amoeba.ucsd.edu', 80, ''))
    # f = server.Site(root)
    # f = MSEchoFactory()

    util = Utility(dynamo)
    f = UtilFactory(util)

    dynamo.listenMS(['amq.direct','test-http-server'], f)
    dynamo.run()

if __name__ == '__main__':
    main(reactor)
    reactor.run()
