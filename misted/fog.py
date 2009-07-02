import sys

from zope.interface import implements

from twisted.internet import interfaces 
from twisted.internet import base
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

from misted import river


# class MistedConnection(river.AbstractMessageChannel):
class MistedConnection(object):
    """Messaging Service Connection.
    Connections wrap around the "physical" lower level part of the network.

    Use an AMQP connection to start create a channel.

    This is the thing that glues the application protocol to the underlying
    transport. 

    XXX: This should inherit something with logging and be persistable
    """
    
    implements(interfaces.ITransport)

    connected = 0
    disconnecting = False # where does this go?

    def __init__(self, mschan, protocol, reactor=None):
        """
        """
        self.mschan = mschan
        self.protocol = protocol

    def write(self, data):
        """
        push data into ms
        
        this is where some intelligence passes application header data
        separate from undifferentiated application payload
        """
        # pushText(self.chan, data)
        print 'Connection write', data
        self.mschan.sendMessage(data)

    def writeSequence(self, data):
        """
        """

    def doRead(self):
        """this is supposed to get data from mschan and pass to
        protocol.dataReceived
        """

    # @inlineCallbacks
    def doReadFromQueue(self):
        """
        formalize into consumer thing..
        """
        # data = yield self.checkQueue()
        data = self.mschan.received_buffer.pop()
        print 'data', data.content.body
        self.protocol.dataReceived(data.content.body)
        # self.doReadFromQueue()


    def loseConnection(Self):
        """
        """

    def getPeer(self):
        """
        """

    def getHost(self):
        """
        """

    def failIfNotConnected(self, err):
        print 'failIfNotConnected', err

    def startReading(self):
        """
        this has to do with making sure the deliver queue is read
        should it be defined here or in AbstractMessageChannel?
        """


class BaseClient(MistedConnection):
    """Base client for MS connections

    does the managment work for mschan
    """

    def _finishInit(self, whenDone, mschan, error, reactor):
        """what's this dance?

        why is the protocol arg None?
        Because it get's set during the doConnect call
        """
        if whenDone:
            MistedConnection.__init__(self, mschan, None, reactor)
            reactor.callLater(0, whenDone)
        else:
            # reactor.callLater(0, self.failIfNotConnected, error)
            pass

    def createMSChannel(self):
        """make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        mschan = river.AbstractMessageChannel(self.msgsrv)
        return mschan

    def resolveAddress(self):
        """
        set self.realAddress
        this is passed as addr to the protocol buildProtocol(addr)
        might not need to be resolved here?
        """
        print 'resolveAddress'
        self._setRealAddress(self.to_addr) 

    def _setRealAddress(self, address):
        self.realAddress = address
        self.doConnect()

    @inlineCallbacks
    def doConnect(self):
        """
        this is where the application protocol is instantiated

        this is where mschan is configured; the analog to connecting a
        socket to a host
        """
        yield self.mschan._createListeningChannel()
        self.mschan.connect(self.realAddress)
        self.protocol = self.connector.buildProtocol(None)
        self.protocol.makeConnection(self)
        # self.doReadFromQueue()

    @inlineCallbacks
    def checkQueue(self):
        data = yield self.queue.get()
        returnValue(data)

class MistedClient(BaseClient):
    """knows how to get amqp channels

    create an instance of a mschannel, give it to Connection

    implements address getting of ITransport
    """

    def __init__(self, to_addr, from_addr, connector, reactor=None, msgsrv=None):
        """this knows what topics to bind the ms channel to (listen)

        create the mschan and pass it on to the baseclient/Connection

        """
        self.to_addr = to_addr
        self.from_addr = from_addr
        self.connector = connector
        self.msgsrv = msgsrv # where should this end up being set?
        # try:
        mschan = self.createMSChannel()
        mschan.bind(from_addr)

        whenDone = self.resolveAddress # this thing figures out the real
                                        # amqp address to talk to
        err = None
        # What's this dance?
        self._finishInit(whenDone, mschan, err, reactor)



class MistedServer(MistedConnection):
    """
    This is like a socket which came from an accept()

    
    """

    def __init__(self, mschan, protocol, client_address, server, sessionno,
            reactor):
        MistedConnection.__init__(self, mschan, protocol, reactor)
        self.server = server
        self.client_address = client_address # ?
        self.sessionno = sessionno # ?
        self.address = client_address# [0] # ?

        self.startReading() # a Connection responsability
        self.connected = 1


class MistedListeningPort:
    """
    Basically a simple consumer that can have converstions.

    It's listening on a topic, and it creates instances of a server
    handling protocol for each message, or group of messages in a
    interaction.
    """

    implements(interfaces.IListeningPort)

    connected = 0

    transport = MistedServer
    sessionno = 0 # is this needed?
    interface = ''

    hack_started = False

    def __init__(self, topic_address, factory, backlog=50, interface='',
            reactor=None, msgsrv=None):
        self.this_address = topic_address
        self.factory = factory
        self.backlog = backlog
        self.interface = interface
        self.msgsrv = msgsrv
        self.reactor = reactor

    def createMSChannel(self):
        mschan = river.AbstractMessageChannel(self.msgsrv)
        return mschan

    @inlineCallbacks
    def startListening(self):
        """
        configuration of mschan
        """
        mschan = self.createMSChannel()
        mschan.bind(self.this_address)

        # self._realPortNumber ?
        self.factory.doStart()

        # implement
        # mschan.listen
        yield mschan._createListeningChannel()
        self.connected = True
        # implement
        self.mschan = mschan
        # implement
        self.mschan.startReading(self)

    def doRead(self):
        """Not exactly doRead, but this function will get a message from
        self.mschan and decide what to do with it...
        in tcp, a new socket is created, a new protocol instance is built
        by factory, and a new instance of Server glues the 3 together

        this should be called when soemthing else gets a message...
        """
        print 'server do read'
        if not self.hack_started:
            self.hack_started = True
            self.mschan 
            addr = ''# address, get it
            protocol = self.factory.buildProtocol(addr)
            s = self.sessionno
            self.sessionno = s + 1
            # should self.mschan really go in here?
            transport = self.transport(self.mschan, protocol, addr, self, s, self.reactor)
            # transport = self._preMakeConnection(transport)
            protocol.makeConnection(transport)
            self.reactor.callLater(0, transport.doReadFromQueue)

    def stopListening(self):
        """
        """

    def getHost(self):
        """
        Get the host that this port is listening for.

        Returns an IAddress provider.
        """

class MistedConnector(base.BaseConnector):
    """
    The baseConnector knows what to do with the Factory
    The Client knows how to get and use the real underlying transport.
    """

    def __init__(self, to_addr, factory, timeout, from_addr,
            reactor=None, msgsrv=None):
        self.to_addr = to_addr
        self.from_addr = from_addr
        self.msgsrv = msgsrv
        base.BaseConnector.__init__(self, factory, timeout, reactor)

    def _makeTransport(self):
        c = MistedClient(self.to_addr, self.from_addr, self,
                self.reactor, self.msgsrv)
        # c.doConnect() # should NOT need this.
        return c


def connectMS(to_addr, factory, timeout=30, from_addr=None, reactor=None, msgsrv=None):
    c = MistedConnector(to_addr, factory, timeout, from_addr, reactor, msgsrv)
    c.connect()
    return c

def listenMS(topic_address, factory, reactor, msgsrv):
    p = MistedListeningPort(topic_address, factory, reactor=reactor,
            msgsrv=msgsrv)
    p.startListening()
    return p

from twisted.web.client import HTTPClientFactory
def XXtest_client(_amqp_conn):
    """need one of these available to MS Connection
    """
    global amqp_conn
    amqp_conn = _amqp_conn
    print 'got amqp_conn'
    f = HTTPClientFactory('google.com')
    c = connectMS('test_mshttp_server', f, from_addr='test_mshttp_client')




if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "%s path_to_spec content" % sys.argv[0]
        sys.exit(1)
    spec = txamqp.spec.load(sys.argv[1])
    d = amqpConnnectionFactory(spec)
    d.addCallback(test_client)
    reactor.run()


