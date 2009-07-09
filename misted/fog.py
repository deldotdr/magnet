import sys

from zope.interface import implements

from twisted.internet import base
from twisted.internet import reactor
from twisted.internet import interfaces 
from twisted.python import log
from twisted.python.util import unsignedID
from twisted.persisted import styles

# minimize use of deferreds if possible
from twisted.internet.defer import inlineCallbacks, returnValue


class AbstractPocket(log.Logger, styles.Ephemeral, object):
    """Basis of pocket based connections (like abstract.FileDescriptor in
    twisted)
    Foundation of a messaging based Transport

    Not much going on here except formalities, following the ITransport
    interface, and ititing state variables
    """

    implements(interfaces.ITransport)

    connected = 0
    disconnected = 0
    disconnecting = 0

    def __init__(self, reactor, dynamo):
        self.reactor = reactor
        self.dynamo = dynamo

    def write(self, data):
        """
        sockets buffer this, but maybe this doesn't need to...
        depends on if amqp client is local
        """

    def writeSequence(self, iovec):
        """why is it iovec in tcp
        does messaging need this?
        """
        pass

    def loseConnection(self):
        """
        """

    def connectionLost(self, reason):
        """
        """

class Connection(AbstractPocket):
    """Messaging Service Connection.
    Connections wrap around the "physical" lower level part of the network.

    Use an AMQP connection to start create a channel.

    This is the thing that glues the application protocol to the underlying
    transport. 

    XXX: This should inherit something with logging and be persistable
    """
    
    # this is where we get to create IMSTransport, if needed
    # implements(interfaces.ITransport)


    def __init__(self, pkt, protocol, reactor=None):
        """
        """
        AbstractPocket.__init__(self, reactor, dynamo)
        self.pocket = pkt
        self.protocol = protocol

    def write(self, data):
        """
        bypassing async startWriting/doWrite procedure
        
        this is where some intelligence passes application header data
        separate from undifferentiated application payload
        """
        self.pocket.send(data)

    def writeSequence(self, data):
        """
        Not Implemented (yet)
        """

    def doRead(self):
        """this is supposed to get data from mschan and pass to
        protocol.dataReceived
        """
        data = self.pocket.recv()
        self.protocol.dataReceived(data)


    def startReading(self):
        """
        this has to do with making sure the deliver queue is read
        should it be defined here or in AbstractMessageChannel?
        """

    def connectionLost(self, reason):
        """
        """

    logstr = "Uninitialized"

    def logPrefix(self):
        return self.logstr


class BaseClient(Connection):
    """Base client for MS connections

    does the managment work for mschan
    """

    def _finishInit(self, whenDone, pkt, error, reactor, dynamo):
        """what's this dance?

        why is the protocol arg None?
        Because it get's set during the doConnect call
        """
        if whenDone:
            Connection.__init__(self, pkt, None, reactor)
            reactor.callLater(0, whenDone)
        else:
            # reactor.callLater(0, self.failIfNotConnected, error)
            pass

    def createMessagingPocket(self):
        """
        make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        pkt = self.dynamo.pocket()
        return pkt

    def resolveAddress(self):
        """
        set self.realAddress
        this is passed as addr to the protocol buildProtocol(addr)
        """
        print 'resolveAddress'
        self._setRealAddress(self.addr) 

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
        yield self.pkt.connect(self.realAddress)
        self._connectionDone()

    def _connectionDone(self)
        self.protocol = self.connector.buildProtocol(None)
        self.logstr = self.protocol.__class__.__name__ + ', client'
        self.startReading()
        self.protocol.makeConnection(self)

    def connectionLost(self, reason):
        """
        """

    def failIfNotConnected(self, err):
        print 'failIfNotConnected', err

class Client(BaseClient):
    """
    carries out the creation of pockets


    implements address getting of ITransport
    """

    def __init__(self, addr, bindAddress, connector, reactor=None, dynamo=None):
        """
        XXX reactor = dynamo ?

        """
        self.addr = addr
        self.bindAddress = bindAddress
        self.connector = connector
        # try:
        pkt = self.createMessagingPocket()

        whenDone = self.resolveAddress # this thing figures out the real
                                        # amqp address to talk to

        if bindAddress:
            pkt.bind(bindAddress)

        err = None
        # What's this dance?
        # why is whenDone passed?
        self._finishInit(whenDone, pkt, err, reactor, dynamo)

    def __repr__(self):
        s = "<%s to %s at %x>" % (self.__class__, self.addr,
                                             unsignedID(self))
        return s


class Server(Connection):
    """
    This is like a socket which came from an accept()

    
    """

    def __init__(self, pkt, protocol, client_address, server, sessionno,
                                                reactor, dynamo):
        Connection.__init__(self, pkt, protocol, reactor, dynamo)
        self.server = server
        self.client_address = client_address # ?
        self.sessionno = sessionno # ?
        self.address = client_address# [0] # ?

        self.startReading() # a Connection responsability
        self.connected = 1

class BaseListeningPort(AbstractPocket):
    """
    """

    def createMessagingPocket(self):
        """
        make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        pkt = self.dynamo.pocket()
        return pkt



class ListeningPort(BaseListeningPort):
    """
    The listener of the server pattern.
    Facilitates bi-directional connections

    """

    implements(interfaces.IListeningPort)

    connected = 0

    transport = Server
    sessionno = 0 # is this needed?
    interface = ''

    hack_started = False

    def __init__(self, listen_address, factory, backlog=50, interface='',
                                        reactor=None, dynamo=None):
        BaseListeningPort.__init__(self, reactor, dynamo)
        self.listen_address = listen_address
        self.factory = factory
        self.backlog = backlog
        self.interface = interface


    @inlineCallbacks
    def startListening(self):
        """
        configuration of mschan
        """
        pkt = self.createMessagingPocket()
        pkt.bind(self.listen_address)

        # self._realPortNumber ?
        self.factory.doStart()
        pkt.listen()
        self.connected = True
        self.pocket = pkt
        self.startReading()

    def doRead(self):
        """
        XXX skipping error checks, max accepts, etc.
        """
        pkt, addr = self.pocket.accept()
        protocol = self.factory.buildProtocol(addr)
        s = self.sessionno
        self.sessionno = s + 1
        # should self.mschan really go in here?
        transport = self.transport(pkt, protocol, addr, self, s,
                                    self.reactor, self.dynamo)
        # transport = self._preMakeConnection(transport)
        protocol.makeConnection(transport)

    def stopListening(self):
        """
        """

    def getHost(self):
        """
        Get the host that this port is listening for.

        Returns an IAddress provider.
        """

class Connector(base.BaseConnector):
    """
    The baseConnector knows what to do with the Factory
    The Client knows how to get and use the real underlying transport.
    """

    def __init__(self, addr, factory, timeout, bindAddress, reactor, dynamo):
        self.addr = addr
        self.bindAddress = bindAddress
        self.dynamo = dynamo
        base.BaseConnector.__init__(self, factory, timeout, reactor)

    def _makeTransport(self):
        return Client(self.addr, self.bindAddress, self, reactor, self.dynamo)

    def getDestination(self):
        """
        The address given to the conenctor is a messaging service address
        (nothing to do with amqp routing_keys, queue names, or exchange
        names)

        What is it? What parts does it need?

        The prototype impementation is a trivial name.
        """
        return self.addr







