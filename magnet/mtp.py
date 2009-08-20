"""
Messaging Service Transport and Connection Patterns

This module is modeled on twisted.internet.tcp

This module provides implementations of the interfaces:
    ITransport
    IConnector
    IListeningPort

ITransport is the central interface. The mtp Connection implements the
generic ITransport interface, and wraps around a Pocket object instance.

Connection is the general transport object. 

Connections can be created in different ways, for different usages.
Server/ListenPort and Connection/Connector are two examples. 
@note 
The well established client and server patterns of the internet/tcp socket
were implemented here as a proof of concept and as a nucleus to build the
new abstraction of the 'messaging service' transport.

IConnectod and IListeningPort facilitate creating new Connections. They
represent a certain pattern of usage of a Connection (they will not be the
only patterns).

@file mtp.py
@author Dorian Raymer
@date 7/9/09
"""

import sys

from zope.interface import implements

from twisted.internet import base
from twisted.internet import abstract
from twisted.internet import reactor
from twisted.internet import defer 
from twisted.internet import error 
from twisted.internet import interfaces 
from twisted.python import log
from twisted.python import failure
from twisted.python import reflect
from twisted.python.util import unsignedID
from twisted.persisted import styles



class AbstractDescriptor(abstract.FileDescriptor):
    """
    An abstract superclass of all objects which may be notified when they
    are readable or writable. This is modeled after/subclassed from
    t.i.abstract.FileDescriptor, but we are not dealing with
    file descriptors -- this class is the abstract representation of the
    core new concept provided by the 'messaging service' or messaging
    based Inter Process Communication. The name AbstractDescriptor is
    tentative (a better name will emerge with refinement of the messaging
    service concepts/understanding)

    @todo
    Rename this. 
    Background:
    The tcp connection inherits the FileDescriptor class. FileDescriptor is
    an abstract super class of objects that can be notified when they are
    readable or writable -- an object which can be operated on by select().
    """



    def __init__(self, reactor, dynamo):
        abstract.FileDescriptor.__init__(self, reactor)
        self.dynamo = dynamo

    def startReading(self):
        self.dynamo.addReader(self)

    def startWriting(self):
        self.dynamo.addWriter(self)

    def stopReading(self):
        self.dynamo.removeReader(self)

    def stopWriting(self):
        self.dynamo.removeWriter(self)


class Connection(AbstractDescriptor):
    """Messaging Service Connection.
    Connections wrap around the "physical" lower level part of the network.

    This glues the application protocol to the underlying transport. 

    @todo When/if the time comes, an interface to the 'messaging service'
    (i.e. IMSTransport, or something to the effect) extending ITransport 
    could be defined, and this class would be an implementation.
    """
    
    # this is where we get to create IMSTransport, if needed
    # implements(interfaces.ITransport)

    def __init__(self, pkt, protocol, reactor, dynamo):
        """
        """
        AbstractDescriptor.__init__(self, reactor, dynamo)
        self.pocket = pkt
        self.protocol = protocol

    def doRead(self):
        """
        poll notifies that data is ready to be read
        protocol.dataReceived
        """
        data = self.pocket.recv()
        self.protocol.dataReceived(data)

    def writeSomeData(self, data):
        """
        This write actually writes on to the pocket.
        @todo Currently returns len(data), assumes all data is sent. Verify
        """
        self.pocket.send(data)
        return len(data)

    def _closeWriteConnection(self):
        """
        @todo
        Does this need to be implemented?
        """

    def readConnectionLost(self, reason):
        """
        @todo
        Does this need to be implemented?
        """

    def connectionLost(self, reason):
        """
        """
        AbstractDescriptor.connectionLost(self, reason)
        protocol = self.protocol
        del self.protocol
        del self.pocket
        protocol.connectionLost(reason)

    logstr = "Uninitialized"

    def logPrefix(self):
        return self.logstr

    def ack(self):
        """
        """
        pass


class BaseClient(Connection):
    """Base client for MS connections

    does the management work for mschan
    """

    def _finishInit(self, whenDone, pkt, error, reactor, dynamo):
        """what's this dance?

        why is the protocol arg None?
        Because it get's set during the doConnect call
        """
        if whenDone:
            Connection.__init__(self, pkt, None, reactor, dynamo)
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

    @defer.inlineCallbacks
    def resolveAddress(self):
        """
        set self.realAddress
        this is passed as addr to the protocol buildProtocol(addr)
        @note Not sure if there is any need to resolve here. Usually host
        names are resolved to IP addresses, but there may not be an
        analogous requirement here.

        @note It could be necessary to further qualify the messaging
        service address using configuration/context known at runtime.
        """
        yield self.pocket.bind(self.bindAddress)
        self._setRealAddress(self.addr) 
        yield self.doConnect()

    def _setRealAddress(self, address):
        """
        @note There should be no awareness of amqp addresses in this code.
        """
        # self.realAddress = ['amq.direct',address]
        self.realAddress = address
        # yield self.doConnect()

    @defer.inlineCallbacks
    def doConnect(self):
        """
        this is where the application protocol is instantiated

        """
        status = yield self.pocket.connect(self.realAddress)
        self._connectionDone()
        defer.returnValue(None)

    def _connectionDone(self):
        self.protocol = self.connector.buildProtocol(None)
        self.connected = 1
        self.logstr = self.protocol.__class__.__name__ + ', client'
        self.startReading()
        self.protocol.makeConnection(self)

    def connectionLost(self, reason):
        """
        """
        if not self.connected:
            self.failIfNotConnected(error.ConnectError(string=reason))
        else:
            Connection.connectionLost(self, reason)
            self.connector.connectionLost(reason)

    def failIfNotConnected(self, err):
        """
        Copied from tcp. 
        @todo
        make sure this makes sense
        """
        if (self.connected or self.disconnected or not hasattr(self,
            "connector")):
            return
        self.connector.connectionFailed(failure.Failure(err))
        if hasattr(self, "reactor"):
            self.stopReading()
            self.stopWriting()
            del self.connector



class Client(BaseClient):
    """
    carries out the creation of pockets


    implements address getting of ITransport
    """

    def __init__(self, addr, bindAddress, connector, reactor, dynamo):
        """

        """
        self.addr = addr
        self.bindAddress = bindAddress
        self.connector = connector
        self.dynamo = dynamo
        # try:
        pkt = self.createMessagingPocket()

        whenDone = self.resolveAddress # this thing figures out the real
                                        # amqp address to talk to
        # Connections must always call bind. Pocket will handle None address
        # pkt.bind(bindAddress)

        # Need to create Pocket errors/exceptions
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
        self.dynamo = dynamo
        self.logstr = "%s, %s, %s" % (self.protocol.__class__.__name__,
                                        self.sessionno,
                                        str(self.address))
        self.repstr = "<%s # %s on %s>" % (self.protocol.__class__.__name__,
                                        self.sessionno,
                                        str(self.server.listen_address))

        self.startReading() # a Connection responsibility
        self.connected = 1

    def getHost(self):
        """
        @todo
        evolve this to work with pocket address
        """

    def getPeer(self):
        """
        @todo
        evolve this to work with pocket address
        """

class BaseListeningPort(AbstractDescriptor):
    """
    Connection for creating connections.
    The listening port pattern is a server pattern.
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
    sessionno = 0 
    interface = ''

    def __init__(self, listen_address, factory, backlog=50, interface='',
                                        reactor=None, dynamo=None):
        BaseListeningPort.__init__(self, reactor, dynamo)
        self.listen_address = listen_address
        self.factory = factory
        self.backlog = backlog
        self.interface = interface
        self.dynamo = dynamo

    def __repr__(self):
        """
        @todo
        finish address resolve...
        """
        return "<%s of %s on %s>" % (self.__class__,
                self.factory.__class__, str(self.listen_address))

    @defer.inlineCallbacks
    def startListening(self):
        """
        configuration of mschan
        """
        pkt = self.createMessagingPocket()
        yield pkt.bind(self.listen_address)

        log.msg("%s starting on %s" % (self.factory.__class__, str(self.listen_address)))
        # self._realPortNumber ?
        self.factory.doStart()
        pkt.listen()
        self.connected = True
        self.pocket = pkt
        self.startReading()

    # @defer.inlineCallbacks
    def doRead(self):
        """
        @todo skipping error checks, max accepts, etc.
        """
        # try #  implement handeling errors for bad connection requests
        # pkt, addr = yield self.pocket.accept()
        pkt, addr = self.pocket.accept()
        protocol = self.factory.buildProtocol(addr)
        s = self.sessionno
        self.sessionno = s + 1
        transport = self.transport(pkt, protocol, addr, self, s,
                                    self.reactor, self.dynamo)
        # transport = self._preMakeConnection(transport)
        protocol.makeConnection(transport)

    def loseConnection(self,
            connDone=failure.Failure(error.ConnectionDone('Connection Done'))):
        """
        """
        self.disconnecting = True
        self.stopReading()
        if self.connected:
            self.deferred = defer.Deferred()
            self.reactor.callLater(0, self.connectionLost, connDone)
            return self.deferred

    stopListening = loseConnection

    def connectionLost(self, reason):
        log.msg('(Messaging Service Listener %s Closed)' % str(self.listen_address))
        BaseListeningPort.connectionLost(self, reason)

        d = None
        if hasattr(self, "deferred"):
            d = self.deferred
            del self.deferred

        try:
            self.factory.doStop()
        except:
            self.disconnecting = False
            if d is not None:
                d.errback(failure.Failure())
            else:
                raise
        else:
            self.disconnecting = False
            if d is not None:
                d.callback(None)

    def logPrefix(self):
        return reflect.qual(self.factory.__class__)

    def getHost(self):
        """
        @todo
        """

class Connector(base.BaseConnector):
    """
    Pattern for establishing bi-directional 'messaging service' connections
    between two peers'
    
    @note
    The baseConnector knows what to do with the Factory
    The Client knows how to get and use the real underlying transport.
    """

    def __init__(self, addr, factory, timeout, bindAddress, reactor, dynamo):
        self.addr = addr
        self.bindAddress = bindAddress
        self.dynamo = dynamo
        base.BaseConnector.__init__(self, factory, timeout, reactor)

    def _makeTransport(self):
        return Client(self.addr, self.bindAddress, self, self.reactor, self.dynamo)

    def getDestination(self):
        """
        The address given to the connector is a messaging service address
        (nothing to do with amqp routing_keys, queue names, or exchange
        names)

        @todo What is it? What parts does it need?

        The prototype implementation is a trivial name.
        """
        return self.addr

class WorkConsumerClient(Client):

    def createMessagingPocket(self):
        """
        make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        pkt = self.dynamo.work_consumer_pocket()
        return pkt

    def ack(self):
        """
        Prototype for acknowledging work message was successfully processed.
        """
        self.pocket.ack()

class WorkConsumerConnector(Connector):
    def _makeTransport(self):
        return WorkConsumerClient(self.addr, self.bindAddress, self, self.reactor, self.dynamo)

class WorkProducerClient(Client):

    def createMessagingPocket(self):
        """
        make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        pkt = self.dynamo.work_producer_pocket()
        return pkt

class WorkProducerConnector(Connector):
    def _makeTransport(self):
        return WorkProducerClient(self.addr, self.bindAddress, self, self.reactor, self.dynamo)


class SimpleConsumerClient(Client):

    def createMessagingPocket(self):
        """
        make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        pkt = self.dynamo.simple_consumer_pocket()
        return pkt

    def ack(self):
        """
        Prototype for acknowledging work message was successfully processed.
        """
        self.pocket.ack()

class SimpleConsumerConnector(Connector):
    def _makeTransport(self):
        return SimpleConsumerClient(self.addr, self.bindAddress, self, self.reactor, self.dynamo)

class SimpleProducerClient(Client):

    def createMessagingPocket(self):
        """
        make an instance of AbstractMessageChannel, passing it a active
        msgsrv object
        """
        pkt = self.dynamo.simple_producer_pocket()
        return pkt

class SimpleProducerConnector(Connector):
    def _makeTransport(self):
        return SimpleProducerClient(self.addr, self.bindAddress, self, self.reactor, self.dynamo)

