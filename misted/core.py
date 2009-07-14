"""
Provides the pocket reactor.

@file core.py
@author Dorian Raymer
@date 7/9/09
"""
from twisted.internet import defer
from twisted.internet import task
from twisted.python import log

from misted import mtp
from misted import pocket



class PocketReactorCore(object):
    """
    The pocket reactor supporting messaged based Inter Process
    Communication.

    The network events generated by the Twisted select reactor (or any
    Twisted reactor implementing the IReactorFDSet) originate from the
    operating system via select, or another file descriptor poll.

    The pocket connections to the messaging IPC facility are not file
    descriptors and therefore cannot be added as readers or writers
    in the regular select (or equivalent) reactor event loop -- pockets
    operate in an abstraction realm above that of the socket.

    This must always be running. Pockets depend on channels from the AMQP
    client.
    """

    def __init__(self, reactor, client):
        """the client attribute of the messaging service core is the amqp
        client instance, (connected and running)
        """
        self.reactor = reactor
        self.client = client


    def pocket(self):
        """pocket instance factory

        For prototype, always return new pocket
        """
        chan = self.client.channel()
        p = pocket._PocketObject(chan)
        p.dynamo = self
        return p

    def listenMS(self, addr, factory, backlog=50):
        """Connects given factory to the given message service address.
        """
        p = mtp.ListeningPort(addr, factory, reactor=self.reactor, dynamo=self)
        p.startListening()
        return p

    def connectMS(self, addr, factory, timeout=30, bindAddress=None):
        """Connect a message service client to given message service
        address.
        """
        if bindAddress == None:
            bindAddress = ['amq.direct', '']
        c = mtp.Connector(addr, factory, timeout, bindAddress, self.reactor, self)
        c.connect()
        return c

    def run(self):
        """
        @todo improve this loop mechanism
        """
        self.loop.start(0.001)

def _pocket_poll(readers, writers):
    """Poll over read and write pocket objects checking for read and
    writeablity. This is the equivalent to select polling over file
    descriptors.

    The getting of the pocket reference explicitly from the abstract pocket
    might best be wrapped by a getter (maybe that's what filehandle is for)
    """
    readables = [r for r in readers if r.pocket.read_ready()]
    writeables = [w for w in writers if w.pocket.write_ready()]
    return readables, writeables

class PocketReactor(PocketReactorCore):
    """Analog of select reactor
    Event driver for pockets

    @todo This will be a service, or service collection
    @todo  or a cooperator service
    """

    def __init__(self, reactor, client):
        """
        readers and writers are pocket obects
        """
        PocketReactorCore.__init__(self, reactor, client)
        self._readers = {}
        self._writers = {}
        self.loop = task.LoopingCall(self.doIteration)

    def doIteration(self):
        """Run one iteration of checking the channel buffers

        (This is the analog to what select does with file descriptors)
        """
        r, w = _pocket_poll(self._readers.keys(), self._writers.keys())

        _drdw = self._doReadOrWrite
        _logrun = log.callWithLogger

        for pkts, method in ((r, 'doRead'), (w, 'doWrite')):
            for pkt in pkts:
                # @todo not sure why dict is passed in and out of logger?
                _logrun(pkt, _drdw, pkt, method, dict)

    def _doReadOrWrite(self, pkt, method, dict):
        """
        """
        try:
            # This just calls doRead or doWrite
            # leaving out other error checking done in select reactor
            why = getattr(pkt, method)()
        except:
            log.err()

    def addReader(self, reader):
        """
        """
        self._readers[reader] = 1

    def addWriter(self, writer):
        """
        """
        self._writers[writer] = 1

    def removeReader(self, reader):
        """
        """
        if reader in self._readers:
            del self._readers[reader]

    def removeWriter(self, writer):
        """
        """
        if writer in self._writers:
            del self._writers[writer]

    def removeAll(self):
        """
        """

    def getReaders(self):
        """
        """
        return self._readers.keys()

    def getWriters(self):
        """
        """
        return self._writers.keys()
