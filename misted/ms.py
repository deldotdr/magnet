"""
NOT USED
This was an initial test case stand alone file.

"""
import sys

from zope.interface import implements

from twisted.internet.interfaces import ITransport
from twisted.internet import base
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

import common


@inlineCallbacks
def getQueue(conn, chan):
    # in order to get the queue, we've got some setup to do; keep in mind that
    # we're not interested in persisting messages
    #
    # create an exchange on the message server
    yield chan.exchange_declare(
        exchange=common.EXCHANGE_NAME, type="direct",
        durable=False, auto_delete=True)
    # create a message queue on the message server
    yield chan.queue_declare(
        queue=common.QUEUE_NAME, durable=False, exclusive=False,
        auto_delete=True)
    # bind the exchange and the message queue
    yield chan.queue_bind(
        queue=common.QUEUE_NAME, exchange=common.EXCHANGE_NAME,
        routing_key=common.ROUTING_KEY)
    # we're writing a consumer, so we need to create a consumer, identifying
    # which queue this consumer is reading from; we give it a tag so that we
    # can refer to it later
    yield chan.basic_consume(
        queue=common.QUEUE_NAME,
        consumer_tag=common.CONSUMER_TAG)
    # get the queue that's associated with our consumer
    queue = yield conn.queue(common.CONSUMER_TAG)
    returnValue(queue)


@inlineCallbacks
def processMessage(chan, queue):
    msg = yield queue.get()
    print "Received: %s from channel #%s" % (
        msg.content.body, chan.id)
    processMessage(chan, queue)
    returnValue(None)


@inlineCallbacks
def main_consume(spec):
    delegate = TwistedDelegate()
    # create the Twisted consumer client
    consumer = ClientCreator(
        reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    # connect to the RabbitMQ server
    conn = yield common.getConnection(consumer)
    # get the channel
    chan = yield common.getChannel(conn)
    # get the message queue
    queue = yield getQueue(conn, chan)
    while True:
        yield processMessage(chan, queue)

@inlineCallbacks
def pushText(chan, body):
    msg = Content(body)
    # we don't want to see these test messages every time the consumer connects
    # to the RabbitMQ server, so we opt for non-persistent delivery
    msg["delivery mode"] = common.NON_PERSISTENT
    # publiish the message to our exchange; use the routing key to decide which
    # queue the exchange should send it to
    yield chan.basic_publish(
        exchange=common.EXCHANGE_NAME, content=msg,
        routing_key=common.ROUTING_KEY)
    returnValue(None)


@inlineCallbacks
def cleanUp(conn, chan):
    yield chan.channel_close()
    # the AMQP spec says that connection/channel closes should be done
    # carefully; the txamqp.protocol.AMQPClient creates an initial channel with
    # id 0 when it first starts; we get this channel so that we can close it
    chan = yield conn.channel(0)
    # close the virtual connection (channel)
    yield chan.connection_close()
    reactor.stop()
    returnValue(None)


@inlineCallbacks
def main_produce(spec):
    delegate = TwistedDelegate()
    # create the Twisted producer client
    producer = ClientCreator(
        reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    # connect to the RabbitMQ server
    conn = yield common.getConnection(producer)
    # get the channel
    chan = yield common.getChannel(conn)
    # send the text to the RabbitMQ server
    yield pushText(chan, sys.argv[2])
    # shut everything down
    yield cleanUp(conn, chan)


@inlineCallbacks
def amqpConnnectionFactory(spec):
    delegate = TwistedDelegate()
    # create the Twisted consumer client
    consumer = ClientCreator(
        reactor, AMQClient, delegate=delegate,
        vhost=common.VHOST, spec=spec)
    # connect to the RabbitMQ server
    conn = yield common.getConnection(consumer)
    returnValue(conn)



class MistedConnection:
    """Messaging Service Connection.
    Connections wrap around the "physical" lower level part of the network.

    Use an AMQP connection to start create a channel.
    """
    
    implements(ITransport)

    def __init__(self, chan, protocol, reactor=None):
        """
        """

    def write(self, data):
        """
        """
        pushText(self.chan, data)

    def writeSequence(self, data):
        """
        """

    @inlineCallbacks
    def doReadFromQueue(self):
        """
        """
        data = yield self.checkQueue()
        print 'data', data.content.body
        self.protocol.dataReceived(data)
        self.doReadFromQueue()


    def loseConnection(Self):
        """
        """

    def getPeer(self):
        """
        """

    def getHost(self):
        """
        """

    def resolveAddress(self):
        """
        set self.realAddress
        this is passed as addr to the protocol buildProtocol(addr)
        might not need to be resolved here?
        """
        print 'resolveAddress'
        self.doConnect()

    def failIfNotConnected(self, err):
        print 'failIfNotConnected', err

class MistedClient(MistedConnection):

    def __init__(self, to_addr, from_addr, connector, reactor=None):
        self.connector = connector

    @inlineCallbacks
    def createAMQPChannel(self, amqp_conn):
        """
        """
        self.chan = yield common.getChannel(amqp_conn)

    @inlineCallbacks
    def doConnect(self):
        """
        """
        yield self.createAMQPChannel(amqp_conn)
        self.queue = yield getQueue(amqp_conn, self.chan)
        self.protocol = self.connector.buildProtocol(None)
        self.protocol.makeConnection(self)
        self.doReadFromQueue()

    @inlineCallbacks
    def checkQueue(self):
        data = yield self.queue.get()
        returnValue(data)


class MistedConnector(base.BaseConnector):

    def __init__(self, to_addr, factory, timeout, from_addr, reactor=reactor):
        self.to_addr = to_addr
        self.from_addr = from_addr
        base.BaseConnector.__init__(self, factory, timeout, reactor)

    def _makeTransport(self):
        c = MistedClient(self.to_addr, self.from_addr, self,
                self.reactor)
        c.doConnect()
        return c


def connectMS(to_addr, factory, timeout=30, from_addr=None):
    c = MistedConnector(to_addr, factory, timeout, from_addr)
    c.connect()
    return c

from twisted.web.client import HTTPClientFactory
def test_client(_amqp_conn):
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


