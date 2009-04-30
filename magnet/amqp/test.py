
import sys
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.protocol import ClientCreator
from twisted.python import log
log.startLogging(sys.stdout)

from connection import Connection


@defer.inlineCallbacks
def do(conn):
    yield conn.open(virtual_host='/')
    ch = conn.channel()
    yield ch.channel_open()
    yield ch.exchange_declare(exchange='dodod')



client = ClientCreator(reactor, Connection)
d = client.connectTCP(host='amoeba.ucsd.edu', port=5672)

d.addCallback(do)
reactor.run()
