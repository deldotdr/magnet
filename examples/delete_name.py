
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks


from magnet.amqp import AMQPClientCreator
from magnet.core import PocketReactor


BROKER_HOST = 'amoeba.ucsd.edu'
BROKER_HOST = 'localhost'
BROKER_PORT = 5672


@inlineCallbacks
def main(reactor):
    clientCreator = AMQPClientCreator(reactor)
    client = yield clientCreator.connectTCP(BROKER_HOST, BROKER_PORT)

    p_reactor = PocketReactor(reactor, client)

    pkt = p_reactor.pocket()
    pkt.delete_name('factor')

    p_reactor.run()



if __name__ == '__main__':
    main(reactor)
    reactor.run()
