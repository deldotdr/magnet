
# This could be cool if starting up the preactor (or the amqp client)
# didn't involve a deferred...
# import sys
# del sys.modules['magnet.preactor']
# from magnet.core import install
# install()

from twisted.internet import defer

@defer.inlineCallbacks
def Preactor():
    """
    Make a preactor instance.
    Get configuration from conf file.
    If no local conf file (dir magnet started in), fallback on .magnet.conf
    in users home dir.
    If no conf in users home, fallback on magnet.conf in magnet python
    package.
    """
    import os
    import sys
    import ConfigParser
    import magnet
    pkg_conf = os.path.join(magnet.__path__[0], 'magnet.conf')
    home_path = os.path.expanduser('~')
    home_conf = os.path.join(home_path, '.magnet.conf') # @todo hardcoded filename
    # see if there is a conf file in the cur dir
    local_conf = os.path.join(os.path.abspath('.'), 'magnet.conf')
    c = ConfigParser.ConfigParser()
    confs_read = c.read([pkg_conf, home_conf, local_conf])
    if not confs_read:
        raise Exception("""No magnet.conf file located!! This is where\
                necessary AMQP Broker configuration is looked up.""")

    from twisted.internet import reactor
    from magnet.amqp import AMQPClientCreator
    from magnet.core import PocketReactor
    
    username = c.get('amqp_broker', 'username')
    password = c.get('amqp_broker', 'password')
    vhost = c.get('amqp_broker', 'vhost')
    clientCreator = AMQPClientCreator(reactor, username=username,
                                    password=password, vhost=vhost)

    broker_host = c.get('amqp_broker', 'host')
    broker_port = int(c.get('amqp_broker', 'port'))
    amqp_client = yield clientCreator.connectTCP(broker_host, broker_port)

    preactor = PocketReactor(reactor, amqp_client)
    preactor.run()
    defer.returnValue(preactor)
 
