"""
@filename names.py
@author Dorian Raymer
@date 7/27/09

"""

from zope.interdface import implements

from twisted.internet import interfaces


"""Differentiate between config for consumers and config for producers.
The different connection control stuff can be accomplished with this table,
or by creating a few distinct 'connector' entry points (classes in mtp.py)
"""
_bidirectional_conversation = {'exchange_type':'direct',
                            'publish_immediate':True,
                            'publish_mandatory':True
                            }

_work_consumer = {'exchange_type':'topic',
                }

_work_producer = {'exchange_type':'topic',
                'publish_immediate':False,
                'publish_mandatory':True
                }

_exchange_point = {'exchange_type':'topic',
                }

"""Use of test Directory:
    The local application is "connected" to a remote application in the
    messaging service by the remote application name.
    An exchange point application is an application that distributes
    messages. Messages are sent in with a topic. Applications can connect
    to distributors, find out what topics are registered to flow into the
    distributor, and subscribe to have some subset of those topics
    delivered to them.

"""

def application_name(name, exchange, routing_key=None, queue=None, opts={}):
    d = {
            'exchange':exchange,
            'routing_key':routing_key,
            'queue':queue
         }
    d.update(opts)
    return (name, d)

TEST_DIRECTORY = dict((
        application_name('cpe_exchange_point',
                            exchange='amq.direct', 
                            routing_key='cpe_exchange_point',
                            queue='cpe_exchange_point',
                            opts=_exchange_point
                            ),
        
        ))

class PocketResolver(object):
    """
    """

    def __init__(self, directory):
        self.directory = directory

    def resolveApplicationName(self, name):
        """
        return an AMQP exchange name and routing key
        """
        if not self.directory.has_key(name):
            raise KeyError("Application %s not found" % name)
        return self.directory[name]

    def getFullAMQPAddressFromName(self, name):
        """
        """


class ExchangePointName(object):
    """
    Prototype and test of Exchange point name.
    Might not need this to be a class.
    """

    implements(interfaces.IAddress)

    def __init__(self, exchange_point, topic):
        """
        @param exchange_point name of application 
        @param topic thing that represents a routing pattern for moving a
        message within the domain of an exchange point
        """
        self.exchange_point = exchange_point
        self.topic = topic


class ApplicationName(object):
    """
    Record of name/messaging service address to AMQP exchange, routing key,
    queue name, and associated configuration of those items.

    @note
    Config items:
    - publish:
      - immediate (detect lost peer)
      - mandatory (ensure delivered to any available queue)
    - bind
    - consume
    - qos

    @note Patterns
    - Bidirectional Connection (peer to peer conversation)
    """


class NameToAMQPAddress:
    """Simple lookup table for resolving names to amqp routing_key,
    exchange_name, queue_name tuple.

    Setting an entry can be thought of as configuring a service/application
    process. 
    Setting an entry establishes what communication pattern is utilized by
    the service/application process.

    @note The look up table is implicitly scoped to one virtual host.
    """

    def resolve(self, name):
        """return tuple of exchange_name, routing_key, queue_name
        """
