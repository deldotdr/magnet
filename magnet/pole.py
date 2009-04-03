"""
Magnetic poles are where field lines emanate to and from. 
Instead of working out the semantics of Actors, Agents, etc., the word pole
will be a place holder for the concept that messages (particles, like
electrons) will move over field lines (network protocol, like AMQP) in
circuits that terminate at poles.

There are many types of poles between which can move many types of
particles mediated by many types of fields.

There are many types of actors that can exchange many types of messages
over many network protocols.

Poles can be made of many types of materials, and even be the sum of
multiple smaller poles in an organized connection.

Particles hold many attributes; the value or type of each attribute is
independent of each other; their scopes are orthogonal. 

Messages can be serialized/organized in many formats, have many purposes,
use many key:value pairs, represent many purposes/roles, etc.
Any purpose one purpose can be separately represented in one of several
formats using many variations of key:value pairs, etc...

"""

import uuid

from zope.interface import Interface, implements

from twisted.plugin import IPlugin
from twisted.internet import defer
from twisted.application import service

from twisted.internet.utils import getProcessOutput


class IPoleService(Interface):
    """
     Could also be named IAgentService, or IActorService

    Interface between Application service, and message delivery protocol.
     
    """

    def handleMessage(msg):
        """
        """
        pass

class IMultiPoleService(Interface):
    """MultiPole means the exploiting the multiplexing of the connection.
    The MultiPole is a collection of Roles, each Role listens on it's own
    consumer
    """

    def handleMessage(msg):
        """
        """
        pass

    def getRoles():
        """
        """
        pass

    def addRole(role):
        """
        set service parent
        """
        pass


class BasePole(service.Service):
    """This might be called BaseAgent or BaseActor
    """

    implements(IPlugin, IPoleService)

    def __init__(self, exchange='magnet', routing_pattern='test', system_name='test', service_name='test', token=None):
        """
        exchange is the real name of the exchange that should be used for
        now.

        system_name: what system is this agent representing (system is like
        an ever present unit

        service_name: the name of this service (i.e. control, monitor, ...)

        token: if supplied, should be a unique id within the realm of the
        system

        routing/resource naming:
            system.service.selector[.method]
            /system/service/selector[/method]

            (selector could be a token, *, etc...)
        """

        self.exchange = exchange
        self.routing_pattern = routing_pattern
        self.system_name = system_name
        self.service_name = service_name
        if token is None:
            token = uuid.uuid4().hex
        self.token = token
        self.actions = []
        for m in self.__dict__.keys():
            if m.startswith('action_'):
                self.actions.append(m)


    def __iter__(self):
        """iterate over all actions defined in the class"""
        return iter(self.actions)

    def handleMessage(self, message_object):
        try:
            method = getattr(self, 'action_%s' % message_object['method'])
        except AttributeError:
            "Received unknown Action"
            return None
        res = defer.maybeDeferred(method, message_object)
        return res

    def addRole(self, role):
        """A Role is a class with more actions.
        """

    def sendMessage(self, message_object, key):
        self.parent.sendMessage(message_object, key)

    def startService(self):
        service.Service.startService(self)

    def stopService(self):
        service.Service.stopService(self)

    def do_when_running(self):
        pass

class SendOne(BasePole):

    def send_when_running(self, routing_key, command, payload):
        self.send_routing_key = routing_key
        self.send_command = command
        self.send_payload = payload

    def do_when_running(self):
        message_object = {'method':self.send_command, 'payload':self.send_payload}
        self.sendMessage(message_object, self.send_routing_key)


class MultiPole(BasePole):
    """In the simplest form, MultiPole can have top level actions just like
    BasePole. 
    Separately created Poles can be added (by name) to MultiPole. These
    child poles will also use a messageHandler to decide wich action_
    method should handle the message.
    """
    pass

class MonoPoleSingleRole:
    """One consumer, one set of actions
    The pole also holds a producer channel for sending messages.

    Messages produced can have any routing key, and will be published to
    the same exchange as the consumers (this might be an arbitrary
    constraint).
    """
    pass

class MonoPoleMultiRole:
    """One consumer, multiple sets of actions (multiple roles). Role
    specified in message, not in routing/binding key
    """
    pass

class MultiPoleMultiRole(service.MultiService):
    """Each Role has it's own consumer
    When a role is registered, the Pole knows to configure a consumer for
    it. 
    Multiple consumers means multiple channels within one connection.
    Consumers and channels should map 1:1; don't use a channel for more
    than one consumer.

    When benchmarking, the characteristics of connection multiplexing
    should be assessed.
    """

    implements(IPlugin, IMultiPoleService)


    def __init__(self, exchange, system_name, service_name, token=None):
        """
        exchange is the real name of the exchange that should be used for
        now.

        system_name: what system is this agent representing (system is like
        an ever present unit

        service_name: the name of this service (i.e. control, monitor, ...)

        token: if supplied, should be a unique id within the realm of the
        system

        routing/resource naming:
            system.service.selector[.method]
            /system/service/selector[/method]

            (selector could be a token, *, etc...)
        """
        service.MultiService.__init__(self)
        self.exchange = exchange
        self.system_name = system_name
        self.service_name = service_name
        if token is None:
            token = uuid.uuid4().hex
        self.token = token
        self.actions = []
        for m in self.__dict__.keys():
            if m.startswith('action_'):
                self.actions.append(m)


    def startService(self):
        service.MultiService.startService(self)





class Role(service.Service):
    """Container of actions particular one thing.
    
    Ex: Controlling an OS application process.

    """

    def __init__(self, name):
        self.name = name


    def startService(self):
        """
        Get a consumer
        """
        service.Service.startService(self)

    def handleMessage(self, msg):
        """
        """
















