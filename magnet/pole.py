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


from zope.interface import Interface, implements

from twisted.internet import defer
from twisted.application import service

from twisted.internet.utils import getProcessOutput



class IPoleService(Interface):
    """
    Perhaps IAgentService, or IActorService

    This interface defines how a twisted application service that does
    a named function invoked though a handleMessage method.
    """

    def handleMessage(msg):
        """
        """
        pass


class BasePole(service.Service):
    """This might be called BaseAgent or BaseActor
    """

    implements(IPoleService)

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


    def __iter__(self):
        """iterate over all actions defined in the class"""
        return iter(self.actions)

    def handleMessage(self, message_object):
        method = getattr(self, 'action_%s' % message_object['method'])
        res = method(message_object)
        return res

    def putAction(self, topic, action):
        """a way to add an action service consuming or producing on its own channel
        within an agent
        """
        pass

    def sendMessage(self, message_object, key):
        self.parent.sendMessage(message_object, key)

    def startService(self):
        service.Service.startService(self)

    def stopService(self):
        service.Service.stopService(self)


class MultiPole(BasePole):
    """In the simplest form, MultiPole can have top level actions just like
    BasePole. 
    Separately created Poles can be added (by name) to MultiPole. These
    child poles will also use a messageHandler to decide wich action_
    method should handle the message.
    """
    pass


class MagnitePole(BasePole):
    """This Pole should be compatible with a Nanite Agent.
    Nanite Agents poses Actions. 

    """




















