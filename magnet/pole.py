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
from twisted.python import log
from twisted.internet import defer
from twisted.internet import task
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
    exchange='agents'

    def send_when_running(self, routing_key, command, payload):
        self.send_routing_key = routing_key
        self.send_command = command
        self.send_payload = payload

    def do_when_running(self):
        role_message = {'method':'say',
                        'payload':self.send_payload}
        message_object = {'method':self.send_command, 
                        'role':'Control',
                        'payload':role_message}
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
    """
    Interaction type of a resource agent; contains set of actions.
    Ex: Controlling an OS application process.

    Roles have a finite state machine 
    """
    agent = None
    name = None
    send_when_running = []

    def __init__(self):

        for m in self.__dict__.keys():
            if m.startswith('action_'):
                self.actions.append(m)

    def __iter__(self):
        """iterate over all roles defined in the class"""
        return iter(self.actions)

    def setAgent(self, agent):
        """When a role is added to an agent, the agent sets this reference
        to itself.
        """
        self.agent = agent

    def startService(self):
        """
        """
        log.msg('Role start')
        service.Service.startService(self)
        for s in self.send_when_running:
            log.msg('%s StartService Sending %s' % (self.name, s))
            m = getattr(self, s)
            m()

    def consume_message(self, message_object):
        """
        """
        try:
            method = getattr(self, 'action_%s' % message_object['method'])
        except AttributeError:
            "Received unknown Action"
            return None
        res = defer.maybeDeferred(method, message_object)
        return res

    def sendMessage(self, resource_name, message_payload):
        """Send a message from a role to another
        """
        # message_object = {'role':self.name,
        #                 'payload':message_payload}
        self.agent.sendMessage(resource_name, self.name, message_payload)


class ResourceControlRole(Role):
    """Role for controlling a reource.
    """
    name = 'Control'

class ResourceMonitorRole(Role):
    """Role for monitoring a resource.
    """
    name = 'Monitor'

class ResourceCapability(Role):
    """Role for utilizing a resource capability.
    """
    name = 'Capability' # hmm..this is silly?

class AgentControl(Role):
    """Role for controling an agent.
    """
    name = 'Agent Control'
    send_when_running = ['register']

    def register(self):
        """Say hello to a controller
        """
        role_message = {'method':'register',
                        'agent_id':self.agent.unique_id,
                        'agent_name':self.agent.name}
        self.sendMessage('Controller', role_message)

    def action_register_ok(self, message_object):
        heartbeat = message_object['payload']
        self.agent.heartbeat.beat(heartbeat)

class HeartBeat(object):
    """
    Simple heartbeat.
    Thin wrapper around task.LoopingCall
    Needs a send function to send the heartbeat.
    """

    def __init__(self, send_f, agent):
        """
        send_f: the send function to call
        exchange: exchange to send to
        routing_key: route to address
        """
        role_message = {'method':'heartbeat',
                        'agent_id':agent.unique_id}
        a = ('Controller', 'Agent Control', role_message,)
        self.heart = task.LoopingCall(send_f, *a)
        log.msg(self.heart.a)


    def beat(self, rate):
        """set heart rate and start heartbeat
        rate: seconds
        """
        d = self.heart.start(rate)

    def stop(self):
        self.heart.stop()

class Agent(service.Service):
    """Represents a resource.
    Is a container holding different 'behaviors'/roles.
    Each behavior holds a set of actions.
    Each behavior is a state machine that consumes events generated by the
    agent receiving a message. 
    The behavior consumes these events.
    The agent is also a state machine.

    The agent has a name that is representative through out a whole system
     (name of a queue/routing key in amqp)
    """
    active = False
    incoming_queue = None
    agent_chan = None
    name = None

    def __init__(self, exchange, resource_name, unique_id=None):
        """
        Resource name is the canonical name for this agent with in a system.
        unique_name is for addressing specific instances of an agent type
        unique_name should be unique to the exchange
        """
        self.exchange = exchange
        self.resource_name = resource_name
        self.name = resource_name
        if unique_id is None:
            unique_id = uuid.uuid4().hex
        self.unique_id = unique_id
        log.msg('Agent %s unique_id: %s' % (self.name, self.unique_id))

        self.roles = []
        self.role_collection = service.MultiService()
        # self.role_collection.setServiceParent(self)
        self.known_agents = {} # Other agents this agent can talk to
        self.heartbeat = HeartBeat(self.sendMessage, self)

    def __iter__(self):
        """iterate over all roles defined in the class"""
        return iter(self.roles)

    def activateAgent(self, agent_chan):
        log.msg('activeate Agent')
        self.agent_chan = agent_chan
        self.active = True
        self.agent_chan.listen_to_queue(self.consume_message)
        self.startService()

    def startService(self):
        service.Service.startService(self)
        self.role_collection.startService()

    def stopService(self):
        service.Service.stopService(self)
        self.role_collection.stopService()

    def addAgentContact(self, name, address):
        """Add an agent this agent can talk to.
        name: name of agent (resource name...)
        address: tuple (exchange, routing_key)
        """
        self.known_agents[name] = address

    def addRole(self, role):
        """
        roles are how agents handle messages.
        Each role is a set of message handlers that do actions.
        The agent consumes messages sent to it's queue.
        Two queue types:
         - named queue, consumed by all agents with this role (load balance)
         - unique queue, consumed only by this role
        """
        self.role_collection.addService(role)
        role.setAgent(self)


    def consume_message(self, agent_message):
        """Agent message is for the agent layer.
        Agent message header specifies what role/interaction type the
        message is intended for.
        """
        log.msg('Agent consumer handler')
        role_name = agent_message['role']
        try:
            role = self.role_collection.getServiceNamed(role_name)
            role.consume_message(agent_message['payload'])
        except AttributeError:
            log.err("Received unknown Interaction")
            return None
        self.agent_chan.listen_to_queue(self.consume_message)


    def sendMessage(self, to_name, to_role, message_object):
        """send agent message
        The Agent, or a Role within the agent, addresses the message to
        another agent.
        How should the exchange and routing_key be determined; Agents know
        about and talk to other agents...
        """
        exchange, routing_key = self.known_agents.get(to_name)
        agent_message = {'role':to_role,
                        'payload':message_object}
        self.agent_chan.send_message(exchange, routing_key, 
                                                    agent_message)


class SimpleAgent(service.Service):
    """Represents a resource.
    Is a container holding different 'behaviors'/roles.
    Each behavior holds a set of actions.
    Each behavior is a state machine that consumes events generated by the
    agent receiving a message. 
    The behavior consumes these events.
    The agent is also a state machine.

    The agent has a name that is representative through out a whole system
     (name of a queue/routing key in amqp)
    """

    def __init__(self, exchange, resource_name, token=None):
        """
        Resource name is the canonical name for this agent with in a system.
        token is for addressing specific instances of an agent type
        """
        self.exchange = exchange
        self.resource_name = resource_name

        self.actions = []
        for m in self.__dict__.keys():
            if m.startswith('action_'):
                self.actions.append(m)

    def __iter__(self):
        """iterate over all roles defined in the class"""
        return iter(self.actions)

    def startService(self):
        service.Service.startService(self)
        self.connection

    def stopService(self):
        service.Service.stopService(self)


    def consume_message(self, agent_message):
        """Agent message is for the agent layer.
        Agent message header specifies what role/interaction type the
        message is intended for.
        """
        try:
            method = getattr(self, 'action_%s' % agent_message['interaction'])
        except AttributeError:
            "Received unknown Interaction"
            return None



    def sendMessage(self, message_object, key):
        self.parent.sendMessage(message_object, key)












