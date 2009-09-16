"""
Twisted Application(like) services of Magnet MS Connectors/Consumers/Producers, etc.

@file service.py
@author Dorian Raymer
@date 9/15/09
"""

from twisted.application import service
from twisted.python import log

class MSServer(service.Service):
    """
    Service for preactor.listenMS
    """

    _listener = None

    def __init__(self, name, factory, preactor):
        """
        @param name message service name to listen on
        @param factory protocol factory
        @param preactor The running Magnet preactor
        @note This is non-general
        """
        self.name = name
        self.factory = factory
        self.preactor = preactor

    def startService(self):
        service.Service.startService(self)
        self._listener = self.preactor.listenMS(self.name, self.factory)

    def stopService(self):
        service.Service.stopService(self)
        if self._listener is not None:
            d = self._listener.stopListening()
            del self._listener
            return d


class MSClient(service.Service):
    """
    Service for preactor.connectMS
    """

    _connection = None

    def __init__(self, name, factory, preactor):
        """
        @param name message service name to listen on
        @param factory protocol factory
        @param preactor The running Magnet preactor
        @note This is non-general
        """
        self.name = name
        self.factory = factory
        self.preactor = preactor

    def startService(self):
        service.Service.startService(self)
        self._connection = self.preactor.connectMS(self.name, self.factory)

    def stopService(self):
        service.Service.stopService(self)
        if self._connection is not None:
            self._connection.disconnect()
            del self._connection

class MSSimpleConsumer(service.Service):
    """
    Service for preactor.connectMS
    """

    _consumer = None

    def __init__(self, name, factory, preactor):
        """
        @param name message service name to listen on
        @param factory protocol factory
        @param preactor The running Magnet preactor
        @note This is non-general
        """
        self.name = name
        self.factory = factory
        self.preactor = preactor

    def startService(self):
        service.Service.startService(self)
        self._consumer = self.preactor.connectSimpleConsumer(self.name, self.factory)

    def stopService(self):
        service.Service.stopService(self)
        if self._consumer is not None:
            self._consumer.disconnect()
            del self._consumer

class MSSimpleProducer(service.Service):
    """
    Service for preactor.connectMS
    """

    _producer = None

    def __init__(self, name, factory, preactor):
        """
        @param name message service name to listen on
        @param factory protocol factory
        @param preactor The running Magnet preactor
        @note This is non-general
        """
        self.name = name
        self.factory = factory
        self.preactor = preactor

    def startService(self):
        service.Service.startService(self)
        self._producer = self.preactor.connectSimpleProducer(self.name, self.factory)

    def stopService(self):
        service.Service.stopService(self)
        if self._producer is not None:
            self._producer.disconnect()
            del self._producer



class MSWorkConsumer(service.Service):
    """
    Service for preactor.connectMS
    """

    _consumer = None

    def __init__(self, name, factory, preactor):
        """
        @param name message service name to listen on
        @param factory protocol factory
        @param preactor The running Magnet preactor
        @note This is non-general
        """
        self.name = name
        self.factory = factory
        self.preactor = preactor

    def startService(self):
        service.Service.startService(self)
        log.msg('start consumer')
        self._consumer = self.preactor.connectWorkConsumer(self.name, self.factory)
        log.msg(self._consumer)

    def stopService(self):
        service.Service.stopService(self)
        log.msg('stop consumer')
        if self._consumer is not None:
            self._consumer.disconnect()
            del self._consumer

class MSWorkProducer(service.Service):
    """
    Service for preactor.connectMS
    """

    _producer = None

    def __init__(self, name, factory, preactor):
        """
        @param name message service name to listen on
        @param factory protocol factory
        @param preactor The running Magnet preactor
        @note This is non-general
        """
        self.name = name
        self.factory = factory
        self.preactor = preactor

    def startService(self):
        service.Service.startService(self)
        self._producer = self.preactor.connectWorkProducer(self.name, self.factory)

    def stopService(self):
        service.Service.stopService(self)
        if self._producer is not None:
            self._producer.disconnect()
            del self._producer



