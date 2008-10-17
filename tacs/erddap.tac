
from twisted.application import service

from magnet.agent.service import AMQPService
from magnet.agent.service import ReportHostname
from magnet.agent.service import RunScript
from magnet.agent.service import Status

import magnet
spec_path = magnet.__path__[0] + '/amqp0-8.xml'

# Client config object
config = {
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'exchange':'provision',
        'vhost':'/',
        'spec':spec_path,
        'username':'guest',
        'password':'guest',
    }


agent_service = AMQPService(config)

config_task = {
            'exchange':'provision',
            'routing_key':'node.erddap',
            'base_routing_key':'node.erddap',
            'queue':'erddap',
            }

task_report = ReportHostname(config_task)
task_status =  Status(config_task)
task_runscript = RunScript(config_task)



agent_service.addService(task_status)
agent_service.addService(task_report)
agent_service.addService(task_runscript)


application = service.Application('ContextAgent')

agent_service.setServiceParent(service.IServiceCollection(application))
