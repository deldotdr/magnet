
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

config_task_report = {
            'exchange':'announce',
            'routing_key':'erddap',
            'queue':'erddap.announce',
            }

config_task_status = {
            'exchange':'status',
            'routing_key':'erddap',
            'queue':'erddap.status',
            }

config_task_runscript = {
            'exchange':'command',
            'routing_key':'erddap.runscript',
            'queue':'erddap.runscript',
            }


task_report = ReportHostname(config_task)
task_status =  Status(config_task)
task_runscript = RunScript(config_task)

task_status.setServiceParent(agent_service)
task_report.setServiceParent(agent_service)
task_runscript.setServiceParent(agent_service)


application = service.Application('ContextAgent')

agent_service.setServiceParent(service.IServiceCollection(application))
