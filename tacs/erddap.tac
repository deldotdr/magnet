
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
        'vhost':'/',
        'spec':spec_path,
        'username':'guest',
        'password':'guest',
    }


agent_service = AMQPService(config)

config_task_report = {
            'exchange':'status',
            'routing_key':'erddap',
            'queue':'erddap',
            }

config_task_status = {
            'exchange':'status',
            'routing_key':'erddap',
            'queue':'erddap',
            }

config_task_runscript = {
            'exchange':'command',
            'routing_key':'erddap',
            'queue':'erddap',
            }


task_report = ReportHostname(config_task_report)
task_status =  Status(config_task_status)
task_runscript = RunScript(config_task_runscript)

task_status.setServiceParent(agent_service)
task_report.setServiceParent(agent_service)
task_runscript.setServiceParent(agent_service)


application = service.Application('ContextAgent')

agent_service.setServiceParent(service.IServiceCollection(application))
