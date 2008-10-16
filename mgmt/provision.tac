
import boto

import magnet
spec_path = magnet.__path__[0] + '/amqp0-8.xml'

from magnet.amqp.service import AMQPService
from magnet.mgmt.service import StartAMI

ec2 = boto.connect_ec2("1XE4TR2G8BCV1NEKVRR2", "aAjco0GQRbFud7A9uHjNH4h5ZQUnm0j9iio9Brfr")


ed_util_config = {
    'imgid':"ami-f627c39f",
    'N':'2',
    }


# start N ami's of certain type
start_ami = StartAMI(ec2, ed_util_config)

# messaging client service
msg_config = {
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'exchange':'provision',
        'vhost':'/',
        'spec':spec_path,
        'username':'guest',
        'password':'guest',
    }


msg_service = AMQPService(msg_config)

# provisioning operations
opps_config = {
            'exchange':'provision',
            'routing_key':'node.erddap.util',
            'queue':'erddap',
            }

status = task.NodeStatus(opps_config)

setup_apps = task.SetupApps(opps_config)

config_apps = task.ConfigApps(opps_config)

start_apps = task.StartApps(opps_config)


# main service
application = service.Application('Provisioner')
ser.setServiceParent(service.IServiceCollection(application))



