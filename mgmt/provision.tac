
import boto


from twisted.application import service

import magnet
spec_path = magnet.__path__[0] + '/amqp0-8.xml'

from magnet.mgmt.service import Unit

ec2 = boto.connect_ec2("1XE4TR2G8BCV1NEKVRR2", "aAjco0GQRbFud7A9uHjNH4h5ZQUnm0j9iio9Brfr")


erddap_util_config = {
        'node_type':'erddap_crawl',
        'ami_id':'ami-b62acedf',
        'num_insts':2,
        'mgmt_host':'rabbitmq.amoeba.ucsd.edu',
        }
 
erddap_crawl = Unit(erddap_util_config)



# main service
application = service.Application('Provisioner')
erddap_crawl.setServiceParent(service.IServiceCollection(application))



