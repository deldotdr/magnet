
import os
import sys
import boto

from twisted.application import service

import magnet
magnet_path = magnet.__path__[0]
spec_path = magnet_path + '/amqp0-8.xml'

from magnet.mgmt.service import Unit
from magnet.mgmt.service import Provisioner

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    ec2 = boto.connect_ec2(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)
else:
    print 'Need AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY environment variables'
    sys.exit(1)


erddap_util_config_config = {
        'public_dns_name':'erddap_util',
        'memcachedHostname':'get_memcached',
        'useMessagingLoadDataset':'false',
        'brokerHostname':'get_rabbitmq',
        'exchange':'xxxxx',
        'loadTopic':'xxxxx',
        'statusTopic':'xxxxx',
        'templ_file':'/home/tomcat/magnet/scripts/setup.xml',
        'final_path':'/opt/apache-tomcat-6.0.18/content/erddap/setup.xml',
        }
 

load_erddap_script = magnet_path + '/scripts/load_erddap.sh'
config_erddap_script = magnet_path + '/scripts/config_erddap.sh'
run_erddap_script = magnet_path + '/scripts/run_erddap.sh'
erddap_util_config = {
        'node_type':'erddap_util',
        'ami_id':'ami-b62acedf',
        'num_insts':2,
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'vhost':'/',
        'username':'guest',
        'password':'guest',
        'spec':spec_path,
        'load_app_script':load_erddap_script,
        'config_app_script':True,
        'run_app_script':run_erddap_script,
        'node_config_dict':erddap_util_config_config,
        }


erddap_util = Unit(erddap_util_config, ec2)


erddap_crawl_config_config = {
        'public_dns_name':'erddap_crawl',
        'memcachedHostname':'get_memcached',
        'useMessagingLoadDataset':'true',
        'brokerHostname':'get_rabbitmq',
        'exchange':'crawler',
        'loadTopic':'load',
        'statusTopic':'status',
        'templ_file':'/home/tomcat/magnet/scripts/setup.xml',
        'final_path':'/opt/apache-tomcat-6.0.18/content/erddap/setup.xml',
        }
 
erddap_crawl_config = {
        'node_type':'erddap_crawl',
        'ami_id':'ami-b62acedf',
        'num_insts':2,
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'vhost':'/',
        'username':'guest',
        'password':'guest',
        'spec':spec_path,
        'load_app_script':load_erddap_script,
        'config_app_script':True,
        'run_app_script':run_erddap_script,
        'node_config_dict':erddap_crawl_config_config,
        }



erddap_crawl = Unit(erddap_crawl_config, ec2)


run_memcached_script = magnet_path + '/scripts/run_memcached.sh'
memcached_config_dict = {
        'private_dns_name':'memcached',
        }

memcached_config = {
        'node_type':'memcached',
        'ami_id':'ami-3631d55f',
        'num_insts':1,
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'vhost':'/',
        'username':'guest',
        'password':'guest',
        'spec':spec_path,
        'load_app_script':False,
        'config_app_script':False,
        'run_app_script':run_memcached_script,
        'node_config_dict':memcached_config_dict,
        }
 
memcached = Unit(memcached_config, ec2)

run_rabbit_script = magnet_path + '/scripts/run_rabbitmq.sh'
rabbitmq_config_dict = {
        'private_dns_name':'rabbit',
        }
rabbitmq_config = {
        'node_type':'rabbitmq',
        'ami_id':'ami-672bcf0e',
        'num_insts':1,
        'host':'rabbitmq.amoeba.ucsd.edu',
        'port':5672,
        'vhost':'/',
        'username':'guest',
        'password':'guest',
        'spec':spec_path,
        'load_app_script':False,
        'config_app_script':False,
        'run_app_script':run_rabbit_script,
        'node_config_dict':rabbitmq_config_dict,
        }
 
rabbitmq = Unit(rabbitmq_config, ec2)


provisioner = Provisioner()

erddap_util.setServiceParent(provisioner)
erddap_crawl.setServiceParent(provisioner)
memcached.setServiceParent(provisioner)
rabbitmq.setServiceParent(provisioner)


from twisted.application import internet
from twisted.cred import portal, checkers
from twisted.conch import manhole, manhole_ssh

def getManholeFactory(namespace, **passwords):
    realm = manhole_ssh.TerminalRealm( )
    def getManhole(_): return manhole.Manhole(namespace)
    realm.chainedProtocolFactory.protocolFactory = getManhole
    p = portal.Portal(realm)
    p.registerChecker(
        checkers.InMemoryUsernamePasswordDatabaseDontUse(**passwords))
    f = manhole_ssh.ConchFactory(p)
    return f

manfact = getManholeFactory(locals(), admin='secret')
mansrv = internet.TCPServer(2222, manfact)
 


# main service
application = service.Application('Provisioner')
provisioner.setServiceParent(service.IServiceCollection(application))

mansrv.setServiceParent(application)

