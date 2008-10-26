
import os
import sys
import boto

from twisted.application import service

import magnet
magnet_path = magnet.__path__[0]
spec_path = magnet_path + '/amqp0-8.xml'

from magnet.mgmt.service import Unit
from magnet.mgmt.service import EC2Provisioner

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

if AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY:
    pass
else:
    print 'Need AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY environment variables'
    sys.exit(1)

provisioner = EC2Provisioner(aws_access_key=AWS_ACCESS_KEY, 
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

 

load_erddap_script = magnet_path + '/scripts/load_erddap.sh'
erddap_util_setup = magnet_path + '/scripts/erddap_util_setup.xml'
final_setup_path = '/opt/apache-tomcat-6.0.18/content/erddap/setup.xml'
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
        'setup_templ':erddap_util_setup,
        'final_setup_path':final_setup_path,
        'config_app':True,
        'run_app_script':run_erddap_script,
        }


erddap_util = Unit(erddap_util_config)


erddap_crawl_setup = magnet_path + '/scripts/erddap_crawl_setup.xml'
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
        'setup_templ':erddap_crawl_setup,
        'final_setup_path':final_setup_path,
        'config_app':True,
        'run_app_script':run_erddap_script,
        }



erddap_crawl = Unit(erddap_crawl_config)


run_memcached_script = magnet_path + '/scripts/run_memcached.sh'

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
        'config_app':False,
        'run_app_script':run_memcached_script,
        }
 
memcached = Unit(memcached_config)

run_rabbit_script = magnet_path + '/scripts/run_rabbitmq.sh'
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
        'config_app':False,
        'run_app_script':run_rabbit_script,
        }
 
rabbitmq = Unit(rabbitmq_config)



erddap_util.setServiceParent(provisioner)
erddap_crawl.setServiceParent(provisioner)
memcached.setServiceParent(provisioner)


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

