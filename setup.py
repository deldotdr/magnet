#!/usr/bin/env python

from distutils.core import setup

setup(name='magnet',
        version='0.2.0',
        description='Interacting Message Agents over AMQP',
        author='Dorian Raymer', 
        author_email='deldotdr@gmail.com',
        packages=['magnet', 'twisted'],
        package_data={
            'magnet':['spec/*.xml'],
            'twisted':[
                'plugins/magnet_plugin.py'
                ]
            }
        )
