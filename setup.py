#!/usr/bin/env python

from distutils.core import setup

from magnet import __version__ as version

setup(name='magnet',
        version=version,
        description='',
        author='Dorian Raymer', 
        author_email='deldotdr@gmail.com',
        packages=['magnet'],
        package_data={
            'magnet':['magnet.conf', 'spec/*.xml'],
            }
        )
