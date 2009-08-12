#!/usr/bin/env python

from distutils.core import setup

setup(name='magnet',
        version='0.3.0',
        description='',
        author='Dorian Raymer', 
        author_email='deldotdr@gmail.com',
        packages=['magnet'],
        package_data={
            'magnet':['spec/*.xml'],
            }
        )
