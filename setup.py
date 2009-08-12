#!/usr/bin/env python

from distutils.core import setup

setup(name='misted',
        version='0.2.0',
        description='',
        author='Dorian Raymer', 
        author_email='deldotdr@gmail.com',
        packages=['misted'],
        package_data={
            'misted':['spec/*.xml'],
            }
        )
