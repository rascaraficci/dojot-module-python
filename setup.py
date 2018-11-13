# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
      name='dojot.module',
      version='0.53',
      description='Library for new dojot modules development',
      url='http://github.com/dojot/dojot-module-python',
      author='Matheus Campanha Ferreira',
      author_email='campanha@cpqd.com.br',
      license='GPL-3.0',
      packages=find_packages(exclude=['test']),
      install_requires=['requests','kafka-python', 'colorlog'],
      zip_safe=False
)
