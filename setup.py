from setuptools import setup, find_packages

setup(name='dojotmodulepython',
      version='0.49',
      description='dojot module',
      url='http://github.com/matheuscampanhaf',
      author='bla',
      author_email='bla@bla.com',
      license='MIT',
      packages=find_packages(exclude=['test']),
      install_requires=['requests','kafka-python'],
      zip_safe=False)