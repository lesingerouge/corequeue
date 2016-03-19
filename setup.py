try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from os import path

README = path.abspath(path.join(path.dirname(__file__), 'README.rst'))

setup(
      name='corequeue',
      version='0.1',
      description='Redis queue for python',
      author='Daniel Enache',
      author_email='daniel@bakemono.ro',
      url='https://github.com/lesingerouge/corequeue',
      packages=['corequeue'],
      license='MIT',
      install_requires=['redis', 'hiredis']
)
