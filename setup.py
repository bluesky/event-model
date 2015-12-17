import versioneer
from setuptools import setup


setup(name='event_model',
      py_modules=['event_model'],
      author='Brookhaven National Laboratory',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      package_data={'schemas': ['*.json']},)
