import versioneer
from setuptools import setup


setup(name='event_model',
      packages=['event_model'],
      author='Brookhaven National Laboratory',
      description='Data model for event-based data collection and analysis',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      package_data={'event_model': ['schemas/*.json']},
      install_requires=['jsonschema'],
      include_package_data=True)
