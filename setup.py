import versioneer
from setuptools import setup


setup(name='event_model',
      packages=['event_model'],
      author='Brookhaven National Laboratory',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      package_data={'event_model': ['schemas/*.json']},
      include_package_data=True)
