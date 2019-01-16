from os import path
import versioneer
from setuptools import setup


here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'requirements.txt')) as requirements_file:
    # Parse requirements.txt, ignoring any commented-out lines.
    requirements = [line for line in requirements_file.read().splitlines()
                    if not line.startswith('#')]


setup(name='event_model',
      packages=['event_model'],
      author='Brookhaven National Laboratory',
      description='Data model for event-based data collection and analysis',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      package_data={'event_model': ['schemas/*.json']},
      install_requires=requirements,
      include_package_data=True)
