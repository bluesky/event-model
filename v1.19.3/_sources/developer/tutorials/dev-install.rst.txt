Developer install
=================

These instructions will take you through the minimal steps required to get a dev
environment setup, so you can run the tests locally.

Clone the repository
--------------------

First clone the repository locally using `Git
<https://git-scm.com/downloads>`_::

    $ git clone git://github.com/bluesky/event-model.git

Install dependencies
--------------------

You should install into a `venv` (which requires python 3.8 or later):
.. code::

    $ cd event-model
    $ python3 -m venv venv
    $ source venv/bin/activate
    $ pip install -e '.[dev]'

See what was installed
----------------------

To see a graph of the python package dependency tree type::

    $ pipdeptree

Build and test
--------------

Now you have a development environment you can run the tests in a terminal::

    $ tox -p

This will run in parallel the following checks:

- `../how-to/build-docs`
- `../how-to/run-tests`
- `../how-to/static-analysis`
- `../how-to/lint`
