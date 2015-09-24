***********
Development
***********
How to start
============

We try to make it as easy as possible to start development on snakebite.
We recommend to use virtualenv (+ virtualenvwrapper) for development purposes,
it's not required to but highly recommended. To install, and create development
environment for snakebite:

1. install virtualenvwrapper:
``$ pip install virtualenvwrapper``
2. create development environment:
``$ mkvirtualenv snakebite_dev``

More about virtualenvwrapper and virtualenv `here <http://virtualenvwrapper.readthedocs.org/en/latest/>`_

Below is the list of recommended steps to start development:

1. clone repo:
``$ git clone git@github.com:spotify/snakebite.git``
2. fetch all developer requirements:
``$ pip install -r requirements-dev.txt``
3. run tests:
``$ python setup.py test``

If tests succeeded you are ready to hack! Remember to always test
your changes and please come back with a PR <3

Open issues
===========

If you're looking for open issues please take a look `here <https://github.com/spotify/snakebite/issues>`_.

Thanks!
