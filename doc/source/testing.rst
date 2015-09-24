*******
Testing
*******

Snakebite provides integration and unit tests for its functionalities.
To be able to truly test integration with HDFS, we provide wrapper
around :mod:`snakebite.minicluster <minicluster>`, and base class for integration
tests ``MiniClusterTestBase`` - on setup for such test class minicluster is
started, when tests are done minicluster is destroyed. There's some
performance overhead - but it's not a problem (yet).

Snakebite by default uses `nose <https://nose.readthedocs.org/en/latest/>`_
and `tox <https://tox.readthedocs.org/en/latest/>`_ for testing. Tests
are integrated with `setup.py`, so to start tests one can simply:
``$ python setup.py test``

Because we require minicluster to fully test snakebite,
java needs to be present on the system.

.. note:: It's possible to run snakebite tests inside snakebite Docker test
  image - to learn more see section Fig below. Note that it's not default
  testing method as it requires Docker to be present.

Tox
===

`Tox <https://tox.readthedocs.org/en/latest/>`_ allow us to create automated
isolated python test environments. It's also a place where we can prepare environment
for testing - like download hadoop distributions, set environment variables etc.
Tox configuration is available in ``tox.ini`` file in root directory.

There are 4 test environments:
 * python 2.6 + CDH
 * python 2.7 + CDH
 * python 2.6 + HDP
 * python 2.7 + HDP

We bootstrap environment with ``pip install -r requirements-dev.txt`` (deps section)
And then we setup environment via ``/scripts/ci/setup_env.sh`` script.
``setup_env.sh`` script downloads hadoop distribution tar, and extracts it.
Help for ``setup_env.sh``::
Setup environment for snakebite tests

  options:
          -h, --help            show brief help
          -o, --only-download   just download hadoop tar(s)
          -e, --only-extract    just extract hadoop tar(s)
          -d, --distro          select distro (hdp|cdh)b


When environment is ready we actually run tests via: ``/scripts/ci/run_tests.sh``

One can run tests manually via ``/scripts/ci/run_tests.sh`` but make sure
that ``HADOOP_HOME`` environment variable exists so that it knows where to find
minicluster jar file. This way it's possible to test snakebite against custom
Hadoop distributions. ``run_tests.sh`` script uses ``nose`` for testing, so that
if you wish to pass anything to nose, just add parameters to ``run_tests.sh``.

One can pass parameters to tox/nose through setup.py via ``--tox-args`` flag:

``$ python setup.py test --tox-args="--recreate -e py26-hdp '--quiet'"``

Will test py26-hdp tox environment, make sure it will be recreated,
and also through ``run_tests.sh`` script instruct nose to be quite.

``$ python setup.py test --tox-args="-e py26-hdp test/test_test.py``

Will use py26-hdp tox environment and also instruct nose to run only
tests from test/test_test.py.

Fig
===

.. note:: Fig is experimental testing method, it's very promissing though.

`Fig <http://www.fig.sh/>`_ is "fast, isolated development environments
using Docker". It abstracts away whole test environment, create completely
fresh and isolated test environments using Docker.

Currently we use base testing image ``ravwojdyla/snakebite_test:base``,
it was created using ``/scripts/build-base-test-docker.sh`` and
``/scripts/Dockerfile``. Base test image is a Ubuntu Trusty with:
* oracle java 7
* python 2.6
* python 2.7
* pip
* CDH distribution
* HDP distribution

Base docker image doesn't change, to create new test image with
current working tree, based on ``ravwojdyla/snakebite_test:base``:

``$ fig build``

Fig will create new image based on ``ravwojdyla/snakebite_test:base``,
with current working tree, that can be used for tests.
Fig currently specifies 4 tests:
* ``testPy26cdh``: python 2.6 + CDH
* ``testPy26hdp``: python 2.6 + HDP
* ``testPy27cdh``: python 2.7 + CDH
* ``testPy27hdp``: python 2.7 + HDP

To run specific test (eg. testPy26cdh):

``$ fig run testPy26cdh``

The biggest value in Fig is that tests are completely isolated,
all the snakebite dependencies are present on test image. Unfortunately
Fig depends on Docker - which is quite a big dependency to have, and that's
why it's default method of testing for snakebite. It's worth to mention that
Fig still uses Tox inside test container.
