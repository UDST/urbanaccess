Installation
=====================

Under Construction

UrbanAccess relies on a number of libraries in scientific Python stack which can be easily installed using the `Anaconda`_ python distribution.

Dependencies
~~~~~~~~~~~~~~~~~~

* requests >= 2.9.1
* pandas >= 0.16.0
* numpy >= 1.11
* osmnet >= 0.1a
* pandana >= 0.2.0
* matplotlib >= 2.0
* geopy >= 1.11.0
* pyyaml >= 3.11
* scikit-learn >= 0.17.1

Current status
~~~~~~~~~~~~~~~~~~

UrbanAccess is currently in a alpha release. Further code refinements and speed optimizations are expected.

*Forthcoming improvements:*

* Tutorial/demo
* Unit tests
* Speed optimizations

Installation
~~~~~~~~~~~~~~

pip and conda installations are forthcoming. UrbanAccess is currently in a alpha release and further code refinements and speed optimizations are expected. As such, it is suggest to install using the ``develop`` command rather than the ``install``. Make sure you are using the latest version of the code base by using git's ``git pull`` inside the cloned repository.

To install UrbanAccess follow these steps:

1. Git clone the `UrbanAccess repo <https://github.com/udst/urbanaccess>`__
2. in the cloned directory run: ``python setup.py develop``

To update to the latest version:

Use ``git pull`` inside the cloned repository


.. _Anaconda: http://docs.continuum.io/anaconda/