Installation
=====================

UrbanAccess relies on a number of libraries in the scientific Python stack which can be easily installed using the `Anaconda`_ python distribution which can be downloaded `here <https://www.continuum.io/downloads>`__.

Dependencies
------------

* requests >= 2.9.1
* six >= 1.11
* pandas >= 0.17.0
* numpy >= 1.11
* osmnet >= 0.1.4
* pandana >= 0.2.0
* matplotlib >= 2.0
* geopy >= 1.11.0
* pyyaml >= 3.11
* scikit-learn >= 0.17.1

Dependencies can be installed through the ``conda-forge`` and ``udst`` channels. To add these as default installation channels for conda, run this code in a terminal::

    conda config --add channels udst
    conda config --add channels conda-forge

Current status
--------------

*Forthcoming improvements:*

* Unit tests

Install the latest release
--------------------------

conda
~~~~~~
UrbanAccess is available on conda and can be installed with::

    conda install -c udst urbanaccess

pip
~~~~~~
UrbanAccess is available on PyPI and can be installed with::

    pip install -U urbanaccess

Development Installation
------------------------

Developers contributing code can install using the ``develop`` command rather than ``install``. Make sure you are using the latest version of the codebase by using git's ``git pull`` inside the cloned repository.

To install UrbanAccess follow these steps:

1. Git clone the `UrbanAccess repo <https://github.com/udst/urbanaccess>`__
2. in the cloned directory run: ``python setup.py develop``

To update to the latest development version:

Use ``git pull`` inside the cloned repository


.. _Anaconda: http://docs.continuum.io/anaconda/