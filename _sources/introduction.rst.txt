.. _intro-section:

Introduction
=============

UrbanAccess: A tool for computing GTFS transit and OSM pedestrian networks for accessibility analysis.

Overview
~~~~~~~~~~

UrbanAccess is tool for creating multi-modal graph networks for use in multi-scale (e.g. address level to the metropolitan level) transit accessibility analyses with the network analysis tool Pandana. UrbanAccess uses open data from General Transit Feed Specification (GTFS) data to represent disparate operational schedule transit networks and pedestrian OpenStreetMap (OSM) data to represent the pedestrian network. UrbanAccess provides a generalized, computationally efficient, and unified accessibility calculation framework by linking tools for: 1) network data acquisition, validation, and processing; 2) computing an integrated pedestrian and transit weighted network graph; and 3) network analysis using Pandana.

UrbanAccess offers the following tools:

* GTFS and OSM network data acquisition via APIs
* Network data validation and regional network aggregation
* Compute network impedance:

  * by transit schedule day of the week and time of day
  * by transit mode
  * by including average passenger headways to approximate passenger transit stop wait time

* Integrate pedestrian and transit networks to approximate pedestrian scale accessibility
* Resulting networks are designed to be used to compute accessibility metrics using the open source network analysis tool `Pandana <https://github.com/UDST/pandana>`__

|travel_time_net|
*Integrated AC Transit and BART transit and pedestrian network travel times for Oakland, CA*

Minimum GTFS data requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The minimum `GTFS data types <https://developers.google.com/transit/gtfs/>`__ required to use UrbanAccess are: ``stop_times``, ``stops``, ``routes``, ``calendar``, and ``trips``

License
~~~~~~~~

UrbanAccess is licensed under the AGPL license.

Citation and academic literature
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To cite this tool and for a complete description of the UrbanAccess methodology see the paper below:

`Samuel D. Blanchard and Paul Waddell. Forthcoming 2017. "UrbanAccess: A Generalized Methodology for Measuring Regional Accessibility with an Integrated Pedestrian and Transit Network." Transportation Research Record: Journal of the Transportation Research Board. No. 2653.`

Reporting bugs
~~~~~~~~~~~~~~~~~~~~~~~~
Please report any bugs you encounter via `GitHub Issues <https://github.com/UDST/urbanaccess/issues>`__.

Contributing to UrbanAccess
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you have improvements or new features you would like to see in UrbanAccess:

1. Open a feature request via `GitHub Issues <https://github.com/UDST/urbanaccess/issues>`__.
2. Contribute your code from a fork or branch by using a Pull Request and request a review so it can be considered as an addition to the codebase.

Related UDST libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~
- `Pandana <https://github.com/UDST/pandana>`__
- `OSMnet <https://github.com/UDST/osmnet>`__

General workflow
~~~~~~~~~~~~~~~~~~~

1. **Download and load data**

* :ref:`Download GTFS data <gtfs-download>`
* :ref:`Load and process GTFS data <gtfs-loader>`
* :ref:`Download and load OSM street network data <osm-download-load>`

2. **Create network graphs**

* :ref:`Transit network: Create a travel time weight graph network <transit-network>`
* :ref:`Street network: Create a travel time weight graph network <street-network>`
* :ref:`Integrate the two graph networks <int-network>`

3. **Plot the network**

* :ref:`Inspect the network and visualize the impedance <plot-section>`

4. **Compute a network analysis**

* `Compute an accessibility query using Pandana <https://github.com/UDST/pandana>`__

.. |travel_time_net| image:: _images/travel_time_net.png
	:scale: 80%