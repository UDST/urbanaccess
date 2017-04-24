.. _dataloader-section:

Download and Load Data
========================

UrbanAccess has data downloaders and data loaders that can be used to acquire and load transit and street network datasets:

1. Transit Network: General Transit Feed Specification (GTFS)

* :ref:`Download <gtfs-download>`
* :ref:`Load <gtfs-loader>`

2. Street Network: OpenStreetMap (OSM)

* :ref:`Download and Load <osm-download-load>`

.. _gtfs-data-object:

The GTFS Data Object
~~~~~~~~~~~~~~~~~~~~~~

The GTFS data object stores the processed and aggregated GTFS feed data in Pandas dataframes.

.. autodata:: urbanaccess.gtfs.gtfsfeeds_dataframe.gtfsfeeds_dfs
    :annotation:

.. _gtfs-download:

Downloading GTFS Data
~~~~~~~~~~~~~~~~~~~~~~

Manage and download multiple feeds at once using the ``feeds`` object.

.. autodata:: urbanaccess.gtfsfeeds.feeds
    :annotation:

The ``feeds`` object is an instance of the ``urbanaccess_gtfsfeeds`` class with the following functions:

.. autoclass:: urbanaccess.gtfsfeeds.urbanaccess_gtfsfeeds
    :members:

Search for feeds on the GTFS Data Exchange (Note the GTFS Data Exchange is no longer being maintained as of Summer 2016 so feeds here may be out of date).

.. autofunction:: urbanaccess.gtfsfeeds.search

Download data from feeds in your ``feeds`` object or from custom feed URLs.

.. autofunction:: urbanaccess.gtfsfeeds.download

.. _gtfs-loader:

Loading GTFS Data
~~~~~~~~~~~~~~~~~~~

Load raw GTFS data (from multiple feeds) into a UrbanAccess transit data object and run data through the validation and formatting sequence.

GTFS feeds are assumed to either be a single feed designated by the feed folder or multiple feeds desginated as the root folder that holds all individual feed folders.

.. autofunction:: urbanaccess.gtfs.load.gtfsfeed_to_df

.. _headways:

Computing route-stop level headways is optional but required if you wish to use headways in your network integration step :ref:`here <int-network>`.

.. autofunction:: urbanaccess.gtfs.headways.headways

.. _save-gtfs:

Saving Processed GTFS data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: urbanaccess.gtfs.network.save_processed_gtfs_data

.. _load-gtfs:

Loading Processed GTFS data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: urbanaccess.gtfs.network.load_processed_gtfs_data

.. _osm-download-load:

Downloading and Loading OpenStreetMap Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download OSM street network nodes and edges.

.. autofunction:: urbanaccess.osm.load.ua_network_from_bbox

