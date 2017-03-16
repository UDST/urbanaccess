.. _network-section:

Create an Integrated Transit and Street Network
=================================================

After loading the transit and street network data, UrbanAccess has tools to compute a travel time weighted graph network of nodes and edges for use with Pandana network accessibility queries.

.. _network-data-object:

The Network Data Object
~~~~~~~~~~~~~~~~~~~~~~~~

The network data object stores the individual transit and street network node and edge components as well as the final integrated network in Pandas dataframes.

.. autofunction:: urbanaccess.network.ua_network

.. autofunction:: urbanaccess.network.urbanaccess_network

.. _transit-network:

Creating a transit network
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a transit network from the loaded GTFS feeds with travel time impedance.

.. autofunction:: urbanaccess.gtfs.network.create_transit_net

.. _street-network:

Creating a street network
~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a street network from the loaded OSM data with travel time impedance.

.. autofunction:: urbanaccess.osm.network.create_osm_net

.. _int-network:

Network Integration
~~~~~~~~~~~~~~~~~~~~

Create an integrated transit and street network.

If using headways in your integrated network you must first compute the route-stop level headway in this step :ref:`here <headways>`.

.. autofunction:: urbanaccess.network.integrate_network

.. _save-network:

Save Network
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: urbanaccess.network.save_network

.. _load-network:

Load Network
~~~~~~~~~~~~~~~~~~~~

.. autofunction:: urbanaccess.network.load_network