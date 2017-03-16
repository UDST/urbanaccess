.. _plot-section:

Plot Networks
==============

UrbanAccess offers some basic plotting methods to visualize your UrbanAccess network data.

For example you can:

**Plot the transit network**
|transit_net|
*AC Transit and BART transit network for Oakland, CA*

**Plot the street network network**
|ped_net|
*Pedestrian network for Oakland, CA*

**Plot the travel times on the integrated network**
|travel_time_net|
*Integrated AC Transit and BART transit and pedestrian network travel times for Oakland, CA*


.. autofunction:: urbanaccess.plot.plot_net

.. autofunction:: urbanaccess.plot.col_colors


.. |transit_net| image:: _images/transit_net.png
	:scale: 80%
.. |ped_net| image:: _images/ped_net.png
	:scale: 80%
.. |travel_time_net| image:: _images/travel_time_net.png
	:scale: 80%