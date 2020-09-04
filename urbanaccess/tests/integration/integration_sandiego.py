import os
import time
import pandas as pd

import matplotlib

matplotlib.use('agg')
import matplotlib.pyplot as plt

import cartopy.crs as ccrs

import urbanaccess

start_time = time.time()

name = 'san diego'

print('-------------------------')
print('Starting integration test for {}...'.format(name))

script_path = os.path.dirname(os.path.realpath(__file__))
root_path = os.path.join(script_path, 'data', name)
data_path = os.path.join(root_path, 'gtfsfeed_text')

urbanaccess.gtfsfeeds.search(search_text=name, add_feed=True)

urbanaccess.gtfsfeeds.download(data_folder=root_path)

# create dummy calendar.txt file because
dummy_txt_file = os.path.join(root_path,
                              'gtfsfeed_text',
                              'MTS',
                              'calendar.txt')

data = {'service_id': -99, 'monday': 0, 'tuesday': 0, 'wednesday': 0,
        'thursday': 0, 'friday': 0, 'saturday': 0, 'sunday': 0}

index = range(1)

pd.DataFrame(data, index).to_csv(dummy_txt_file, index=False)

validation = True
verbose = True
# small bbox for testing purposes
bbox = (-117.291412, 32.655563, -117.097778, 32.806899)
remove_stops_outsidebbox = True
append_definitions = True

loaded_feeds = urbanaccess.gtfs.load.gtfsfeed_to_df(data_path,
                                                    validation,
                                                    verbose,
                                                    bbox,
                                                    remove_stops_outsidebbox,
                                                    append_definitions)

transit_net = urbanaccess.gtfs.network.create_transit_net(
    gtfsfeeds_dfs=loaded_feeds,
    day='monday',
    calendar_dates_lookup={'exception_note': ['FINAL', 'WD'],
                           'schedule_type': 'WD'},
    timerange=['07:00:00', '10:00:00'])

# This is the standard map projection for California
teale_albers = ccrs.AlbersEqualArea(false_northing=-4000000.0, false_easting=0,
                                    central_longitude=-120.0, central_latitude=0,
                                    standard_parallels=(34.0, 40.5))
teale_albers_ax = plt.axes(projection=teale_albers)

urbanaccess.plot.plot_net(nodes=transit_net.transit_nodes,
                          edges=transit_net.transit_edges,
                          bbox=bbox,
                          fig_height=25,
                          margin=0.02,
                          edge_linewidth=1,
                          edge_alpha=1,
                          node_color='black',
                          node_size=1,
                          node_alpha=1,
                          node_edgecolor='none',
                          node_zorder=3,
                          nodes_only=False,
                          ax=teale_albers_ax)

print('{} integration test completed successfully. Took {:,'
      '.2f} seconds'.format(name, time.time() - start_time))
print('-------------------------')
