import os
import time

import urbanaccess
from urbanaccess.gtfsfeeds import feeds

start_time = time.time()

name = 'madison'
url = 'http://www.gtfs-data-exchange.com/agency/city-of-madison/latest.zip'

print ('-------------------------')
print('Starting integration test for {}...'.format(name))

new_feed = {name: url}
feeds.add_feed(new_feed)

script_path = os.path.dirname(os.path.realpath(__file__))
root_path = os.path.join(script_path, 'data', name)
data_path = os.path.join(root_path, 'gtfsfeed_text')

urbanaccess.gtfsfeeds.download(data_folder=root_path)

validation = True
verbose = True
# small bbox for testing purposes
bbox = (-89.426651, 43.045809, -89.326401, 43.105495)
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
    day='wednesday',
    timerange=['07:00:00', '10:00:00'])

loaded_feeds = urbanaccess.gtfs.headways.headways(loaded_feeds,
                                                  headway_timerange=[
                                                      '07:00:00', '10:00:00'])

osm_nodes, osm_edges = urbanaccess.osm.load.ua_network_from_bbox(bbox=bbox)

ua_network = urbanaccess.osm.network.create_osm_net(
    osm_edges=osm_edges,
    osm_nodes=osm_nodes,
    travel_speed_mph=3,
    network_type='walk')

urbanaccess_nw = urbanaccess.network.integrate_network(
    urbanaccess_network=ua_network,
    headways=True,
    urbanaccess_gtfsfeeds_df=loaded_feeds,
    headway_statistic='mean')

color_range = urbanaccess.plot.col_colors(df=urbanaccess_nw.net_edges,
                                          col='weight',
                                          num_bins=5,
                                          cmap='YlOrRd',
                                          start=0.1,
                                          stop=0.9)

urbanaccess.plot.plot_net(nodes=urbanaccess_nw.net_nodes,
                          edges=urbanaccess_nw.net_edges,
                          bbox=bbox,
                          fig_height=25,
                          margin=0.02,
                          edge_color=color_range,
                          edge_linewidth=1,
                          edge_alpha=1,
                          node_color='black',
                          node_size=1,
                          node_alpha=1,
                          node_edgecolor='none',
                          node_zorder=3,
                          nodes_only=False)

print('{} integration test completed successfully. Took {:,'
      '.2f} seconds'.format(name, time.time() - start_time))
print ('-------------------------')
