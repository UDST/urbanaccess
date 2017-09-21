import os
import time

import urbanaccess
from urbanaccess.gtfsfeeds import feeds

start_time = time.time()

name = 'gb'
url = 'http://www.gbrail.info/gtfs.zip'

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
bbox = (-0.5383, 51.3546, 0.2856, 51.7508)
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
    timerange=['07:00:00', '10:00:00'])

print('{} integration test completed successfully. Took {:,'
      '.2f} seconds'.format(name, time.time() - start_time))
print ('-------------------------')
