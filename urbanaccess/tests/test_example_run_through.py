import geopandas as gpd
import pandana as pdna
import pandas as pd
import pytest
import shapely.wkt
import sys
import urbanaccess

# Note: This is a modified version of the example.py gist and is intended
#       to act as a stand in for some real, more directed unit tests for
#       this library. In the meantime, it covers a general "script" walking
#       through a general/common use case with the library.

def test_workflow():

    # helper functions
    def _parse_wkt(s):
        """Parse wkt and ewkt strings into shapely shapes.

        For ewkt (the PostGIS extension to wkt), the SRID indicator is removed.
        """
        if s.startswith('SRID'):
            s = s[s.index(';') + 1:]
        return shapely.wkt.loads(s)

    long_dash = ''.join(['-' for n in range(25)])
    def update_status(custom_note='', clarify=False):
        for item in ['\n', long_dash, custom_note]:
            print(item)


    # let's find transit providers in Madison, WI
    search_results = urbanaccess.gtfsfeeds.search(search_text='madison')

    # =============
    # Section break
    # =============
    update_status(('We just queried for transit providers with UA. '
                    'Next, we will specify a transit resource to download.'))

    # add a feed to the gtfs to include in the analysis
    feeds = urbanaccess.gtfsfeeds.feeds
    name = 'madison'
    # Note: query suggests: http://www.cityofmadison.com/metro/gtfs/mmt_gtfs.zip
    #       but this address is currently is 404'ing (04-16-2017)
    #       ...as a result, using link below which _does_ work in the meantime
    url = 'http://www.gtfs-data-exchange.com/agency/city-of-madison/latest.zip'
    new_feed =  {name:url}
    feeds.add_feed(new_feed)

    # download the feed, will be placed in folders within data/gtfsfeed_text
    # according to the dict key name
    urbanaccess.gtfsfeeds.download()

    # =============
    # Section break
    # =============
    update_status(('Next, we need to load the feeds into a Pandas DataFrame.'))

    # now that we have saved the raw gtfs data, we need to load it in
    gtfsfeed_path = None # use default gtfs save location
    validation = True
    verbose = True
    bbox = (-89.441414,43.047314,-89.325371,43.129553)
    remove_stops_outsidebbox = True
    append_definitions = True
    # updates these attributes: stops, routes, trips, stop_times, calendar,
    #                           calendar_dates, stop_times_int, headways
    loaded_feeds = urbanaccess.gtfs.load.gtfsfeed_to_df(
                                            gtfsfeed_path,
                                            validation,
                                            verbose,
                                            bbox,
                                            remove_stops_outsidebbox,
                                            append_definitions)

    # =============
    # Section break
    # =============
    update_status(('Next, we need to interpolate stop times data from GTFS.'))

    # what remains an empty dataframe is stop_times_int, which we still
    # need to generate before we can get to calculating headways
    columns = ['route_id', 'direction_id', 'trip_id', 'service_id', 
               'unique_agency_id']
    day = 'wednesday' # pick an arbitrary day of week
    tripschedualselector = urbanaccess.gtfs.network._trip_schedule_selector
    cal_selected_trips = tripschedualselector(
                                    loaded_feeds.trips[columns],
                                    loaded_feeds.calendar,
                                    loaded_feeds.calendar_dates,
                                    day)

    # approximate missing stop times via linear interpolation
    interpolatestoptimes = urbanaccess.gtfs.synthesize.interp_stop_times
    intermediate_interpolation = interpolatestoptimes(
                                    loaded_feeds.stop_times,
                                    cal_selected_trips,
                                    day)

    # now calculate the difference in top times in new column
    timedifference = urbanaccess.gtfs.synthesize.add_time_difference
    stop_times_int = timedifference(intermediate_interpolation)

    # now we can update loaded_feeds with this new dataframe
    loaded_feeds.stop_times_int = stop_times_int

    # =============
    # Section break
    # =============
    update_status(('Now we can calculate headways with the interpolated data.'))

    # now we need to calculate the headways, given the downloaded gtfs
    headway_timerange = ['07:00:00','10:00:00'] # approx a.m. peak
    # the below function updates loaded_feeds, so that headways is populated
    loaded_feeds = urbanaccess.gtfs.headways.headways(loaded_feeds,
                                                      headway_timerange)

    # =============
    # Section break
    # =============
    update_status(('At this point we are able to save/reload the data locally.'))

    # save the results from these initial processing steps locally
    filename = 'temp_network_analyzed.h5'
    urbanaccess.gtfs.network.save_processed_gtfs_data(loaded_feeds,
                                                      'data',
                                                      filename)
    # we can now reload from that save location if we want
    loaded_feeds = urbanaccess.gtfs.network.load_processed_gtfs_data('data',
                                                                     filename)

    # =============
    # Section break
    # =============
    update_status(('Next, we need to generate the transit and osm networks.'))

    # to proceed, we need to generate a network describing the transit data
    ua_network = urbanaccess.gtfs.network.create_transit_net(
                                    gtfsfeeds_dfs=loaded_feeds,
                                    day=day,
                                    timerange=headway_timerange,
                                    overwrite_existing_stop_times_int=False,
                                    use_existing_stop_times_int=True,
                                    save_processed_gtfs=False)

    update_status(('Transit feed downloaded, now we want OSM from bbox.'))
    # now we're ready to download OSM data, let's use same bbox from gtfs search
    bbox = (-89.871597,42.824365,-88.972778,43.307942) # big
    # bbox = (-89.395366,43.06663,-89.373479,43.082305) # small
    osm_nodes, osm_edges = urbanaccess.osm.load.ua_network_from_bbox(bbox=bbox)

    update_status(('Both networks downloaded, now can clean osm data.'))
    # clean up the osm edges, make them type string to allow concatenating
    osm_edges['from'] = osm_edges['from'].astype(str)
    osm_edges['to'] = osm_edges['to'].astype(str)
    osm_edges['id'] = osm_edges['from'].str.cat(osm_edges['to'])

    keep = ['id',
            'access',
            'bridge',
            'distance',
            'from',
            'hgv',
            'highway',
            'lanes',
            'maxspeed',
            'name',
            'oneway',
            'ref',
            'service',
            'to',
            'tunnel']

    # ensure that we have all the cols we need
    for col in keep:
        # make all missing but required cols empty
        if col not in osm_edges.columns.values:
            print('Missing column: ' + str(col))
            osm_edges[col] = ''

    osm_edges = osm_edges[keep]
    osm_edges.drop_duplicates(subset='id', keep='first', inplace=True)

    update_status(('Next, create an OSM network in Pandana ready format.'))
    # with osm data, we can create a network just as we did with the gtfs data
    ua_network = urbanaccess.osm.network.create_osm_net(
                                    osm_edges = osm_edges,
                                    osm_nodes = osm_nodes,
                                    travel_speed_mph = 3, # walk speed average
                                    network_type = 'walk')

    # =============
    # Section break
    # =============
    update_status(('Now we have all networks we need, '
                   'so we can integrate them.'))

    # result urbanaccess_nw vars is an object with the following attributes:
    #   osm_edges, osm_nodes,
    #   transit_edges, transit_nodes
    # and then returns the above, plus:
    #   net_connector_edges, net_edges, net_nodes
    urbanaccess_nw = urbanaccess.network.integrate_network(
                                    urbanaccess_network=ua_network,
                                    headways=True,
                                    urbanaccess_gtfsfeeds_df=loaded_feeds,
                                    headway_statistic='mean')

    # now to shift over to pandana's domain
    nod_x = urbanaccess_nw.net_nodes['x'].astype(float)
    nod_y = urbanaccess_nw.net_nodes['y'].astype(float)

    # use the integer representation of each from and to id
    # (pandana can't handle them as strings)
    fr_null = urbanaccess_nw.net_edges['from_int'].isnull()
    to_null = urbanaccess_nw.net_edges['to_int'].isnull()
    print('There are {} disconnected edges.'.format(
        urbanaccess_nw.net_edges[(fr_null | to_null)]))
    urbanaccess_nw.net_edges = urbanaccess_nw.net_edges[~(fr_null | to_null)]
    
    edg_fr = urbanaccess_nw.net_edges['from_int'].astype(int)
    edg_to = urbanaccess_nw.net_edges['to_int'].astype(int)
    edg_wt_df = urbanaccess_nw.net_edges[['weight']].astype(float)

    # insantiate a pandana network object
    # set twoway to false since UA networks are oneways
    p_net = pdna.Network(nod_x, nod_y, edg_fr, edg_to, edg_wt_df, twoway=False)

    # precompute step, requires a max 'horizon' distance
    horizon_dist = 60
    p_net.precompute(horizon_dist)

    # read in an example dataset
    blocks_df = pd.read_csv('./urbanaccess/tests/fixtures/blocks.csv')
    geometry = blocks_df['geometry'].map(_parse_wkt)
    blocks_df = blocks_df.drop('geometry', axis=1)
    crs = {'init': 'epsg:4326'}
    blocks_gdf = gpd.GeoDataFrame(blocks_df, crs=crs, geometry=geometry)

    # we need to extract the point lat/lon values
    blocks_gdf['x'] = blocks_gdf.centroid.map(lambda p: p.x)
    blocks_gdf['y'] = blocks_gdf.centroid.map(lambda p: p.y)

    #  set node_ids as an attribute on the geodataframe
    blocks_gdf['node_ids'] = p_net.get_node_ids(blocks_gdf['x'],
                                                blocks_gdf['y'])
    p_net.set(blocks_gdf['node_ids'],
              variable=blocks_gdf['emp'],
              name='emp')

    # let's just make sure it can run through at least one aggregation
    # without erroring in pandana and be happy with that for now

    # Note: the results will be very limited because the blocks do not overlap
    #       well with the network dataset from the bbox
    s = p_net.aggregate(15,
                        type='sum',
                        decay='flat',
                        imp_name='weight',
                        name='emp')
