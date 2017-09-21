import numpy as np
import pandas as pd
from geopy.distance import vincenty
from sklearn.neighbors import KDTree

from urbanaccess.utils import log, _join_camel


def integrate_walk_and_transit_networks(transit_nw,
                                        walk_nw,
                                        gtfsfeeds_df, 
                                        headway_stat='mean'):
    # TODO: Need consistency between lon, lat and x, y
    location_cols_rename = {'lon': 'x', 'lat': 'y'}
    walk_nw['nodes'] = walk_nw['nodes'].rename(columns=location_cols_rename)

    # base case of resulting object
    integrated_nw = {
        'nodes': None,
        'edges': None,
        'connector_edges': None
    }

    # rename from and to columns in transit network but be careful not to
    # conflate the int and non-int names for the to and from columns
    trans_edge_cols = transit_nw['edges'].columns
    if 'node_id_from' not in trans_edge_cols and 'from' in trans_edge_cols:
        rename_cols = {'from': 'node_id_from'}
        transit_nw['edges'].rename(columns=rename_cols, inplace=True)

    if 'node_id_to' not in trans_edge_cols and 'to' in trans_edge_cols:
        rename_cols = {'to': 'node_id_to'}
        transit_nw['edges'].rename(columns=rename_cols, inplace=True)

    # create unique ids based off of from and to destinations
    from_sub = transit_nw['edges'][['node_id_from', 'unique_route_id']]
    from_update = from_sub.apply(lambda x: _join_camel(x), axis=1)
    transit_nw['edges']['node_id_route_from'] = from_update

    to_sub = transit_nw['edges'][['node_id_to', 'unique_route_id']]
    to_update = to_sub.apply(lambda x: _join_camel(x), axis=1)
    transit_nw['edges']['node_id_route_to'] = to_update

    # overwrite nodes data based off of edge data
    stops = gtfsfeeds_df['stops']
    trans_edges = transit_nw['edges']
    transit_nw['nodes'] = _route_id_to_node(stops, trans_edges)

    walk_nds = walk_nw['nodes']
    trans_nds = transit_nw['nodes']
    net_connector_edges = _generate_connector_edges(walk_nds, trans_nds)

    hdwys = gtfsfeeds_df['headways']
    conn_edges = _add_headway_impedance(net_connector_edges, hdwys, 'mean')
    integrated_nw['connector_edges'] = conn_edges

    # change cols in transit edges and nodes
    trans_renames = {'node_id_route_from': 'from',
                     'node_id_route_to': 'to'}
    transit_nw['edges'].rename(columns=trans_renames, inplace=True)

    # get rid of node reference cols now that
    # we have new from and to cols
    drop_cols = ['node_id_from', 'node_id_to']
    transit_nw['edges'].drop(drop_cols, inplace=True, axis=1)

    transit_nw['nodes'].reset_index(inplace=True, drop=False)
    tr_nodes_rename = {'node_id_route': 'id'}
    transit_nw['nodes'].rename(columns=tr_nodes_rename, inplace=True)

    # concat all network components
    net_edges_concat = [transit_nw['edges'],
                        walk_nw['edges'],
                        integrated_nw['connector_edges']]
    integrated_nw['edges'] = pd.concat(net_edges_concat, axis=0)

    net_nodes = [transit_nw['nodes'], walk_nw['nodes']]
    integrated_nw['nodes'] = pd.concat(net_nodes, axis=0)

    return integrated_nw


def _route_id_to_node(stops_df, edges_w_routes):
    # assign route ids to the transit nodes table

    # create unique stop ids
    st_sub = stops_df[['stop_id', 'unique_agency_id']]
    stops_df['unique_stop_id'] = st_sub.apply(lambda x: _join_camel(x), axis=1)

    tmp1 = pd.merge(edges_w_routes[['node_id_from', 'node_id_route_from']],
                    stops_df[['unique_stop_id', 'stop_lat', 'stop_lon']],
                    how='left',
                    left_on='node_id_from',
                    right_on='unique_stop_id',
                    sort=False,
                    copy=False)
    tmp1_cols_to_rename = {'node_id_route_from': 'node_id_route',
                           'stop_lon': 'x',
                           'stop_lat': 'y'}
    tmp1.rename(columns=tmp1_cols_to_rename, inplace=True)

    tmp2 = pd.merge(edges_w_routes[['node_id_to', 'node_id_route_to']],
                    stops_df[['unique_stop_id', 'stop_lat', 'stop_lon']],
                    how='left',
                    left_on='node_id_to',
                    right_on='unique_stop_id',
                    sort=False,
                    copy=False)
    tmp2_cols_to_rename = {'node_id_route_to': 'node_id_route',
                           'stop_lon': 'x',
                           'stop_lat': 'y'}
    tmp2.rename(columns=tmp2_cols_to_rename, inplace=True)

    keep_cols = ['node_id_route', 'x', 'y']
    tmp1_sub = tmp1[keep_cols]
    tmp2_sub = tmp2[keep_cols]
    transit_nodes_w_routes = pd.concat([tmp1_sub, tmp2_sub], axis=0)

    transit_nodes_w_routes.drop_duplicates(subset='node_id_route',
                                           keep='first',
                                           inplace=True)

    # set node index to be unique stop id
    transit_nodes_w_routes = transit_nodes_w_routes.set_index('node_id_route')

    return transit_nodes_w_routes


def _generate_connector_edges(walk_nodes, transit_nodes, walk_speed=4.8):
    # assign impedance to OSM edges as col weight

    # TODO: Need consistency between lon, lat and x, y
    walk_nodes = walk_nodes.rename(columns={'lon': 'x', 'lat': 'y'})

    # default travel speed is in km/hour
    # until we update below logic, need to convert to mph
    walk_speed_mph = (walk_speed / 1.609)  # 3 mph

    xy_cols = ['x', 'y']
    want_trans = transit_nodes[xy_cols]
    want_osm = walk_nodes[xy_cols]

    transit_nodes['nearest_osm_node'] = _nearest_neighbor(want_osm, want_trans)

    # Note: This for loop iterates through each transit node's nearest osm
    #       node and creates two new edges (one from transit to osm and
    #       one the other way around). Each is given a weight, which is the
    #       time it takes to walk directly from one node to the other.
    net_conn_edges = []
    for transit_node_id, row in transit_nodes.iterrows():
        # create new edge between the node in df2 (transit)
        # and the node in openstreetmap (pedestrian)
        osm_node_id = int(row['nearest_osm_node'])
        osm_row = walk_nodes.loc[osm_node_id]

        # get the distance from a given transit node to nearest osm node
        transit_row_point = (row['y'], row['x'])
        osm_row_point = (osm_row['y'], osm_row['x'])
        distance = vincenty(transit_row_point, osm_row_point).miles

        # given that, we can now generate a time cost for this link
        ped_transit_link_cost = distance / walk_speed_mph * 60

        # save the edge
        to_str = 'transit to osm'  # network type
        to_osm = (transit_node_id, osm_node_id, ped_transit_link_cost, to_str)
        net_conn_edges.append(to_osm)

        # make the edge bi-directional
        fr_str = 'osm to transit'  # network type
        from_osm = (osm_node_id, transit_node_id,
                    ped_transit_link_cost, fr_str)
        net_conn_edges.append(from_osm)

    connect_cols = ['from', 'to', 'weight', 'net_type']
    nw_connectors = pd.DataFrame(net_conn_edges, columns=connect_cols)

    return nw_connectors


def _nearest_neighbor(df1, df2, use_4326_constraints=True):
    df1_new = df1.copy() # osm nodes
    df2_new = df2.copy() # transit nodes

    # be aggressive about ensuring float limits (no vals over than 4 decimals)
    for col in ['x', 'y']:
        df1_new[col] = np.around(df1_new[col], decimals=4)
        df2_new[col] = np.around(df2_new[col], decimals=4)

    # drop out any invalid x, y columns from the left
    invalid_osm_rows = (df1_new['x'].isnull() | df1_new['y'].isnull() | 
                        (~np.isfinite(df1_new['x'])) | 
                        (~np.isfinite(df1_new['y'])))
    orig_df1_len = len(df1_new)
    df1_new = df1_new[~invalid_osm_rows]
    cleaned_df1_len = len(df1_new)
    
    # log if any rows were removed from osm nodes dataset
    df1_cleaned_diff = orig_df1_len - cleaned_df1_len
    if df1_cleaned_diff > 0:
        log(('{} OSM node rows ommitted during nearest neighbor calculations'
             'due to being invalid numeric values.').format(df1_cleaned_diff))

    # let's make sure that latitudes and longitudes are not in excess of
    # their geographic limits
    if use_4326_constraints:
        # y is latitude -90 - +90
        # x is longitude -180 - +180
        df1_new = _adjust_outliers(df1_new, 'x', 90)
        df2_new = _adjust_outliers(df2_new, 'x', 90)
        df1_new = _adjust_outliers(df1_new, 'y', 180)
        df2_new = _adjust_outliers(df2_new, 'y', 180)

    # identify problem rows on the right
    invalid_trans_rows = (df2_new['x'].isnull() | df2_new['y'].isnull())
    invalid_trans_rows_ct = len(df2_new[invalid_trans_rows])
    if invalid_trans_rows_ct > 0:
        log(('{} out of {} invalid rows identified for the transit nodes '
             'dataframe, but not removed. These may cause operation to '
             'fail.').format(invalid_trans_rows_ct, len(df2_new)))

    # for xy coordinates df find the nearest in a subsequent dataframe
    kdt = KDTree(df1_new.as_matrix().astype(np.float))
    df2_mtx = df2_new.as_matrix().astype(np.float)

    indexes = kdt.query(df2_mtx, k=1, return_distance=False)

    # this is returning the osmids (indexed) from the left dataframe
    return df1.index.values[indexes]


def _adjust_outliers(df_ref, col, treshhold):
    # proactively prevent mutation outside of function scope
    df = df_ref.copy()

    # first, handle the positive columns
    pos_thresh = abs(treshhold)
    neg_thresh = ((-1) * pos_thresh)
    
    mask_pos = df[col] > pos_thresh
    if len(df[mask_pos]):
        log(('{} rows in transit stops dataset exceeded positive threshold '
             'of {} for {} column.').format(len(df[mask_pos]), pos_thresh, col))
    
    df.loc[mask_pos, col] = pos_thresh - 0.0001
    
    # now handle the negative columns
    mask_neg = df[col] < neg_thresh
    if len(df[mask_neg]):
        log(('{} rows in transit stops dataset exceeded negative threshold '
             'of {} for {} column.').format(len(df[mask_neg]), neg_thresh, col))

    df.loc[mask_neg, col] = neg_thresh + 0.0001

    return df


def _add_headway_impedance(ped_to_transit_edges_df, headways_df, headway_stat):
    # add route stop level headways to the osm to transit connector
    # travel time weight column

    headways_sub = headways_df[[headway_stat, 'node_id_route']]
    osm_to_transit_w_headway = pd.merge(ped_to_transit_edges_df,
                                        headways_sub,
                                        how='left',
                                        left_on=['to'],
                                        right_on=['node_id_route'],
                                        sort=False,
                                        copy=False)

    # Note: Calculate a weighted impedence measure based on halved
    #       headway and precalc'd weight value which was determined as
    #       walk time between transit node and nearest osm node
    halved = (osm_to_transit_w_headway[headway_stat] / 2.0)
    half_plus_weight = osm_to_transit_w_headway['weight'] + halved
    osm_to_transit_w_headway['weight_w_hdwy'] = half_plus_weight

    # Note: Currently a fail safe to ensure that, should an edge
    #       have a null value under the headway statistic section
    #       (e.g. mean is null or missing), then the original
    #       weight is used instead
    orig_weight = osm_to_transit_w_headway['weight']
    osm_to_transit_w_headway['weight_w_hdwy'].fillna(orig_weight, inplace=True)

    # replace the original weight col with new calculated one
    osm_to_transit_w_headway.drop('weight', axis=1, inplace=True)
    rename_cols = {'weight_w_hdwy': 'weight'}
    osm_to_transit_w_headway.rename(columns=rename_cols, inplace=True)

    return osm_to_transit_w_headway


def make_pdna_compatible(edge_df, node_df):
    # Note: Pandana needs ids that are integers, and it silently
    #       drops all those rows (connectors) without that
    #       which leads to confusing results where there is effectively
    #       no access from one node to another
    node_df['id_int'] = range(1, len(node_df) + 1)

    # rename edge_df id so that it does
    # not conflict with node_df id column
    edge_col_rename = {'id': 'edge_id'}
    edge_df.rename(columns=edge_col_rename, inplace=True)

    # we will be re-using the id columns subset a few times
    id_cols = ['id', 'id_int']
    node_ids_sub = node_df[id_cols]

    # now merge the two columns together so we get the node_df
    # integer ids and can attribute them to the edge_df as well
    edge_with_from_int_id = pd.merge(edge_df,
                                     node_ids_sub,
                                     left_on='from',
                                     right_on='id',
                                     sort=False,
                                     copy=False,
                                     how='left')

    # rename the integer id and drop unwanted node_df carry-overs
    edge_from_col_rename = {'id_int': 'from_int'}
    edge_with_from_int_id.rename(columns=edge_from_col_rename, inplace=True)
    edge_with_from_int_id.drop(['id'], axis=1, inplace=True)

    # same process again, but this time we want to merge in the to
    # column instead of the from column and get the integee version
    # of the to column's unique id
    from_and_to_as_int = pd.merge(edge_with_from_int_id,
                                  node_ids_sub,
                                  left_on='to',
                                  right_on='id',
                                  sort=False,
                                  copy=False,
                                  how='left')

    edge_to_col_rename = {'id_int': 'to_int'}
    from_and_to_as_int.rename(columns=edge_to_col_rename, inplace=True)
    from_and_to_as_int.drop(['id'], axis=1, inplace=True)

    # convert all object type lists to string type
    incl_types = ['object']
    col_list = from_and_to_as_int.select_dtypes(include=incl_types).columns
    for col in col_list:
        from_and_to_as_int[col] = from_and_to_as_int[col].astype(str)

    # final clean up steps for node_df, now that is done being used
    # to update edge_df; by first making the integer version of the id
    # set as the pandas index...
    node_df.set_index('id_int', drop=True, inplace=True)

    # ...and then to ensure that this column remains cast
    # as a string (whereas int column is now type integer)
    node_df['id'] = node_df['id'].astype(str)

    # TODO: later, when we implement nearest neighbor measures
    #       in the GTFS step, we may want to ensure that the
    #       'nearest_osm_node' column is removed from node_df
    return {'edges': from_and_to_as_int, 'nodes': node_df}
