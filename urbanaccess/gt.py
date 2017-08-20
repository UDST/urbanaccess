import os
import re
import requests
import urbanaccess
import urllib


# base url for transit.land api queries
TL_API_URL = 'http://transit.land/api/v1/'


def generate_gtfs_object(bbox, radius=100,
                         prevent_super_regional_transit=True):
    lon, lat = _get_bbox_midpoint(bbox)
    clean_bbox = ','.join([str(ea) for ea in list(bbox)])
    req_data = {'bbox': clean_bbox}

    # this asks if we should include operators not tethered
    # to a given metro area (basically filters out Amtrak and the like)
    valid_operators = None
    if prevent_super_regional_transit:
        valid_operators = _get_valid_metro_operators(req_data)
    
    # query the feeds endpoint for all feeds in bbox
    all_feeds = _get_feeds(req_data)

    # filter out operators for valid ones only if desired
    if valid_operators:
        valid_feeds = []
        for feed in all_feeds:
            keep_feed = False
            for op in feed['operators_in_feed']:
                # if at least one of the operators in this feed
                # is in our list of valid operators, we need 
                # to keep the feed
                if op['operator_onestop_id'] in valid_operators:
                    keep_feed = True
            if keep_feed:
                valid_feeds.append(feed)
        all_feeds = valid_feeds

    # some feeds may be dropped if no valid download location can
    # be determined from the transit.land api
    valid_zip_locs = _get_names_and_zip_locations(all_feeds)
    _save_to_local(valid_zip_locs, './tmp')


def _get_bbox_midpoint(bbox):
    # get the middle/center point
    middle_lon_diff = bbox[0] - bbox[2]
    middle_lat_diff = bbox[1] - bbox[3]

    middle_lon = bbox[0] - middle_lon_diff
    middle_lat = bbox[1] - middle_lat_diff

    return (middle_lon, middle_lat)


def _get_valid_metro_operators(req_data):
    op_query_base = TL_API_URL + 'operators'
    resp = requests.get(op_query_base, data=req_data)
    resp_json = resp.json()

    ops_with_metro = []
    for op in resp_json['operators']:
        if op['metro']:
            ops_with_metro.append(op['onestop_id'])

    return ops_with_metro


def _get_feeds(req_data):
    op_query_base = TL_API_URL + 'feeds'
    resp = requests.get(op_query_base, data=req_data)

    return resp.json()['feeds']


def _get_names_and_zip_locations(all_feeds):
    zip_locs = []
    for feed in all_feeds:
        tl_id = feed['onestop_id']
        new_feed_meta = {
            'name': _create_clean_name(tl_id),
            'url': feed.get('url', None)
        }

        # check if we can update the url with one
        # that is from transit.land's S3 bucket which
        # we presume will be more reliable
        if not feed['license_redistribute'] == 'no':
            feeds_url = feed['feed_versions_url']
            active_feed = feed['active_feed_version']
            tl_url = _get_tl_gtfs_zip_loc(feeds_url, active_feed)

            # handle a situation where a None is returned
            # and prevent it from overriding the other feed url
            if tl_url:
                new_feed_meta['url'] = tl_url

        # only add to the zip_locs if there is a valid url
        if new_feed_meta['url']:
            zip_locs.append(new_feed_meta)

    return zip_locs


def _create_clean_name(id):
    name_portion = id.split('-')[2]
    regex = re.compile('[^a-zA-Z]')
    clean_name = regex.sub('', name_portion)

    return clean_name


def _get_tl_gtfs_zip_loc(feeds_url, active_feed):
    resp = requests.get(feeds_url)
    resp_json = resp.json()

    zip_url = None
    for ver in resp_json['feed_versions']:
        if ver['sha1'] == active_feed:
            zip_url = ver.get('download_url', None)
            break

    return zip_url


def _save_to_local(zip_locs, folder):
    # make sure the folder exists already
    os.makedirs(folder, exist_ok=True)

    for zip_meta in zip_locs:
        url = zip_meta['url']
        name = zip_meta['name']
        filepath = ''.join([folder, '/', name, '.zip'])
        urllib.request.urlretrieve(url, filepath)


# ---------------------
# Example of execution
# ---------------------

# custom bounding box is placed here
bbox = (-122.373782,37.454539,-121.469214,37.905824)

# get gtfs for a region
generate_gtfs_object(bbox)
