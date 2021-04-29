import pytest
import os
import pandas as pd
import yaml

from urbanaccess import gtfsfeeds
from urbanaccess.gtfsfeeds import feeds


@pytest.fixture
def feed_dict1():
    return {
        'ac transit':
            'http://www.actransit.org/wp-content/uploads/GTFSJune182017B.zip'}


@pytest.fixture
def feed_dict2():
    return {
        'Bay Area Rapid Transit':
            'http://www.gtfs-data-exchange.com/agency/bay-area-rapid-transit'
            '/latest.zip'}


@pytest.fixture
def feed_dict3():
    return {
        'ac transit': 'http://www.actransit.org/wp-content/uploads'
                      '/GTFSJune182017B.zip',
        'Bay Area Rapid Transit':
            'http://www.gtfs-data-exchange.com/agency/bay-area-rapid-transit'
            '/latest.zip'}


@pytest.fixture
def feed_yaml(tmpdir):
    yaml_dict = {
        'gtfs_feeds': {
            'ac transit': 'http://www.actransit.org/wp-content/uploads'
                          '/GTFSJune182017B.zip',
            'Bay Area Rapid Transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip'}}

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)
    return tmpdir.strpath


def test_feed_object():
    assert isinstance(gtfsfeeds.feeds, gtfsfeeds.urbanaccess_gtfsfeeds)
    assert isinstance(feeds.to_dict(), dict)


def test_add_feed(feed_dict1, feed_dict2):
    feeds.add_feed(add_dict=feed_dict1)
    assert len(feeds.gtfs_feeds.keys()) == 1
    feeds.add_feed(add_dict=feed_dict2)
    assert len(feeds.gtfs_feeds.keys()) == 2
    feed_dict_replace = {'Bay Area Rapid Transit': 'test'}
    feeds.add_feed(add_dict=feed_dict_replace, replace=True)

    for key, value in feeds.gtfs_feeds.items():
        if key == 'Bay Area Rapid Transit':
            assert value == 'test'
    assert isinstance(feeds, gtfsfeeds.urbanaccess_gtfsfeeds)
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_remove_feed(feed_dict3):
    feeds.add_feed(add_dict=feed_dict3)
    feeds.remove_feed(del_key='ac transit')
    assert len(feeds.gtfs_feeds.keys()) == 1
    assert 'ac transit' not in feeds.gtfs_feeds.keys()
    feeds.remove_feed(remove_all=True)
    assert len(feeds.gtfs_feeds.keys()) == 0
    assert isinstance(feeds, gtfsfeeds.urbanaccess_gtfsfeeds)
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_to_yaml_feed(tmpdir, feed_dict3):
    feeds.add_feed(add_dict=feed_dict3)
    feeds.to_yaml(tmpdir.strpath, overwrite=True)

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds.yaml')
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)
    assert yaml_config['gtfs_feeds'] == feed_dict3
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_from_yaml_feed(feed_yaml):
    yaml_path = feed_yaml
    feeds_from_yaml = feeds.from_yaml(yaml_path, 'gtfsfeeds.yaml')

    assert isinstance(feeds_from_yaml, gtfsfeeds.urbanaccess_gtfsfeeds)
    assert len(feeds_from_yaml.gtfs_feeds.keys()) == 2

    valid_feed = ('http://www.gtfs-data-exchange.com/'
                  'agency/bay-area-rapid-transit/latest.zip')
    assert feeds_from_yaml.gtfs_feeds['Bay Area Rapid Transit'] == valid_feed

    valid_feed = ('http://www.actransit.org/wp-content/'
                  'uploads/GTFSJune182017B.zip')
    assert feeds_from_yaml.gtfs_feeds['ac transit'] == valid_feed
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_search_contains_gtfs_data_exchange():
    search_result = gtfsfeeds.search(api='gtfsdataexch',
                                     search_text=['ac transit', 'santa rosa'],
                                     search_field=None, match='contains',
                                     add_feed=False, overwrite_feed=False)

    assert isinstance(search_result, pd.DataFrame)
    assert search_result.empty is False
    assert len(search_result) == 2

    col_list = ['dataexchange_url', 'dataexchange_id', 'name']
    for col in col_list:
        assert col in search_result.columns
        assert search_result[col].isnull().all() == False  # noqa

    value_list = ['ac-transit', 'santa-rosa-citybus']
    for value in value_list:
        assert value in list(search_result['dataexchange_id'])


def test_search_contains_add_feed_gtfs_data_exchange():
    gtfsfeeds.search(api='gtfsdataexch',
                     search_text='ac transit',
                     search_field=None, match='contains',
                     add_feed=True, overwrite_feed=False)

    assert len(feeds.gtfs_feeds.keys()) == 1
    assert 'AC Transit' in feeds.gtfs_feeds.keys()

    # test overwrite feed
    gtfsfeeds.search(api='gtfsdataexch',
                     search_text='Bay Area Rapid Transit',
                     search_field=None, match='exact',
                     add_feed=True, overwrite_feed=True)

    assert len(feeds.gtfs_feeds.keys()) == 1
    assert 'Bay Area Rapid Transit' in feeds.gtfs_feeds.keys()
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_search_exact_search_field_gtfs_data_exchange():
    # test search field
    search_result = gtfsfeeds.search(api='gtfsdataexch',
                                     search_text='San Francisco Bay Area',
                                     search_field=['area'], match='exact',
                                     add_feed=False, overwrite_feed=False)
    assert len(search_result) == 8


def test_download_gtfs_feed_via_feed_object(feed_dict3, tmpdir):
    feeds.add_feed(add_dict=feed_dict3)
    tmp_path = tmpdir.strpath
    gtfsfeeds.download(data_folder=tmp_path)

    filelist = ['ac transit.zip', 'Bay Area Rapid Transit.zip']
    txtlist = ['calendar.txt', 'routes.txt', 'stop_times.txt',
               'stops.txt', 'trips.txt']
    zip_path = os.path.join(tmp_path, 'gtfsfeed_zips')
    txt_path = os.path.join(tmp_path, 'gtfsfeed_text')
    for zipfile in filelist:
        assert os.path.exists(os.path.join(zip_path, zipfile)) is True
    for folder in filelist:
        check_path = os.path.join(txt_path, folder.replace('.zip', ''))
        assert os.path.exists(check_path) is True
        for txt in txtlist:
            check_path = os.path.join(
                txt_path, folder.replace('.zip', ''), txt)
            assert os.path.exists(check_path) is True
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_download_gtfs_feed_via_feed_name_and_dict(tmpdir):
    tmp_path = tmpdir.strpath
    gtfsfeeds.download(
        data_folder=tmp_path,
        feed_name='test_agency',
        feed_url=('http://www.gtfs-data-exchange.com/'
                  'agency/bay-area-rapid-transit/latest.zip'),
        feed_dict=None,
        error_pause_duration=5, delete_zips=False)

    gtfsfeeds.download(
        data_folder=tmp_path,
        feed_dict={
            'test_agency_dict': 'http://www.gtfs-data-exchange.com/agency/'
                                'ac-transit/latest.zip'},
        error_pause_duration=5, delete_zips=False)

    filelist = ['test_agency.zip', 'test_agency_dict.zip']
    txtlist = ['calendar.txt', 'routes.txt', 'stop_times.txt',
               'stops.txt', 'trips.txt']
    zip_path = os.path.join(tmp_path, 'gtfsfeed_zips')
    txt_path = os.path.join(tmp_path, 'gtfsfeed_text')
    for zipfile in filelist:
        assert os.path.exists(os.path.join(zip_path, zipfile)) is True
    for folder in filelist:
        check_path = os.path.join(txt_path, folder.replace('.zip', ''))
        assert os.path.exists(check_path) is True
        for txt in txtlist:
            check_path = os.path.join(
                txt_path, folder.replace('.zip', ''), txt)
            assert os.path.exists(check_path) is True
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)
