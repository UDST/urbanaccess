import pytest
import os
import pandas as pd
import yaml
import zipfile
from six.moves.urllib import request

from urbanaccess import gtfsfeeds
from urbanaccess.gtfsfeeds import feeds
from urbanaccess import config


@pytest.fixture
def dir_w_no_zips(tmpdir):
    # make dir with no zip in it
    dir_path = os.path.join(tmpdir.strpath, 'test')
    os.makedirs(dir_path)
    return dir_path


@pytest.fixture
def gtfs_feed_zipfile(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1,
        calendar_dates_feed_1):
    agency_name = 'agency_a'
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1,
                      'calendar_dates': calendar_dates_feed_1}
    feed_path = os.path.join(tmpdir.strpath, agency_name)
    os.makedirs(feed_path)
    print('writing test data to dir: {}'.format(feed_path))
    filelist = []
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        filelist.extend([feed_file_name])
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    os.makedirs(os.path.join(feed_path, 'gtfsfeed_zips'))
    zip_path = os.path.join(feed_path, 'gtfsfeed_zips', agency_name + '.zip')
    filelist_abs_path = [os.path.join(
        os.path.abspath(feed_path), item) for item in filelist]
    with zipfile.ZipFile(zip_path, 'w') as z:
        for file in filelist_abs_path:
            print('writing file: {} to zip: {}'.format(file, zip_path))
            z.write(file, os.path.basename(file),
                    compress_type=zipfile.ZIP_DEFLATED)
            # remove intermediate data
            os.remove(file)

    return zip_path


@pytest.fixture
def gtfs_feed_zipfile_w_subzips(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1,
        calendar_dates_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1,
                      'calendar_dates': calendar_dates_feed_1}

    feed_path = os.path.join(tmpdir.strpath)
    os.makedirs(os.path.join(feed_path, 'gtfsfeed_zips'))
    zipfile_list = []
    for agency_name in ['agency_a', 'agency_b']:
        print('writing test data to dir: {}'.format(feed_path))
        filelist = []
        for feed_file, feed_df in feed_file_dict.items():
            feed_file_name = '{}.txt'.format(feed_file)
            filelist.extend([feed_file_name])
            feed_df.to_csv(os.path.join(feed_path, feed_file_name),
                           index=False)

        zip_path = os.path.join(
            feed_path, 'gtfsfeed_zips', agency_name + '.zip')
        filelist_abs_path = [os.path.join(
            os.path.abspath(feed_path), item) for item in filelist]
        with zipfile.ZipFile(zip_path, 'w') as z:
            for file in filelist_abs_path:
                print('writing file: {} to zip: {}'.format(file, zip_path))
                z.write(file, os.path.basename(file),
                        compress_type=zipfile.ZIP_DEFLATED)
                # remove intermediate data
                os.remove(file)

        zipfile_list.extend([agency_name + '.zip'])

    filelist_abs_path = [os.path.join(
        os.path.dirname(zip_path), item) for item in zipfile_list]
    parent_zip_path = os.path.join(
        feed_path, 'gtfsfeed_zips', 'zip_w_subzips.zip')
    with zipfile.ZipFile(parent_zip_path, 'w') as z:
        for file in filelist_abs_path:
            print('writing file: {} to zip: {}'.format(file, parent_zip_path))
            z.write(file, os.path.basename(file),
                    compress_type=zipfile.ZIP_DEFLATED)
            os.remove(file)

    return parent_zip_path


@pytest.fixture
def gtfs_feed_zipfile_w_subzips_and_txt(
        tmpdir,
        agency_feed_1, stop_times_feed_1, stops_feed_1,
        routes_feed_1, trips_feed_1, calendar_feed_1,
        calendar_dates_feed_1):
    feed_file_dict = {'agency': agency_feed_1,
                      'stop_times': stop_times_feed_1,
                      'stops': stops_feed_1,
                      'routes': routes_feed_1,
                      'trips': trips_feed_1,
                      'calendar': calendar_feed_1,
                      'calendar_dates': calendar_dates_feed_1}

    feed_path = os.path.join(tmpdir.strpath)
    os.makedirs(os.path.join(feed_path, 'gtfsfeed_zips'))
    agency_name = 'agency_a'
    print('writing test data to dir: {}'.format(feed_path))
    filelist = []
    for feed_file, feed_df in feed_file_dict.items():
        feed_file_name = '{}.txt'.format(feed_file)
        filelist.extend([feed_file_name])
        feed_df.to_csv(os.path.join(feed_path, feed_file_name), index=False)

    zip_path = os.path.join(feed_path, agency_name + '.zip')
    filelist_abs_path = [os.path.join(
        os.path.abspath(feed_path), item) for item in filelist]
    with zipfile.ZipFile(zip_path, 'w') as z:
        for file in filelist_abs_path:
            print('writing file: {} to zip: {}'.format(file, zip_path))
            z.write(file, os.path.basename(file),
                    compress_type=zipfile.ZIP_DEFLATED)

    filelist.extend([agency_name + '.zip'])

    filelist_abs_path = [os.path.join(feed_path, item) for item in filelist]
    parent_zip_path = os.path.join(
        feed_path, 'gtfsfeed_zips', 'zip_w_subzip_and_txt.zip')
    with zipfile.ZipFile(parent_zip_path, 'w') as z:
        for file in filelist_abs_path:
            print('writing file: {} to zip: {}'.format(file, parent_zip_path))
            z.write(file, os.path.basename(file),
                    compress_type=zipfile.ZIP_DEFLATED)
            # remove intermediate data
            os.remove(file)

    return parent_zip_path


@pytest.fixture
def feed_dict1():
    return {
        'ac transit':
            'http://www.actransit.org/wp-content/uploads/GTFSJune182017B.zip'}


@pytest.fixture
def feed_object(feed_dict1):
    feeds.add_feed(add_dict=feed_dict1)
    return feeds


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
def feed_dict4():
    return {
        'septa': 'https://github.com/septadev/GTFS/releases/download/'
                 'v202106061/gtfs_public.zip'}


@pytest.fixture
def feed_dict_invalid_1():
    return {
        1: 'http://www.actransit.org/wp-content/uploads/GTFSJune182017B.zip',
        'Bay Area Rapid Transit':
            'http://www.gtfs-data-exchange.com/agency/bay-area-rapid-transit'
            '/latest.zip'}


@pytest.fixture
def feed_dict_invalid_2():
    return {
        'ac transit': 1,
        'Bay Area Rapid Transit':
            'http://www.gtfs-data-exchange.com/agency/bay-area-rapid-transit'
            '/latest.zip'}


@pytest.fixture
def feed_dict_invalid_3():
    return {
        'ac transit v2': 'http://www.actransit.org/wp-content/uploads/'
                         'GTFSJune182017B.zip'}


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


@pytest.fixture
def feed_yaml_invalid_1(tmpdir):
    yaml_dict = [{
        'gtfs_feeds': {
            'ac transit': 'http://www.actransit.org/wp-content/uploads'
                          '/GTFSJune182017B.zip',
            'Bay Area Rapid Transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip'}}]

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds_invalid_1.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)
    return tmpdir.strpath


@pytest.fixture
def feed_yaml_invalid_2(tmpdir):
    yaml_dict = {
        'test_gtfs_feeds': {
            'ac transit': 'http://www.actransit.org/wp-content/uploads'
                          '/GTFSJune182017B.zip',
            'Bay Area Rapid Transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip'}}

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds_invalid_2.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)
    return tmpdir.strpath


@pytest.fixture
def feed_yaml_invalid_3(tmpdir):
    yaml_dict = {
        'gtfs_feeds': {
            'ac transit': 1,
            'Bay Area Rapid Transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip'}}

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds_invalid_3.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)
    return tmpdir.strpath


@pytest.fixture
def feed_yaml_invalid_4(tmpdir):
    yaml_dict = {
        'gtfs_feeds': {
            1: 'http://www.actransit.org/wp-content/uploads'
               '/GTFSJune182017B.zip',
            'Bay Area Rapid Transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip'}}

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds_invalid_4.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)
    return tmpdir.strpath


@pytest.fixture
def feed_yaml_invalid_5(tmpdir):
    yaml_dict = {
        'gtfs_feeds': {
            'ac transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip',
            'Bay Area Rapid Transit':
                'http://www.gtfs-data-exchange.com/agency/bay-area-rapid'
                '-transit/latest.zip'}}

    yaml_path = os.path.join(tmpdir.strpath, 'gtfsfeeds_invalid_5.yaml')
    with open(yaml_path, 'w') as f:
        yaml.dump(yaml_dict, f, default_flow_style=False)
    return tmpdir.strpath


def test_feed_object():
    assert isinstance(gtfsfeeds.feeds, gtfsfeeds.urbanaccess_gtfsfeeds)
    assert isinstance(feeds.to_dict(), dict)


def test_feed_to_dict(feed_object):
    assert isinstance(feed_object.to_dict(), dict)
    assert feed_object.to_dict() == {
        'gtfs_feeds': {
            'ac transit': 'http://www.actransit.org/wp-content/uploads'
                          '/GTFSJune182017B.zip'}}
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


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


def test_add_feed_invalid(feed_dict1, feed_dict3, feed_dict_invalid_1,
                          feed_dict_invalid_2, feed_dict_invalid_3):
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=['test feed'])
    expected_error = 'add_dict is not a dict'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=feed_dict1, replace=1)
    expected_error = 'replace is not bool'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=feed_dict1, replace=False)
        feeds.add_feed(add_dict=feed_dict3, replace=False)
    expected_error = ('ac transit passed in add_dict already exists in '
                      'gtfs_feeds. Only unique keys are allowed to be '
                      'added.')
    assert expected_error in str(excinfo.value)
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=feed_dict_invalid_1, replace=False)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=feed_dict_invalid_2, replace=False)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=feed_dict_invalid_1, replace=True)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.add_feed(add_dict=feed_dict_invalid_2, replace=True)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)
    with pytest.raises(ValueError) as excinfo:
        # add dict with same value twice to test adding to another dict
        feeds.add_feed(add_dict=feed_dict1, replace=False)
        feeds.add_feed(add_dict=feed_dict_invalid_3, replace=False)
    expected_error = ('duplicate values were found when the passed '
                      'add_dict dictionary was added to the existing '
                      'dictionary. Feed URL values must be unique.')
    assert expected_error in str(excinfo.value)
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


def test_remove_feed_invalid(feed_dict3):
    feeds.add_feed(add_dict=feed_dict3)
    with pytest.raises(ValueError) as excinfo:
        feeds.remove_feed(del_key='ac transit', remove_all=1)
    expected_error = 'remove_all is not bool'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.remove_feed(del_key=1, remove_all=False)
    expected_error = 'del_key must be a string or list of strings'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.remove_feed(del_key='ac transit', remove_all=True)
    expected_error = ('remove_all must be False in order to remove '
                      'individual records: ac transit')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds.remove_feed(del_key='ac transit 2', remove_all=False)
    expected_error = 'ac transit 2 key to delete was not found in gtfs_feeds'
    assert expected_error in str(excinfo.value)
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


def test_to_yaml_feed_create_dir(tmpdir, feed_dict3):
    path_to_create = os.path.join(tmpdir.strpath, 'temp')
    feeds.add_feed(add_dict=feed_dict3)
    feeds.to_yaml(path_to_create, overwrite=True)

    yaml_path = os.path.join(path_to_create, 'gtfsfeeds.yaml')
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)
    assert yaml_config['gtfs_feeds'] == feed_dict3
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_from_yaml_feed(feed_yaml):
    feeds_from_yaml = feeds.from_yaml(feed_yaml, 'gtfsfeeds.yaml')

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


def test_from_yaml_feed_invalid(
        feed_yaml, feed_yaml_invalid_1, feed_yaml_invalid_2,
        feed_yaml_invalid_3, feed_yaml_invalid_4, feed_yaml_invalid_5):
    with pytest.raises(ValueError) as excinfo:
        feeds_from_yaml = feeds.from_yaml(
            gtfsfeeddir=feed_yaml_invalid_2,
            yamlname='gtfsfeeds_invalid_2.yaml')
    expected_error = 'key gtfs_feeds was not found in YAML file'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds_from_yaml = feeds.from_yaml(
            gtfsfeeddir=feed_yaml_invalid_3,
            yamlname='gtfsfeeds_invalid_3.yaml')
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds_from_yaml = feeds.from_yaml(
            gtfsfeeddir=feed_yaml_invalid_4,
            yamlname='gtfsfeeds_invalid_4.yaml')
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        feeds_from_yaml = feeds.from_yaml(
            gtfsfeeddir=feed_yaml_invalid_5,
            yamlname='gtfsfeeds_invalid_5.yaml')
    expected_error = (
        'duplicate values were found in YAML file: {}. Feed URL values must '
        'be unique.'.format('gtfsfeeds_invalid_5.yaml'))
    assert expected_error in str(excinfo.value)


def test_search_all_gtfs_data_exchange():
    search_result = gtfsfeeds.search(
        api='gtfsdataexch')

    assert isinstance(search_result, pd.DataFrame)
    assert search_result.empty is False
    # expect all records to be returned
    assert len(search_result) == 1000

    col_list = ['dataexchange_url', 'dataexchange_id', 'name']
    for col in col_list:
        assert col in search_result.columns
        assert search_result[col].isnull().all() == False  # noqa


def test_search_contains_gtfs_data_exchange():
    search_result = gtfsfeeds.search(
        api='gtfsdataexch',
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
    gtfsfeeds.search(
        api='gtfsdataexch',
        search_text='Bay Area Rapid Transit',
        search_field=None, match='exact',
        add_feed=True, overwrite_feed=True)

    assert len(feeds.gtfs_feeds.keys()) == 1
    assert 'Bay Area Rapid Transit' in feeds.gtfs_feeds.keys()
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_search_exact_search_field_gtfs_data_exchange():
    # test search field
    search_result = gtfsfeeds.search(
        api='gtfsdataexch',
        search_text='San Francisco Bay Area',
        search_field=['area'], match='exact',
        add_feed=False, overwrite_feed=False)
    assert len(search_result) == 8


def test_search_invalid():
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api=1,
            search_text=['ac transit', 'santa rosa'],
            search_field=None, match='contains',
            add_feed=False, overwrite_feed=False)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api='test api',
            search_text=['ac transit', 'santa rosa'],
            search_field=None, match='contains',
            add_feed=False, overwrite_feed=False)
    expected_error = 'test api is not currently a supported API'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        # set config url to None to throw error
        config.settings.gtfs_api = {'gtfsdataexch': None}
        search_result = gtfsfeeds.search(
            api='gtfsdataexch',
            search_text=['ac transit', 'santa rosa'],
            search_field=None, match='contains',
            add_feed=False, overwrite_feed=False)
    expected_error = ('gtfsdataexch API is not defined or is defined '
                      'incorrectly')
    assert expected_error in str(excinfo.value)
    # reset config url
    config.settings.gtfs_api = {
        'gtfsdataexch':
            'http://www.gtfs-data-exchange.com/api/agencies?format=csv'}
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api='gtfsdataexch',
            search_text=['ac transit', 'santa rosa'],
            search_field=None, match='contains test',
            add_feed=False, overwrite_feed=False)
    expected_error = 'match must be either: contains or exact'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api='gtfsdataexch',
            search_text=['ac transit', 'santa rosa'],
            search_field=None, match='contains',
            add_feed=1, overwrite_feed=False)
    expected_error = 'add_feed must be bool'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api='gtfsdataexch',
            search_text=['ac transit', 'santa rosa'],
            search_field='test field', match='contains',
            add_feed=False, overwrite_feed=False)
    expected_error = 'search_field is not list'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api='gtfsdataexch',
            search_text=['ac transit', 'santa rosa'],
            search_field=['test field'], match='contains',
            add_feed=False, overwrite_feed=False)
    expected_error = 'test field column not found in available feed table'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        search_result = gtfsfeeds.search(
            api='gtfsdataexch',
            search_text=1,
            search_field=['name'], match='contains',
            add_feed=False, overwrite_feed=False)
    expected_error = 'search_text is not list'
    assert expected_error in str(excinfo.value)


def test_download_gtfs_feed_via_feed_object(feed_dict3, tmpdir):
    feeds.add_feed(add_dict=feed_dict3)
    # Note: ac transit url used here is an old link whose zipfile contains
    # a sub directory zipfile that is an edge case in the zipfile structure
    # expected however this edge case is handled by the download function
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


def test_download_gtfs_feed_w_subzips(feed_dict4, tmpdir):
    feeds.add_feed(add_dict=feed_dict4)
    # Note: septa url used here contains a zipfile with 2 sub directory
    # zipfiles
    tmp_path = tmpdir.strpath
    gtfsfeeds.download(data_folder=tmp_path)

    zipfilelist = ['septa.zip']
    filelist = ['septa_google_bus', 'septa_google_rail']
    txtlist = ['calendar.txt', 'routes.txt', 'stop_times.txt',
               'stops.txt', 'trips.txt']
    zip_path = os.path.join(tmp_path, 'gtfsfeed_zips')
    txt_path = os.path.join(tmp_path, 'gtfsfeed_text')
    for zipfile in zipfilelist:
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


def test_download_invalid(tmpdir):
    tmp_path = tmpdir.strpath
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name='test_agency',
            feed_url=None,
            feed_dict=None,
            error_pause_duration=5, delete_zips=False)
    expected_error = 'Both feed_name and feed_url parameters are required.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name='test_agency',
            feed_url=('http://www.gtfs-data-exchange.com/'
                      'agency/bay-area-rapid-transit/latest.zip'),
            feed_dict={
                'test_agency_dict': 'http://www.gtfs-data-exchange.com/agency/'
                                    'ac-transit/latest.zip'},
            error_pause_duration=5, delete_zips=False)
    expected_error = ('only feed_dict or feed_name and '
                      'feed_url can be used at once. Both cannot be used.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name=1,
            feed_url=('http://www.gtfs-data-exchange.com/'
                      'agency/bay-area-rapid-transit/latest.zip'),
            feed_dict=None,
            error_pause_duration=5, delete_zips=False)
    expected_error = 'either feed_name and or feed_url are not string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name=None,
            feed_url=None,
            feed_dict='test_agency_dict',
            error_pause_duration=5, delete_zips=False)
    expected_error = 'feed_dict is not dict'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name=None,
            feed_url=None,
            feed_dict={1: 'http://www.gtfs-data-exchange.com/agency/'
                          'ac-transit/latest.zip'},
            error_pause_duration=5, delete_zips=False)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name=None,
            feed_url=None,
            feed_dict={'test_agency_dict': 1},
            error_pause_duration=5, delete_zips=False)
    expected_error = '1 must be a string'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name=None,
            feed_url=None,
            feed_dict=None,
            error_pause_duration=5, delete_zips=False)
    expected_error = 'No records were found in passed feed_dict'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.download(
            data_folder=tmp_path,
            feed_name=None,
            feed_url=None,
            feed_dict={
                'test_agency_dict':
                    'http://www.gtfs-data-exchange.com/agency/'
                    'ac-transit/latest.zip',
                'test_agency_dict 2':
                    'http://www.gtfs-data-exchange.com/agency/'
                    'ac-transit/latest.zip'},
            error_pause_duration=5, delete_zips=False)
    expected_error = ('duplicate values were found in feed_dict. Feed '
                      'URL values must be unique.')
    assert expected_error in str(excinfo.value)
    # clear feeds from global memory
    feeds.remove_feed(remove_all=True)


def test_zipfile_type_check_valid(feed_dict1):
    feed_url_value = feed_dict1['ac transit']
    file = request.urlopen(feed_url_value)
    gtfsfeeds._zipfile_type_check(file, feed_url_value)


def test_unzip_util(gtfs_feed_zipfile):
    unzip_dir = os.path.join(os.path.dirname(gtfs_feed_zipfile).replace(
        'gtfsfeed_zips', 'gtfsfeed_text'))
    sub_zip_filelist = gtfsfeeds._unzip_util(
        zipfile_read_path=gtfs_feed_zipfile,
        unzip_file_path=unzip_dir, has_subzips=False)

    assert isinstance(sub_zip_filelist, list)
    assert sub_zip_filelist == []

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    # test that expected files have been extracted
    assert os.path.exists(unzip_dir) is True
    for txt in txtlist:
        assert os.path.exists(os.path.join(unzip_dir, txt)) is True


def test_unzip_util_w_subzips_w_False_w_subzips(gtfs_feed_zipfile_w_subzips):
    zip_root_dir = os.path.dirname(gtfs_feed_zipfile_w_subzips)
    unzip_dir = os.path.join(zip_root_dir.replace(
        'gtfsfeed_zips', 'gtfsfeed_text'))
    sub_zip_filelist = gtfsfeeds._unzip_util(
        zipfile_read_path=gtfs_feed_zipfile_w_subzips,
        unzip_file_path=unzip_dir, has_subzips=False)

    assert isinstance(sub_zip_filelist, list)
    # returns all the zips that exist in the subdirectory that need to be
    # extracted
    expected_sub_zip_filelist = ['agency_a.zip', 'agency_b.zip']
    assert sub_zip_filelist == expected_sub_zip_filelist

    for zipfile in expected_sub_zip_filelist:
        assert os.path.exists(os.path.join(unzip_dir, zipfile)) is True


def test_unzip_util_w_subzips_w_True_w_subzips(gtfs_feed_zipfile_w_subzips):
    zip_root_dir = os.path.dirname(gtfs_feed_zipfile_w_subzips)
    unzip_dir = os.path.join(zip_root_dir.replace(
        'gtfsfeed_zips', 'gtfsfeed_text'))
    sub_zip_filelist = gtfsfeeds._unzip_util(
        zipfile_read_path=gtfs_feed_zipfile_w_subzips,
        unzip_file_path=unzip_dir, has_subzips=True)

    # expect it to extract all the zips so we expect no more zips to exist
    # in the subdirectory and return nothing
    assert sub_zip_filelist is None


def test_unzip_util_w_subzips_w_True_wo_subzips(gtfs_feed_zipfile):
    zip_root_dir = os.path.dirname(gtfs_feed_zipfile)
    unzip_dir = os.path.join(zip_root_dir.replace(
        'gtfsfeed_zips', 'gtfsfeed_text'))
    sub_zip_filelist = gtfsfeeds._unzip_util(
        zipfile_read_path=gtfs_feed_zipfile,
        unzip_file_path=unzip_dir, has_subzips=True)

    # zip had no zips in its subdirectory so we dont expect any in list
    # and instead expect nothing
    assert sub_zip_filelist is None

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']

    # test that all expected extracted data exists
    assert os.path.exists(unzip_dir) is True
    # test that subzip does not exist in dir
    assert os.path.exists(os.path.join(
        unzip_dir, 'agency_a.zip')) is False
    for txt in txtlist:
        check_path = os.path.join(unzip_dir, txt)
        assert os.path.exists(check_path) is True


def test_unzip_util_w_subzips_w_True_w_subzips_and_txt(
        gtfs_feed_zipfile_w_subzips_and_txt):
    unzip_rootpath = os.path.join(
        os.path.dirname(gtfs_feed_zipfile_w_subzips_and_txt).replace(
            'gtfsfeed_zips', 'gtfsfeed_text'), 'agency_a')
    unzip_dir = os.path.dirname(gtfs_feed_zipfile_w_subzips_and_txt)
    sub_zip_filelist = gtfsfeeds._unzip_util(
        zipfile_read_path=gtfs_feed_zipfile_w_subzips_and_txt,
        unzip_file_path=unzip_rootpath, has_subzips=False)

    assert isinstance(sub_zip_filelist, list)
    # zip had no zips in its subdirectory so we dont expect any in list
    assert sub_zip_filelist == []

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    txt_path = os.path.join(
        unzip_dir.replace('gtfsfeed_zips', 'gtfsfeed_text'), 'agency_a')
    # test that the subzip is not in the unzip directory
    subzip_path = os.path.join(txt_path, 'agency_a.zip')
    assert os.path.exists(subzip_path) is False
    assert os.path.exists(txt_path) is True
    for txt in txtlist:
        check_path = os.path.join(txt_path, txt)
        assert os.path.exists(check_path) is True


def test_unzip_w_delete_False(gtfs_feed_zipfile):
    zip_rootpath = os.path.dirname(gtfs_feed_zipfile)
    gtfsfeeds.unzip(zip_rootpath=zip_rootpath, delete_zips=False)

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    folder_name = os.path.basename(gtfs_feed_zipfile).replace('.zip', '')
    txt_path = os.path.join(zip_rootpath.replace(
        'gtfsfeed_zips', 'gtfsfeed_text'), folder_name)

    # test that directory and files have not been removed
    assert os.path.exists(zip_rootpath) is True
    assert os.path.exists(gtfs_feed_zipfile) is True

    # test that all expected extracted data exists
    assert os.path.exists(txt_path) is True
    for txt in txtlist:
        check_path = os.path.join(txt_path, txt)
        assert os.path.exists(check_path) is True


def test_unzip_w_delete_true(gtfs_feed_zipfile):
    zip_rootpath = os.path.dirname(gtfs_feed_zipfile)
    gtfsfeeds.unzip(zip_rootpath=zip_rootpath, delete_zips=True)

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    folder_name = os.path.basename(gtfs_feed_zipfile).replace('.zip', '')
    txt_path = os.path.join(zip_rootpath.replace(
        'gtfsfeed_zips', 'gtfsfeed_text'), folder_name)

    # test that directory and files have been removed
    assert os.path.exists(zip_rootpath) is False
    assert os.path.exists(gtfs_feed_zipfile) is False

    # test that all expected extracted data exists
    assert os.path.exists(txt_path) is True
    for txt in txtlist:
        check_path = os.path.join(txt_path, txt)
        assert os.path.exists(check_path) is True


def test_unzip_w_subzips(gtfs_feed_zipfile_w_subzips):
    zip_rootpath = os.path.dirname(gtfs_feed_zipfile_w_subzips)
    gtfsfeeds.unzip(zip_rootpath=zip_rootpath, delete_zips=True)

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    txt_path = zip_rootpath.replace('gtfsfeed_zips', 'gtfsfeed_text')
    expected_sub_dir = ['zip_w_subzips_agency_a', 'zip_w_subzips_agency_b']

    # test that all expected extracted data exists
    for sub_dir in expected_sub_dir:
        assert os.path.exists(os.path.join(txt_path, sub_dir)) is True
        for txt in txtlist:
            check_path = os.path.join(txt_path, sub_dir, txt)
            assert os.path.exists(check_path) is True


def test_unzip_w_subzips_and_txt(gtfs_feed_zipfile_w_subzips_and_txt):
    zip_rootpath = os.path.dirname(gtfs_feed_zipfile_w_subzips_and_txt)
    gtfsfeeds.unzip(zip_rootpath=zip_rootpath, delete_zips=True)

    txtlist = ['agency.txt', 'calendar.txt', 'calendar_dates.txt',
               'routes.txt', 'stop_times.txt', 'stops.txt', 'trips.txt']
    txt_path = zip_rootpath.replace('gtfsfeed_zips', 'gtfsfeed_text')

    # test that all expected extracted data exists
    sub_dir = 'zip_w_subzip_and_txt'
    assert os.path.exists(os.path.join(txt_path, sub_dir)) is True
    # test that subzip does not exist in dir
    assert os.path.exists(os.path.join(
        txt_path, sub_dir, 'agency_a.zip')) is False
    for txt in txtlist:
        check_path = os.path.join(txt_path, sub_dir, txt)
        assert os.path.exists(check_path) is True


def test_unzip_error(dir_w_no_zips):
    with pytest.raises(ValueError) as excinfo:
        gtfsfeeds.unzip(zip_rootpath=dir_w_no_zips, delete_zips=False)
    expected_error = ('No zipfiles were found in specified '
                      'directory: {}'.format(dir_w_no_zips))
    assert expected_error in str(excinfo.value)
