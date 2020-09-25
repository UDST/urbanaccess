import pytest
import pandas as pd

import urbanaccess.gtfs.load as gtfs_load
from urbanaccess.gtfs.gtfsfeeds_dataframe import urbanaccess_gtfs_df


@pytest.fixture
def expected_urbanaccess_gtfs_df_keys():
    expected_keys = ['stops', 'routes', 'trips', 'stop_times',
                     'calendar', 'calendar_dates', 'stop_times_int',
                     'headways']
    return expected_keys.sort()


def test_loadgtfsfeed_to_df_wo_calendar(
        agency_a_feed_on_disk_wo_calendar,
        expected_urbanaccess_gtfs_df_keys):
    feed_dir = agency_a_feed_on_disk_wo_calendar
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=feed_dir,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    assert isinstance(loaded_feeds, urbanaccess_gtfs_df)
    urbanaccess_gtfs_df_info = vars(loaded_feeds)
    expected_dfs = ['stops', 'routes', 'trips', 'stop_times',
                    'calendar_dates']
    assert expected_urbanaccess_gtfs_df_keys == list(
        urbanaccess_gtfs_df_info.keys()).sort()
    for key, value in urbanaccess_gtfs_df_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False


def test_loadgtfsfeed_to_df_wo_calendar_dates(
        agency_a_feed_on_disk_wo_calendar_dates,
        expected_urbanaccess_gtfs_df_keys):
    feed_dir = agency_a_feed_on_disk_wo_calendar_dates
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=feed_dir,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    assert isinstance(loaded_feeds, urbanaccess_gtfs_df)
    urbanaccess_gtfs_df_info = vars(loaded_feeds)
    expected_dfs = ['stops', 'routes', 'trips', 'stop_times',
                    'calendar']
    assert expected_urbanaccess_gtfs_df_keys == list(
        urbanaccess_gtfs_df_info.keys()).sort()
    for key, value in urbanaccess_gtfs_df_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False


def test_loadgtfsfeed_to_df_w_calendar_and_calendar_dates(
        agency_a_feed_on_disk_w_calendar_and_calendar_dates,
        expected_urbanaccess_gtfs_df_keys):
    feed_dir = agency_a_feed_on_disk_w_calendar_and_calendar_dates
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=feed_dir,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    assert isinstance(loaded_feeds, urbanaccess_gtfs_df)
    urbanaccess_gtfs_df_info = vars(loaded_feeds)
    expected_dfs = ['stops', 'routes', 'trips', 'stop_times',
                    'calendar', 'calendar_dates']
    assert expected_urbanaccess_gtfs_df_keys == list(
        urbanaccess_gtfs_df_info.keys()).sort()
    for key, value in urbanaccess_gtfs_df_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False


def test_loadgtfsfeed_to_df_wo_calendar_and_calendar_dates(
        agency_a_feed_on_disk_wo_calendar_and_calendar_dates):
    feed_dir = agency_a_feed_on_disk_wo_calendar_and_calendar_dates
    with pytest.raises(ValueError) as excinfo:
        loaded_feeds = gtfs_load.gtfsfeed_to_df(
            gtfsfeed_path=feed_dir,
            validation=False,
            verbose=True,
            bbox=None,
            remove_stops_outsidebbox=False,
            append_definitions=False)
    expected_error = (
        "at least one of `calendar.txt` or `calendar_dates.txt` is required "
        "to complete a GTFS dataset but neither was found in folder")
    assert expected_error in str(excinfo.value)


def test_loadgtfsfeed_to_df_wo_req_file(
        agency_a_feed_on_disk_wo_req_file):
    feed_dir = agency_a_feed_on_disk_wo_req_file
    with pytest.raises(ValueError) as excinfo:
        loaded_feeds = gtfs_load.gtfsfeed_to_df(
            gtfsfeed_path=feed_dir,
            validation=False,
            verbose=True,
            bbox=None,
            remove_stops_outsidebbox=False,
            append_definitions=False)
    expected_error = (
        "trips.txt is a required GTFS text file and was not found in folder")
    assert expected_error in str(excinfo.value)


def test_loadgtfsfeed_to_df_wo_agency(
        agency_a_feed_on_disk_wo_agency,
        expected_urbanaccess_gtfs_df_keys):
    feed_dir = agency_a_feed_on_disk_wo_agency
    loaded_feeds = gtfs_load.gtfsfeed_to_df(
        gtfsfeed_path=feed_dir,
        validation=False,
        verbose=True,
        bbox=None,
        remove_stops_outsidebbox=False,
        append_definitions=False)
    assert isinstance(loaded_feeds, urbanaccess_gtfs_df)
    urbanaccess_gtfs_df_info = vars(loaded_feeds)
    expected_dfs = ['stops', 'routes', 'trips', 'stop_times',
                    'calendar']
    assert expected_urbanaccess_gtfs_df_keys == list(
        urbanaccess_gtfs_df_info.keys()).sort()
    for key, value in urbanaccess_gtfs_df_info.items():
        assert isinstance(value, pd.core.frame.DataFrame)
        # check that df is not empty
        if key in expected_dfs:
            assert value.empty is False
