# coding=utf-8
import pytest
import pandas as pd
import os
import six
import codecs
import sys

import urbanaccess.gtfs.load as gtfs_load
from urbanaccess.gtfs.gtfsfeeds_dataframe import urbanaccess_gtfs_df


@pytest.fixture
def expected_urbanaccess_gtfs_df_keys():
    expected_keys = ['stops', 'routes', 'trips', 'stop_times',
                     'calendar', 'calendar_dates', 'stop_times_int',
                     'headways']
    return expected_keys.sort()


@pytest.fixture
def test_txt_files(tmpdir):
    # test file that does not need to be fixed
    do_not_fix_txt = os.path.join(tmpdir.strpath, 'agency.txt')
    data = ['name,text\n', '  Circulação  , áéíóúüñ¿¡ \n']
    if six.PY2:
        with open(do_not_fix_txt, 'w') as f:
            f.writelines(data)
    else:
        with open(do_not_fix_txt, 'w', encoding='utf-8') as f:
            f.writelines(data)

    # test file that does need to be fixed
    fix_txt = os.path.join(tmpdir.strpath, 'calendar.txt')
    data = ['  name  , text \n', '  Circulação  , áéíóúüñ¿¡ \n']
    if six.PY2:
        with open(fix_txt, 'w') as f:
            f.writelines(data)
    else:
        with open(fix_txt, 'w', encoding='utf-8') as f:
            f.writelines(data)

    fix_txt_wBOM = os.path.join(tmpdir.strpath, 'calendar_dates.txt')
    if six.PY2:
        data = [codecs.BOM_UTF8,
                '  name  , text \n',
                '  Circulação  , áéíóúüñ¿¡ \n']
        with open(fix_txt_wBOM, 'w') as f:
            f.writelines(data)
    else:
        data = [str(codecs.BOM_UTF8),
                '  name  , text \n',
                '  Circulação  , áéíóúüñ¿¡ \n']
        with open(fix_txt_wBOM, 'w', encoding='utf-8') as f:
            f.writelines(data)

    return tmpdir.strpath, do_not_fix_txt, fix_txt, fix_txt_wBOM


@pytest.fixture
def test_txt_files_to_use():
    gtfsfiles_to_use = ['stops.txt', 'routes.txt', 'trips.txt',
                        'stop_times.txt', 'calendar.txt',
                        'agency.txt', 'calendar_dates.txt']
    return gtfsfiles_to_use


def test_txt_standardization(test_txt_files):
    root_dir, do_not_fix_txt, fix_txt, fix_txt_wBOM = test_txt_files

    gtfs_load._standardize_txt(csv_rootpath=root_dir)

    df = pd.read_csv(fix_txt)
    assert list(df.columns) == list(df.columns.str.strip())

    df = pd.read_csv(fix_txt_wBOM)
    assert list(df.columns) == list(df.columns.str.strip())


def test_txt_header_whitespace_check(test_txt_files, test_txt_files_to_use):
    root_dir, do_not_fix_txt, fix_txt, fix_txt_wBOM = test_txt_files

    gtfs_load._txt_header_whitespace_check(
        gtfsfiles_to_use=test_txt_files_to_use,
        csv_rootpath=root_dir)

    # only check 'fix_txt' as 'fix_txt_wBOM' would need to be
    # fixed by _txt_encoder_check first
    df = pd.read_csv(fix_txt)
    assert list(df.columns) == list(df.columns.str.strip())


@pytest.mark.skipif(
    sys.version_info >= (3, 0), reason="requires python < 3.0")
def test_txt_encoder_check(test_txt_files, test_txt_files_to_use):
    root_dir, do_not_fix_txt, fix_txt, fix_txt_wBOM = test_txt_files

    gtfs_load._txt_encoder_check(
        gtfsfiles_to_use=test_txt_files_to_use,
        csv_rootpath=root_dir)

    with open(fix_txt_wBOM, 'r') as f:
        raw = f.read()
    assert raw.startswith(codecs.BOM_UTF8) is False


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
