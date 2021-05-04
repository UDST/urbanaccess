import pytest
import os

import urbanaccess.gtfs.utils_validation as utils_v


@pytest.fixture
def feed_dir_path(tmpdir):
    feed_path = os.path.join(tmpdir.strpath, 'agency_a')
    return feed_path


@pytest.fixture
def stops_southwest_hem(stops_feed_1):
    # modify existing SF Bay Area stop example with lat longs in Santiago
    # subset to smaller example
    df = stops_feed_1.iloc[0:5]
    df['stop_lat'] = [-33.44435765556767, -33.4478920248856,
                      -33.432943957994546, -33.41377569473884,
                      -33.40798366139315]
    df['stop_lon'] = [-70.72333759453699, -70.66684201564102,
                      -70.62667854445978, -70.58270910569762,
                      -70.54489215264125]

    return df


@pytest.fixture
def stops_northeast_hem(stops_feed_1):
    # modify existing SF Bay Area stop example with lat longs in Seoul
    # subset to smaller example
    df = stops_feed_1.iloc[0:5]
    df['stop_lat'] = [37.53929001118801, 37.52240498212254, 37.56066420699992,
                      37.59685440076574, 37.60877071348393]
    df['stop_lon'] = [126.95949033022758, 126.9736991455726,
                      127.03883355064889, 127.08516642231397,
                      127.16128002569009]
    return df


@pytest.fixture
def stops_southeast_hem(stops_feed_1):
    # modify existing SF Bay Area stop example with lat longs in Perth
    # subset to smaller example
    df = stops_feed_1.iloc[0:5]
    df['stop_lat'] = [-31.952397274688817, -31.956666273675044,
                      -32.009579275474636, -32.0471711763962,
                      -32.125143064083886]
    df['stop_lon'] = [115.85785245361353, 115.85557747913276,
                      115.85595920734899, 115.85452196050421,
                      115.8585820269115]
    return df


@pytest.fixture
def bbox_northwest_hem():
    # SF Bay Area
    bbox = (-122.2835992276, 37.7960477575, -122.2128747403, 37.8588253187)
    return bbox


@pytest.fixture
def bbox_northwest_hem_large_extent():
    # SF Bay Area
    bbox = (-122.5308774411, 37.202574249, -121.728875488, 38.0963319579)
    return bbox


@pytest.fixture
def bbox_southwest_hem():
    # Santiago
    bbox = (-70.7461832462, -33.4803792995, -70.6081674991, -33.4177877389)
    return bbox


@pytest.fixture
def bbox_northeast_hem():
    # Seoul
    bbox = (126.8521178368, 37.500812358, 127.041631997, 37.6140336447)
    return bbox


@pytest.fixture
def bbox_southeast_hem():
    # Perth
    bbox = (115.8371222619, -31.9844848755, 115.8906806115, -31.9346755739)
    return bbox


def test_validate_gtfs(stops_feed_1, feed_dir_path, bbox_northwest_hem):
    result = utils_v._validate_gtfs(stops_df=stops_feed_1,
                                    feed_folder=feed_dir_path,
                                    verbose=True, bbox=bbox_northwest_hem,
                                    remove_stops_outsidebbox=True)
    expected = stops_feed_1.loc[stops_feed_1.index.isin(
        [0, 2, 3, 4, 5])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 5
    assert result.equals(expected)


def test_checkcoordinates_northwest(
        capsys, feed_dir_path, stops_feed_1):
    utils_v._checkcoordinates(stops_feed_1, feed_dir_path)
    # check that print is in the correct hemisphere
    captured = capsys.readouterr()
    assert 'northwest' in captured.out


def test_checkcoordinates_southwest(
        capsys, feed_dir_path, stops_southwest_hem):
    utils_v._checkcoordinates(stops_southwest_hem, feed_dir_path)
    # check that print is in the correct hemisphere
    captured = capsys.readouterr()
    assert 'southwest' in captured.out


def test_checkcoordinates_northeast(
        capsys, feed_dir_path, stops_northeast_hem):
    utils_v._checkcoordinates(stops_northeast_hem, feed_dir_path)
    # check that print is in the correct hemisphere
    captured = capsys.readouterr()
    assert 'northeast' in captured.out


def test_checkcoordinates_southeast(
        capsys, feed_dir_path, stops_southeast_hem):
    utils_v._checkcoordinates(stops_southeast_hem, feed_dir_path)
    # check that print is in the correct hemisphere
    captured = capsys.readouterr()
    assert 'southeast' in captured.out


def test_boundingbox_check_invalid_params(
        feed_dir_path,
        stops_feed_1, bbox_northwest_hem):
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=None, lng_min=None,
            lat_max=None, lng_max=None,
            bbox=bbox_northwest_hem,
            remove=3,
            verbose=True)
    expected_error = "remove must be bool."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=None, lng_min=None,
            lat_max=None, lng_max=None,
            bbox=bbox_northwest_hem,
            remove=True,
            verbose=3)
    expected_error = "verbose must be bool."
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        lng_max, lat_min, lng_min, lat_max = bbox_northwest_hem
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=lat_min, lng_min=lng_min,
            lat_max=lat_max, lng_max=lng_max,
            bbox=bbox_northwest_hem,
            remove=True,
            verbose=True)
    expected_error = (
        "lat_min, lng_min, lat_max and lng_max must be None if using bbox.")
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        lng_max, lat_min, lng_min, lat_max = bbox_northwest_hem
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=None, lng_min=None,
            lat_max=None, lng_max=None,
            bbox=(lng_max, lat_min, lng_min),
            remove=True,
            verbose=True)
    expected_error = 'bbox must be a 4 element tuple.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=1, lng_min=2,
            lat_max=3, lng_max=4,
            bbox=None,
            remove=True,
            verbose=True)
    expected_error = (
        'lng_min, lat_min, lng_min, lat_max, and lng_max must be floats.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=None, lng_min=None,
            lat_max=None, lng_max=None,
            bbox=(1, 2, 3, 4),
            remove=True,
            verbose=True)
    expected_error = (
        'lng_min, lat_min, lng_min, lat_max, and lng_max must be floats.')
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=None, lng_min=4,
            lat_max=4, lng_max=4,
            bbox=None,
            remove=True,
            verbose=True)
    expected_error = 'lat_min cannot be None.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=4, lng_min=None,
            lat_max=4, lng_max=4,
            bbox=None,
            remove=True,
            verbose=True)
    expected_error = 'lng_min cannot be None.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=4, lng_min=4,
            lat_max=None, lng_max=4,
            bbox=None,
            remove=True,
            verbose=True)
    expected_error = 'lat_max cannot be None.'
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        result = utils_v._boundingbox_check(
            df=stops_feed_1,
            feed_folder=feed_dir_path,
            lat_min=4, lng_min=4,
            lat_max=4, lng_max=None,
            bbox=None,
            remove=True,
            verbose=True)
    expected_error = 'lng_max cannot be None.'
    assert expected_error in str(excinfo.value)


def test_boundingbox_check_northwest_wo_remove(
        feed_dir_path,
        stops_feed_1, bbox_northwest_hem):
    result = utils_v._boundingbox_check(
        df=stops_feed_1,
        feed_folder=feed_dir_path,
        bbox=bbox_northwest_hem,
        remove=False,
        verbose=True)
    # check that result is identical to input
    assert len(result) == 9
    assert result.equals(stops_feed_1)


def test_boundingbox_check_northwest_w_remove_large_extent(
        feed_dir_path,
        stops_feed_1, bbox_northwest_hem_large_extent):
    result = utils_v._boundingbox_check(
        df=stops_feed_1,
        feed_folder=feed_dir_path,
        bbox=bbox_northwest_hem_large_extent,
        remove=False,
        verbose=True)
    # check that result is identical to input
    assert len(result) == 9
    assert result.equals(stops_feed_1)


def test_boundingbox_check_northwest_w_remove_wo_verbose(
        feed_dir_path,
        stops_feed_1, bbox_northwest_hem):
    result = utils_v._boundingbox_check(
        df=stops_feed_1,
        feed_folder=feed_dir_path,
        bbox=bbox_northwest_hem,
        remove=True,
        verbose=False)
    expected = stops_feed_1.loc[stops_feed_1.index.isin(
        [0, 2, 3, 4, 5])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 5
    assert result.equals(expected)


def test_boundingbox_check_northwest_w_remove_wo_bbox(
        feed_dir_path,
        stops_feed_1, bbox_northwest_hem):
    lng_max, lat_min, lng_min, lat_max = bbox_northwest_hem
    result = utils_v._boundingbox_check(
        df=stops_feed_1,
        feed_folder=feed_dir_path,
        lat_min=lat_min, lng_min=lng_min,
        lat_max=lat_max, lng_max=lng_max,
        remove=True,
        verbose=True)
    expected = stops_feed_1.loc[stops_feed_1.index.isin(
        [0, 2, 3, 4, 5])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 5
    assert result.equals(expected)


def test_boundingbox_check_northwest_w_remove(
        feed_dir_path,
        stops_feed_1, bbox_northwest_hem):
    result = utils_v._boundingbox_check(
        df=stops_feed_1,
        feed_folder=feed_dir_path,
        bbox=bbox_northwest_hem,
        remove=True,
        verbose=True)
    expected = stops_feed_1.loc[stops_feed_1.index.isin(
        [0, 2, 3, 4, 5])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 5
    assert result.equals(expected)


def test_boundingbox_check_southwest_w_remove(
        feed_dir_path,
        stops_southwest_hem, bbox_southwest_hem):
    result = utils_v._boundingbox_check(
        df=stops_southwest_hem,
        feed_folder=feed_dir_path,
        bbox=bbox_southwest_hem,
        remove=True,
        verbose=True)
    expected = stops_southwest_hem.loc[stops_southwest_hem.index.isin(
        [0, 1, 2])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 3
    assert result.equals(expected)


def test_boundingbox_check_northeast_w_remove(
        feed_dir_path,
        stops_northeast_hem, bbox_northeast_hem):
    result = utils_v._boundingbox_check(
        df=stops_northeast_hem,
        feed_folder=feed_dir_path,
        bbox=bbox_northeast_hem,
        remove=True,
        verbose=True)
    expected = stops_northeast_hem.loc[stops_northeast_hem.index.isin(
        [0, 1, 2])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 3
    assert result.equals(expected)


def test_boundingbox_check_southeast_w_remove(
        feed_dir_path,
        stops_southeast_hem, bbox_southeast_hem):
    result = utils_v._boundingbox_check(
        df=stops_southeast_hem,
        feed_folder=feed_dir_path,
        bbox=bbox_southeast_hem,
        remove=True,
        verbose=True)
    expected = stops_southeast_hem.loc[stops_southeast_hem.index.isin(
        [0, 1])]
    # check that result is identical except for the stops that were
    # located outside the bbox
    assert len(result) == 2
    assert result.equals(expected)


def test_check_time_range_format():
    utils_v._check_time_range_format(['07:00:00', '11:00:00'])


def test_check_time_range_format_invalid_params():
    msg = ('starttime and endtime are not in the correct format. '
           'Format should be a 24 hour clock in the following format: '
           '08:00:00 or 17:00:00.')
    with pytest.raises(ValueError) as excinfo:
        utils_v._check_time_range_format(['7:00:0', '10:00:00'])
    expected_error = ("['7:00:0', '10:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_v._check_time_range_format(['10:00:00'])
    expected_error = ("['10:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_v._check_time_range_format('10:00:00')
    expected_error = ("10:00:00 {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_v._check_time_range_format([100000, 170000])
    expected_error = ("[100000, 170000] {}".format(msg))
    assert expected_error in str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        utils_v._check_time_range_format(['10:00:00', '07:00:00'])
    expected_error = ("['10:00:00', '07:00:00'] {}".format(msg))
    assert expected_error in str(excinfo.value)
